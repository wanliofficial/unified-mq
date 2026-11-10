%% @doc AMQP 0-9-1 connection (S5, README §11.1).
%%
%% Implements the subset real clients need for exchange/queue/binding +
%% publish/consume/ack. Messages still travel the broker's normal path:
%% `broker_router' -> `broker_queue' (fsync) -> `broker_subscription'.
%% A queue maps to the destination `amqp/<queue>'; consumers of one queue
%% share a `shared' subscription so AMQP competing-consumer semantics hold.
%%
%% Scope (documented in `docs/protocol-compatibility.md' §4.7):
%%   * connection/channel handshake, exchange.declare/delete,
%%     queue.declare/bind/unbind/purge/delete, basic.qos/consume/cancel,
%%     basic.publish, basic.deliver, basic.ack/nack/reject, basic.recover;
%%   * exchanges: direct, fanout, topic (headers declared but not routed);
%%   * topology is transient (ETS), message bodies are durable;
%%   * `basic.get' answers get-empty, `mandatory' has no basic.return.
-module(amqp_conn).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(FRAME_METHOD, 1).
-define(FRAME_HEADER, 2).
-define(FRAME_BODY, 3).
-define(FRAME_HEARTBEAT, 8).
-define(FRAME_MAX, 131072).
-define(PAYLOAD_MAX, (?FRAME_MAX - 8)).
-define(DEFAULT_TICK, 100).

-record(ch, {
    pending = undefined :: undefined | {binary(), binary(), non_neg_integer(), binary()},
    headers = #{} :: map(),
    deliveries = #{} :: map(),
    next_tag = 1 :: pos_integer(),
    prefetch = 0 :: non_neg_integer(),
    consumers = #{} :: map()
}).

-record(st, {
    ref :: term(),
    transport :: module() | undefined,
    socket :: term() | undefined,
    buffer = <<>> :: binary(),
    phase = start :: start | tune | open | ready,
    client_id :: binary() | undefined,
    tenant = <<"t0">> :: binary(),
    principal = undefined :: binary() | undefined,
    frame_max = ?FRAME_MAX :: pos_integer(),
    channels = #{} :: #{non_neg_integer() => #ch{}},
    tick :: reference() | undefined
}).

%%%_* Lifecycle ------------------------------------------------------------

start_link(Ref, Transport, Opts) ->
    gen_server:start_link(?MODULE, {Ref, Transport, Opts}, []).

init({Ref, Transport, _Opts}) ->
    {ok, #st{ref = Ref, transport = Transport}}.

terminate(_Reason, #st{} = St) ->
    cleanup(St),
    ok.

%%%_* gen_server -----------------------------------------------------------

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({handshake, _Ref, Transport, Socket, _Timeout}, #st{phase = start} = St) ->
    %% Do not send connection.start yet: the client sends the 8-byte protocol
    %% header (`AMQP 0 0 9 1') first, and the spec has the server answer start
    %% after seeing it. `parse/2' recognises the header and calls send_start/1.
    St1 = St#st{transport = Transport, socket = Socket},
    rearm(St1);
handle_info({tcp, Socket, Data}, #st{socket = Socket} = St) ->
    on_data(Data, St);
handle_info({ssl, Socket, Data}, #st{socket = Socket} = St) ->
    on_data(Data, St);
handle_info({tcp_closed, Socket}, #st{socket = Socket} = St) ->
    {stop, normal, St};
handle_info({ssl_closed, Socket}, #st{socket = Socket} = St) ->
    {stop, normal, St};
handle_info({tcp_error, Socket, Reason}, #st{socket = Socket} = St) ->
    {stop, {socket_error, Reason}, St};
handle_info({ssl_error, Socket, Reason}, #st{socket = Socket} = St) ->
    {stop, {socket_error, Reason}, St};
handle_info({broker_deliver, ClientId, _MaxQoS, _Refs, {_P, _O, _Msg}},
            #st{client_id = ClientId} = St) ->
    {noreply, arm_tick(pull_all(St))};
handle_info(pull_tick, St) ->
    {noreply, arm_tick(pull_all(St#st{tick = undefined}))};
handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%_* Input ----------------------------------------------------------------

on_data(Data, St) ->
    Result = parse(<<(St#st.buffer)/binary, Data/binary>>, St#st{buffer = <<>>}),
    case Result of
        {noreply, St1} -> rearm(St1);
        Other -> Other
    end.

rearm(St) ->
    case set_active(St) of
        ok -> {noreply, St};
        {error, Reason} -> {stop, {socket_error, Reason}, St}
    end.

parse(<<"AMQP", 0, 0, 9, 1, Rest/binary>>, #st{phase = start} = St) ->
    %% Protocol header: answer with connection.start and move on.
    {noreply, St1} = send_start(St),
    parse(Rest, St1);
parse(Buffer, St) ->
    case amqp_codec:decode_frame(Buffer) of
        more ->
            {noreply, St#st{buffer = Buffer}};
        {ok, Frame, Rest} ->
            case handle_frame(Frame, St) of
                {noreply, St1} -> parse(Rest, St1);
                Stop -> Stop
            end;
        {error, Reason} ->
            {stop, {protocol_error, Reason}, St}
    end.

handle_frame(#{type := ?FRAME_HEARTBEAT}, St) ->
    {noreply, St};
handle_frame(#{type := ?FRAME_METHOD, channel := Channel, payload := Payload}, St) ->
    {Class, Method, Args} = amqp_codec:decode_method(Payload),
    handle_method(Channel, Class, Method, Args, St);
handle_frame(#{type := ?FRAME_HEADER, channel := Channel, payload := Payload}, St) ->
    handle_header(Channel, Payload, St);
handle_frame(#{type := ?FRAME_BODY, channel := Channel, payload := Payload}, St) ->
    handle_body(Channel, Payload, St);
handle_frame(_Frame, St) ->
    {noreply, St}.

%%%_* Handshake ------------------------------------------------------------

%% @doc Resolve the start-ok credential against the shared broker_security
%% identity table (S7) -- the same table MQTT uses.
%%
%% AMQP 0-9-1's PLAIN mechanism carries the response as `\0user\0password';
%% that is the shape this adapter reads.  `AMQPLAIN' and `EXTERNAL' are
%% accepted as "no credential offered" -- the former names a mechanism this
%% adapter does not implement, the latter defers to an external layer.
%%
%% A client that offers no usable credential stays anonymous unless the
%% deployment requires one.
authenticate(Mechanism, Response) ->
    case plain_credentials(Mechanism, Response) of
        {ok, {user_pass, User, Pass}} ->
            finish(broker_security:authenticate_conn({user_pass, User, Pass}));
        none ->
            case broker_security:auth_required(<<"amqp">>) of
                true -> {error, unauthenticated};
                false -> {ok, anonymous_identity()}
            end
    end.

plain_credentials(<<"PLAIN">>, Response) when is_binary(Response) ->
    case binary:split(Response, <<0>>, [global]) of
        %% An empty user AND password is what a client with no credentials
        %% sends -- that is "no credential offered", not a wrong one.
        [_, <<>>, <<>>] -> none;
        [_, User, Pass] -> {ok, {user_pass, User, Pass}};
        _ -> none
    end;
plain_credentials(_Mechanism, _Response) ->
    none.

finish({ok, Identity}) -> {ok, Identity};
finish({error, _}) -> {error, unauthenticated}.

anonymous_identity() ->
    #{principal => undefined, tenant_id => broker_security:default_tenant()}.

send_start(St) ->
    Props = amqp_codec:encode_table(#{<<"product">> => <<"UnifiedMQ">>,
                                      <<"version">> => <<"0.1.0">>,
                                      <<"platform">> => <<"Erlang">>}),
    Args = <<0:8, 9:8, Props/binary,
             (amqp_codec:encode_longstr(<<"PLAIN">>))/binary,
             (amqp_codec:encode_longstr(<<"en_US">>))/binary>>,
    {noreply, send_method(0, 10, 10, Args, St)}.

handle_method(0, 10, 11, Args, #st{phase = start} = St) ->
    %% connection.start-ok: client-properties, mechanism, response, locale.
    {_Props, Rest1} = amqp_codec:get_table(Args),
    {Mechanism, Rest2} = amqp_codec:get_shortstr(Rest1),
    {Response, Rest3} = amqp_codec:get_longstr(Rest2),
    {_Locale, _} = amqp_codec:get_shortstr(Rest3),
    %% The credential, when the client offered one, resolves against the
    %% shared broker_security identity table (S7) -- the same table MQTT uses.
    %% AMQP 0-9-1's PLAIN mechanism carries `\0user\0password'; that is the
    %% shape this adapter reads.  A client that offers no mechanism stays
    %% anonymous unless the deployment requires one.
    case authenticate(Mechanism, Response) of
        {ok, Identity} ->
            send_tune(St#st{phase = tune, tenant = maps:get(tenant_id, Identity),
                            principal = maps:get(principal, Identity)});
        {error, _} ->
            %% A rejected credential is a connection-level failure, not a
            %% channel one: the handshake never completed.  connection.close
            %% carries a reply-code and a reply-text.
            CloseArgs = iolist_to_binary(
                          [amqp_codec:encode_shortstr(<<"ACCESS_REFUSED">>),
                           amqp_codec:encode_shortstr(<<"authentication failed">>)]),
            send_method(0, 10, 50, CloseArgs, St),
            {stop, normal, St}
    end;
handle_method(0, 10, 31, Args, #st{phase = tune} = St) ->
    %% connection.tune-ok: channel-max, frame-max, heartbeat.
    <<_ChannelMax:16, FrameMax:32, _Heartbeat:16>> = Args,
    {noreply, St#st{phase = open, frame_max = min(?FRAME_MAX, max(4096, FrameMax))}};
handle_method(0, 10, 40, Args, #st{phase = open} = St) ->
    {_VHost, Rest1} = amqp_codec:get_shortstr(Args),
    {_Capabilities, _Rest2} = amqp_codec:get_shortstr(Rest1),
    Args2 = amqp_codec:encode_shortstr(<<>>),
    St1 = send_method(0, 10, 41, Args2, St#st{phase = ready}),
    {noreply, arm_tick(ensure_client(St1))};
handle_method(0, 10, 50, _Args, St) ->
    %% connection.close -> close-ok then stop.
    _ = send_method(0, 10, 51, <<>>, St),
    {stop, normal, St};
handle_method(0, 10, 51, _Args, St) ->
    {stop, normal, St};
handle_method(Channel, 20, 10, _Args, #st{phase = ready} = St) ->
    %% channel.open -> open-ok.
    Ch = maps:get(Channel, St#st.channels, #ch{}),
    {noreply, send_method(Channel, 20, 11, amqp_codec:encode_longstr(<<>>),
                          St#st{channels = (St#st.channels)#{Channel => Ch}})};
handle_method(Channel, 20, 40, _Args, St) ->
    %% channel.close from the client -> close-ok.
    {noreply, send_method(Channel, 20, 41, <<>>, drop_channel(Channel, St))};
handle_method(_Channel, 20, 41, _Args, St) ->
    %% channel.close-ok (our answer to a server-side channel error).
    {noreply, St};
handle_method(Channel, Class, Method, Args, #st{phase = ready} = St) ->
    handle_channel_method(Channel, Class, Method, Args, St);
handle_method(_Channel, _Class, _Method, _Args, St) ->
    {noreply, St}.

handle_channel_method(Channel, 40, 10, Args, St) ->
    %% exchange.declare
    <<_Ticket:16, Rest0/binary>> = Args,
    {Exchange, Rest1} = amqp_codec:get_shortstr(Rest0),
    {Type, Rest2} = amqp_codec:get_shortstr(Rest1),
    {[Passive, Durable, _Auto, _Internal, NoWait], Rest3} = amqp_codec:get_bits(Rest2, 5),
    {_Table, _} = amqp_codec:get_table(Rest3),
    case {Passive, amqp_topo:exchange_type(Exchange)} of
        {true, {error, no_exchange}} ->
            channel_error(Channel, 404, <<"NOT_FOUND - no exchange">>, 40, 10, St);
        _ ->
            case amqp_topo:declare_exchange(Exchange, Type, Durable) of
                ok -> {noreply, reply_ok(Channel, 40, 11, NoWait, St)};
                {ok, _} -> {noreply, reply_ok(Channel, 40, 11, NoWait, St)};
                {error, Reason} -> channel_error(Channel, 406, reason(Reason), 40, 10, St)
            end
    end;
handle_channel_method(Channel, 40, 20, Args, St) ->
    %% exchange.delete
    <<_Ticket:16, Rest0/binary>> = Args,
    {Exchange, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[_IfUnused, NoWait], _} = amqp_codec:get_bits(Rest1, 2),
    ok = amqp_topo:delete_exchange(Exchange),
    {noreply, reply_ok(Channel, 40, 21, NoWait, St)};
handle_channel_method(Channel, 50, 10, Args, St) ->
    %% queue.declare
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue0, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[Passive, Durable, Exclusive, _Auto, NoWait], Rest2} = amqp_codec:get_bits(Rest1, 5),
    {_Table, _} = amqp_codec:get_table(Rest2),
    case {Passive, amqp_topo:queue_exists(Queue0)} of
        {true, false} ->
            channel_error(Channel, 404, <<"NOT_FOUND - no queue">>, 50, 10, St);
        _ ->
            case amqp_topo:declare_queue(Queue0, Durable, Exclusive) of
                {ok, Queue} ->
                    Count = consumer_count(Queue, St),
                    ReplyArgs = <<(amqp_codec:encode_shortstr(Queue))/binary, 0:32, Count:32>>,
                    {noreply, reply_ok(Channel, 50, 11, NoWait, ReplyArgs, St)};
                {error, Reason} ->
                    channel_error(Channel, 406, reason(Reason), 50, 10, St)
            end
    end;
handle_channel_method(Channel, 50, 20, Args, St) ->
    %% queue.bind
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {Exchange, Rest2} = amqp_codec:get_shortstr(Rest1),
    {RK, Rest3} = amqp_codec:get_shortstr(Rest2),
    {[NoWait], Rest4} = amqp_codec:get_bits(Rest3, 1),
    {Table, _} = amqp_codec:get_table(Rest4),
    case amqp_topo:bind(Exchange, Queue, RK, Table) of
        ok -> {noreply, reply_ok(Channel, 50, 21, NoWait, St)};
        {error, Reason} -> channel_error(Channel, 404, reason(Reason), 50, 20, St)
    end;
handle_channel_method(Channel, 50, 50, Args, St) ->
    %% queue.unbind
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {Exchange, Rest2} = amqp_codec:get_shortstr(Rest1),
    {RK, Rest3} = amqp_codec:get_shortstr(Rest2),
    {Table, _} = amqp_codec:get_table(Rest3),
    ok = amqp_topo:unbind(Exchange, Queue, RK, Table),
    {noreply, send_method(Channel, 50, 51, <<>>, St)};
handle_channel_method(Channel, 50, 30, Args, St) ->
    %% queue.purge
    <<_Ticket:16, Rest0/binary>> = Args,
    {_Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[NoWait], _} = amqp_codec:get_bits(Rest1, 1),
    {noreply, reply_ok(Channel, 50, 31, NoWait, <<0:32>>, St)};
handle_channel_method(Channel, 50, 40, Args, St) ->
    %% queue.delete
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[IfUnused, _IfEmpty, NoWait], _} = amqp_codec:get_bits(Rest1, 3),
    case IfUnused andalso consumer_count(Queue, St) > 0 of
        true -> channel_error(Channel, 406, <<"PRECONDITION_FAILED - queue in use">>, 50, 40, St);
        false ->
            ok = amqp_topo:delete_queue(Queue),
            {noreply, reply_ok(Channel, 50, 41, NoWait, <<0:32>>, St)}
    end;
handle_channel_method(Channel, 60, 10, Args, St) ->
    %% basic.qos
    <<_PrefetchSize:32, PrefetchCount:16, _Global:8>> = Args,
    Ch = ch(Channel, St),
    {noreply, send_method(Channel, 60, 11, <<>>,
                          put_ch(Channel, Ch#ch{prefetch = PrefetchCount}, St))};
handle_channel_method(Channel, 60, 20, Args, St) ->
    %% basic.consume
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {Tag0, Rest2} = amqp_codec:get_shortstr(Rest1),
    {[NoLocal, NoAck, _Exclusive, NoWait], Rest3} = amqp_codec:get_bits(Rest2, 4),
    {_Table, _} = amqp_codec:get_table(Rest3),
    Tag = case Tag0 of
              <<>> -> <<"ctag-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>;
              _ -> Tag0
          end,
    case amqp_topo:queue_exists(Queue) of
        false ->
            channel_error(Channel, 404, <<"NOT_FOUND - no queue">>, 60, 20, St);
        true ->
            Filter = queue_filter(Queue),
            case broker_subscription:join(shared, Queue, St#st.client_id, Filter, 1) of
                {ok, Ref} ->
                    Consumer = #{ref => Ref, queue => Queue, no_ack => NoAck,
                                 no_local => NoLocal},
                    Ch = ch(Channel, St),
                    Ch1 = Ch#ch{consumers = (Ch#ch.consumers)#{Tag => Consumer}},
                    St1 = put_ch(Channel, Ch1, St),
                    St2 = reply_ok(Channel, 60, 21, NoWait,
                                   amqp_codec:encode_shortstr(Tag), St1),
                    {noreply, arm_tick(pull_all(St2))};
                {error, Reason} ->
                    channel_error(Channel, 406, reason(Reason), 60, 20, St)
            end
    end;
handle_channel_method(Channel, 60, 30, Args, St) ->
    %% basic.cancel
    {Tag, Rest1} = amqp_codec:get_shortstr(Args),
    {[NoWait], _} = amqp_codec:get_bits(Rest1, 1),
    Ch = ch(Channel, St),
    case maps:take(Tag, Ch#ch.consumers) of
        {#{ref := Ref}, Consumers} ->
            _ = (catch broker_subscription:leave(Ref, St#st.client_id)),
            St1 = put_ch(Channel, Ch#ch{consumers = Consumers}, St),
            {noreply, reply_ok(Channel, 60, 31, NoWait, amqp_codec:encode_shortstr(Tag), St1)};
        error ->
            {noreply, St}
    end;
handle_channel_method(Channel, 60, 40, Args, St) ->
    %% basic.publish: remember exchange/routing key until header+body arrive.
    <<_Ticket:16, Rest0/binary>> = Args,
    {Exchange, Rest1} = amqp_codec:get_shortstr(Rest0),
    {RK, Rest2} = amqp_codec:get_shortstr(Rest1),
    {[_Mandatory, _Immediate], _} = amqp_codec:get_bits(Rest2, 2),
    Ch = ch(Channel, St),
    {noreply, put_ch(Channel, Ch#ch{pending = {Exchange, RK, 0, <<>>}}, St)};
handle_channel_method(Channel, 60, 70, _Args, St) ->
    %% basic.get: not implemented, answer get-empty (documented).
    {noreply, send_method(Channel, 60, 72, amqp_codec:encode_shortstr(<<>>), St)};
handle_channel_method(Channel, 60, 80, Args, St) ->
    %% basic.ack
    <<Tag:64, Rest/binary>> = Args,
    {[Multiple], _} = amqp_codec:get_bits(Rest, 1),
    {noreply, ack_deliveries(Channel, Tag, Multiple, ack, St)};
handle_channel_method(Channel, 60, 90, Args, St) ->
    %% basic.reject
    <<Tag:64, Rest/binary>> = Args,
    {[Requeue], _} = amqp_codec:get_bits(Rest, 1),
    {noreply, ack_deliveries(Channel, Tag, false,
                             case Requeue of true -> release; false -> ack end, St)};
handle_channel_method(Channel, 60, 120, Args, St) ->
    %% basic.nack
    <<Tag:64, Rest/binary>> = Args,
    {[Multiple, Requeue], _} = amqp_codec:get_bits(Rest, 2),
    {noreply, ack_deliveries(Channel, Tag, Multiple,
                             case Requeue of true -> release; false -> ack end, St)};
handle_channel_method(Channel, 60, 110, _Args, St) ->
    %% basic.recover
    {noreply, send_method(Channel, 60, 111, <<>>, St)};
handle_channel_method(_Channel, _Class, _Method, _Args, St) ->
    {noreply, St}.

reply_ok(_Channel, _Class, _Method, true, St) ->
    St;
reply_ok(Channel, Class, Method, false, St) ->
    send_method(Channel, Class, Method, <<>>, St).

reply_ok(Channel, Class, Method, true, _Args, St) ->
    send_method(Channel, Class, Method, <<>>, St),
    St;
reply_ok(Channel, Class, Method, false, Args, St) ->
    send_method(Channel, Class, Method, Args, St).

handle_header(Channel, Payload, St) ->
    case amqp_codec:decode_content_header(Payload) of
        {60, _Weight, BodySize, PropsBin} ->
            Ch = ch(Channel, St),
            case Ch#ch.pending of
                {Exchange, RK, _Old, _Acc} ->
                    {Props, _Rest} = amqp_codec:decode_properties(PropsBin),
                    Ch1 = Ch#ch{pending = {Exchange, RK, BodySize, <<>>},
                                headers = props_to_headers(Props)},
                    St1 = put_ch(Channel, Ch1, St),
                    case BodySize of
                        0 -> {noreply, complete_publish(Channel, St1)};
                        _ -> {noreply, St1}
                    end;
                undefined ->
                    {noreply, St}
            end;
        _ ->
            {noreply, St}
    end.

%% AMQP basic-properties -> the broker's protocol-neutral header map. The
%% `headers' table is exactly where a CloudEvents client puts `ce-*' keys
%% (project binding, docs/protocol-compatibility.md §4.10); `content_type'
%% becomes the ordinary `content-type' header so the shared binding decoder
%% sees the same shape as the other protocols.
props_to_headers(Props) ->
    Table = case maps:get(headers, Props, #{}) of
                T when is_map(T) -> T;
                _ -> #{}
            end,
    case maps:get(content_type, Props, undefined) of
        undefined -> Table;
        CT -> Table#{<<"content-type">> => CT}
    end.

headers_to_props(#{<<"content-type">> := CT} = Headers) ->
    Table = maps:remove(<<"content-type">>, Headers),
    #{content_type => CT, headers => Table};
headers_to_props(Headers) when map_size(Headers) > 0 ->
    #{headers => Headers};
headers_to_props(_) ->
    #{}.

handle_body(Channel, Payload, St) ->
    Ch = ch(Channel, St),
    case Ch#ch.pending of
        {Exchange, RK, Size, Acc} ->
            Acc1 = <<Acc/binary, Payload/binary>>,
            case byte_size(Acc1) >= Size of
                true ->
                    <<Body:Size/binary, _/binary>> = Acc1,
                    Ch1 = Ch#ch{pending = {Exchange, RK, Size, Body}},
                    St1 = put_ch(Channel, Ch1, St),
                    {noreply, complete_publish(Channel, St1)};
                false ->
                    {noreply, put_ch(Channel, Ch#ch{pending = {Exchange, RK, Size, Acc1}}, St)}
            end;
        undefined ->
            {noreply, St}
    end.

complete_publish(Channel, St) ->
    Ch = ch(Channel, St),
    case Ch#ch.pending of
        {Exchange, RK, _Size, Body} ->
            publish(Channel, Exchange, RK, Body, Ch#ch.headers, St),
            put_ch(Channel, Ch#ch{pending = undefined, headers = #{}}, St);
        undefined ->
            St
    end.

%%%_* Publish --------------------------------------------------------------

publish(Channel, Exchange, RK, Body, Headers, St) ->
    Queues = amqp_topo:queues_for(Exchange, RK),
    lists:foreach(fun(Q) ->
                          publish_to_queue(Channel, Q, Exchange, RK, Body,
                                           Headers, St)
                  end, Queues),
    ok.

publish_to_queue(Channel, Queue, Exchange, RK, Body, Headers, St) ->
    Destination = queue_filter(Queue),
    Meta = #{<<"amqp">> => #{<<"exchange">> => Exchange,
                             <<"routing_key">> => RK,
                             <<"queue">> => Queue}},
    Msg = #broker_message{message_id = message_id(),
                          tenant_id = St#st.tenant,
                          destination = Destination,
                          partition_id = 0,
                          payload = Body,
                          payload_encoding = raw,
                          headers = Headers,
                          proto_meta = Meta,
                          persistence = durable,
                          reliability = fsync,
                          created_at = erlang:system_time(millisecond),
                          expires_at = undefined,
                          event = undefined},
    Ctx = #route_ctx{tenant_id = St#st.tenant,
                     publisher = St#st.client_id,
                     reliability = fsync},
    case broker_event:apply_policy(Msg) of
        {ok, Accepted} ->
            route_and_store(Destination, Accepted, Ctx);
        {error, EventReason} ->
            %% AMQP 0-9-1 basic.publish has no per-message negative ack; the
            %% honest signal is a channel error, not a silent drop.
            Text = iolist_to_binary(
                     io_lib:format("cloudevent rejected: ~p", [EventReason])),
            {noreply, St1} = channel_error(Channel, 406, Text, 60, 40, St),
            St1
    end.

route_and_store(Destination, Msg, Ctx) ->
    case broker_router:env() of
        {ok, Env} ->
            Backend = application:get_env(broker_router, backend, broker_router_hash),
            case Backend:route(Destination, Ctx, Env) of
                {ok, Routes} -> store_routes(Routes, Msg);
                {error, _} -> ok
            end;
        {error, _} ->
            ok
    end.

store_routes([], _Msg) ->
    ok;
store_routes([{P, Refs} | Rest], Msg) ->
    PartitionMsg = Msg#broker_message{partition_id = P},
    case (catch broker_queue:append(P, PartitionMsg, fsync)) of
        {ok, Offset} ->
            _ = (catch broker_subscription:dispatch(Refs, {P, Offset, PartitionMsg})),
            store_routes(Rest, Msg);
        _ ->
            store_routes(Rest, Msg)
    end.

%%%_* Delivery -------------------------------------------------------------

pull_all(St) ->
    Consumers = all_consumers(St),
    lists:foldl(fun({Channel, Tag, Consumer}, Acc) ->
                        pull_consumer(Channel, Tag, Consumer, Acc)
                end, St, Consumers).

all_consumers(St) ->
    lists:flatmap(
      fun({Channel, #ch{consumers = Cs}}) ->
              [{Channel, Tag, C} || {Tag, C} <- maps:to_list(Cs)]
      end, maps:to_list(St#st.channels)).

pull_consumer(Channel, _Tag, #{ref := Ref, no_ack := NoAck, queue := Queue}, St) ->
    lists:foldl(
      fun(P, Acc) ->
              case room(Channel, NoAck, Acc) of
                  0 -> Acc;
                  Max -> pull_partition(Channel, Ref, Queue, NoAck, Max, P, Acc)
              end
      end, St, broker_queue:partition_ids()).

%% basic.qos prefetch counts unacked deliveries on the channel. NoAck
%% consumers are not limited (AMQP says prefetch applies only when acks are on).
room(_Channel, true, _St) ->
    32;
room(Channel, false, St) ->
    Ch = ch(Channel, St),
    case Ch#ch.prefetch of
        0 -> 32;
        N -> max(0, N - maps:size(Ch#ch.deliveries))
    end.

pull_partition(Channel, Ref, Queue, NoAck, Max, P, St) ->
    case (catch broker_subscription:claim(Ref, P, Max, St#st.client_id)) of
        {ok, []} ->
            St;
        {ok, Offsets} ->
            {Read, Missing} = read_offsets(P, Offsets),
            Expected = queue_filter(Queue),
            St1 = lists:foldl(
                    fun({O, Msg}, Acc) ->
                            case Msg#broker_message.destination =:= Expected of
                                true ->
                                    deliver(Channel, Ref, Queue, NoAck, P, O, Msg, Acc);
                                false ->
                                    %% Another AMQP queue's message landed in the
                                    %% same partition. A shared cursor scans the
                                    %% whole partition, so consume the offset for
                                    %% this group without delivering it.
                                    _ = (catch broker_subscription:ack(
                                                 Ref, P, O, St#st.client_id)),
                                    Acc
                            end
                    end, St, Read),
            case Missing of
                [] -> ok;
                _ -> _ = (catch broker_subscription:release(Ref, P, Missing, St1#st.client_id))
            end,
            St1;
        _ ->
            St
    end.

read_offsets(P, Offsets) ->
    lists:foldl(
      fun(O, {Acc, Missing}) ->
              case (catch broker_queue:read_from(P, O, 1)) of
                  {ok, [{O, Msg}], _Next} -> {[{O, Msg} | Acc], Missing};
                  _ -> {Acc, [O | Missing]}
              end
      end, {[], []}, Offsets).

deliver(Channel, Ref, _Queue, NoAck, P, O, Msg, St) ->
    Ch = ch(Channel, St),
    Tag = Ch#ch.next_tag,
    Consumers = [T || {T, #{ref := R}} <- maps:to_list(Ch#ch.consumers), R =:= Ref],
    case Consumers of
        [] ->
            _ = (catch broker_subscription:release(Ref, P, [O], St#st.client_id)),
            St;
        [ConsumerTag | _] ->
            #{<<"amqp">> := #{<<"exchange">> := Exchange,
                              <<"routing_key">> := RK}} = Msg#broker_message.proto_meta,
            DeliverArgs = <<(amqp_codec:encode_shortstr(ConsumerTag))/binary,
                            Tag:64, 0:8,
                           (amqp_codec:encode_shortstr(Exchange))/binary,
                           (amqp_codec:encode_shortstr(RK))/binary>>,
            St1 = send_method(Channel, 60, 60, DeliverArgs, St),
            St2 = send_content(Channel, Msg#broker_message.payload,
                               Msg#broker_message.headers, St1),
            case NoAck of
                true ->
                    _ = (catch broker_subscription:ack(Ref, P, O, St2#st.client_id)),
                    put_ch(Channel, Ch#ch{next_tag = Tag + 1}, St2);
                false ->
                    Ch1 = Ch#ch{next_tag = Tag + 1,
                                deliveries = (Ch#ch.deliveries)#{Tag => {Ref, P, O}}},
                    put_ch(Channel, Ch1, St2)
            end
    end.

ack_deliveries(Channel, Tag, Multiple, Mode, St) ->
    Ch = ch(Channel, St),
    {Do, Keep} = maps:fold(
                   fun(T, D, {Yes, No}) ->
                           case Multiple of
                               true when T =< Tag -> {[{T, D} | Yes], No};
                               false when T =:= Tag -> {[{T, D} | Yes], No};
                               _ -> {Yes, No#{T => D}}
                           end
                   end, {[], #{}}, Ch#ch.deliveries),
    lists:foreach(
      fun({T, {Ref, P, O}}) ->
              _ = T,
              case Mode of
                  ack -> _ = (catch broker_subscription:ack(Ref, P, O, St#st.client_id));
                  release -> _ = (catch broker_subscription:release(Ref, P, [O], St#st.client_id))
              end
      end, Do),
    put_ch(Channel, Ch#ch{deliveries = Keep}, St).

%%%_* Wire helpers ---------------------------------------------------------

send_tune(St) ->
    Args = <<2047:16, ?FRAME_MAX:32, 0:16>>,
    {noreply, send_method(0, 10, 30, Args, St)}.

send_method(Channel, Class, Method, Args, St) ->
    send_frame(?FRAME_METHOD, Channel, amqp_codec:encode_method(Class, Method, Args), St).

send_content(Channel, Body, Headers, St) ->
    Header = amqp_codec:encode_content_header(byte_size(Body),
                                              headers_to_props(Headers)),
    St1 = send_frame(?FRAME_HEADER, Channel, Header, St),
    send_body_frames(Channel, Body, St1).

send_body_frames(_Channel, <<>>, St) ->
    St;
send_body_frames(Channel, Body, St) ->
    Max = max(1, St#st.frame_max - 8),
    case byte_size(Body) =< Max of
        true ->
            send_frame(?FRAME_BODY, Channel, Body, St);
        false ->
            <<Chunk:Max/binary, Rest/binary>> = Body,
            St1 = send_frame(?FRAME_BODY, Channel, Chunk, St),
            send_body_frames(Channel, Rest, St1)
    end.

send_frame(Type, Channel, Payload, #st{socket = undefined} = St) ->
    _ = {Type, Channel, Payload},
    St;
send_frame(Type, Channel, Payload, #st{transport = T, socket = S} = St) ->
    _ = T:send(S, amqp_codec:encode_frame(Type, Channel, Payload)),
    St.

set_active(#st{transport = T, socket = S}) ->
    T:setopts(S, [{active, once}]).

channel_error(Channel, Code, Text, Class, Method, St) ->
    Args = <<Code:16, (amqp_codec:encode_shortstr(Text))/binary,
             Class:16, Method:16>>,
    St1 = send_method(Channel, 20, 40, Args, St),
    {noreply, drop_channel(Channel, St1)}.

%%%_* State helpers --------------------------------------------------------

ch(Channel, St) ->
    maps:get(Channel, St#st.channels, #ch{}).

put_ch(Channel, Ch, St) ->
    St#st{channels = (St#st.channels)#{Channel => Ch}}.

drop_channel(Channel, St) ->
    St#st{channels = maps:remove(Channel, St#st.channels)}.

consumer_count(Queue, St) ->
    length([1 || {_, #ch{consumers = Cs}} <- maps:to_list(St#st.channels),
                 #{queue := Q} <- maps:values(Cs), Q =:= Queue]).

ensure_client(#st{client_id = undefined} = St) ->
    ClientId = <<"amqp-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    {ok, _Prev} = broker_subscription:attach(ClientId, self(), false),
    St#st{client_id = ClientId};
ensure_client(St) ->
    St.

queue_filter(Queue) ->
    <<"amqp/", Queue/binary>>.

arm_tick(#st{} = St) ->
    case St#st.tick of
        undefined -> St#st{tick = erlang:send_after(tick_ms(), self(), pull_tick)};
        _ -> St
    end.

tick_ms() ->
    application:get_env(broker_proto_amqp, poll_ms, ?DEFAULT_TICK).

message_id() ->
    base64:encode(crypto:strong_rand_bytes(12)).

reason(Reason) when is_binary(Reason) -> Reason;
reason(Reason) -> iolist_to_binary(io_lib:format("~p", [Reason])).

cleanup(#st{} = St) ->
    cancel_tick(St),
    lists:foreach(
      fun({Channel, #ch{} = Ch}) ->
              lists:foreach(
                fun({_T, {Ref, P, O}}) ->
                        _ = (catch broker_subscription:release(Ref, P, [O], St#st.client_id))
                end, maps:to_list(Ch#ch.deliveries)),
              lists:foreach(
                fun({_T, #{ref := Ref}}) ->
                        _ = (catch broker_subscription:leave(Ref, St#st.client_id))
                end, maps:to_list(Ch#ch.consumers)),
              _ = Channel
      end, maps:to_list(St#st.channels)),
    case St#st.client_id of
        undefined -> ok;
        ClientId -> _ = (catch broker_subscription:detach(ClientId, self())), ok
    end.

cancel_tick(#st{tick = undefined}) -> ok;
cancel_tick(#st{tick = Ref}) -> _ = erlang:cancel_timer(Ref), ok.
