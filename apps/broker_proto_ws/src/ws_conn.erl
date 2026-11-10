%% @doc WebSocket connection process: RFC 6455 handshake + the broker's own
%% JSON/binary message API (S5, README §11.1).
%%
%% One process per connection, started by ranch as the protocol process. The
%% application protocol is deliberately small and documented in
%% `docs/protocol-compatibility.md` §6:
%%
%%   * a **text frame** carries one JSON object with an `op' field;
%%   * a **binary frame** carries `<<Op:8, HeaderLen:32, Header/binary,
%%     Payload/binary>>' where `Header' is the same JSON object and `Payload'
%%     is the raw message body.
%%
%% Operations: `connect', `subscribe', `unsubscribe', `publish', `ack',
%% `ping', `disconnect'. Every message is stored with `fsync' and routed
%% through the same `broker_router'/`broker_queue'/`broker_subscription'
%% facades as MQTT; no protocol-specific storage path exists.
-module(ws_conn).

-behaviour(gen_server).

-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(GUID, <<"258EAFA5-E914-47DA-95CA-C5AB0DC85B11">>).
-define(MAX_HANDSHAKE, 16384).

-record(st, {
    ref :: term(),
    transport :: module() | undefined,
    socket :: term() | undefined,
    buffer = <<>> :: binary(),
    handshake = pending :: pending | done,
    client_id :: binary() | undefined,
    tenant = <<"t0">> :: binary(),
    principal = undefined :: binary() | undefined,
    clean = true :: boolean(),
    subs = #{} :: #{term() => #{filter := binary(), qos := 0..2}},
    pending = #{} :: #{pos_integer() => #{refs := [term()], partition := integer(),
                                          offset := integer()}},
    next_delivery = 1 :: pos_integer(),
    seen = #{} :: map(),
    frag = undefined :: undefined | {0..2, [binary()]},
    closing = false :: boolean()
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

%% ranch sends this after `controlling_process' has moved the socket to us.
handle_info({handshake, _Ref, Transport, Socket, _Timeout}, #st{handshake = pending} = St) ->
    St1 = St#st{transport = Transport, socket = Socket},
    case set_active(St1) of
        ok -> {noreply, St1};
        {error, Reason} -> {stop, {socket_error, Reason}, St1}
    end;
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
handle_info({broker_deliver, ClientId, _MaxQoS, Refs, {P, O, Msg}},
            #st{client_id = ClientId} = St) ->
    {noreply, deliver_refs(Refs, P, O, Msg, St)};
handle_info({ws_takeover, _NewPid}, St) ->
    _ = send_close(1000, <<"session taken over">>, St),
    {stop, normal, St};
handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Handshake ------------------------------------------------------------

on_data(Data, #st{} = St) ->
    Result =
        case St#st.handshake of
            pending ->
                Buffer = <<(St#st.buffer)/binary, Data/binary>>,
                case byte_size(Buffer) > ?MAX_HANDSHAKE of
                    true ->
                        _ = raw_send(<<"HTTP/1.1 431 Request Header Fields Too Large\r\n"
                                       "Connection: close\r\n\r\n">>, St),
                        {stop, normal, St};
                    false ->
                        handshake(Buffer, St)
                end;
            done ->
                parse_frames(<<(St#st.buffer)/binary, Data/binary>>, St)
        end,
    %% `{active, once}' has to be re-armed after every delivery, including the
    %% handshake burst; forgetting it makes the next client frame look like a
    %% silent close.
    case Result of
        {noreply, St1} -> rearm(St1);
        Other -> Other
    end.

rearm(St) ->
    case set_active(St) of
        ok -> {noreply, St};
        {error, Reason} -> {stop, {socket_error, Reason}, St}
    end.

handshake(Buffer, St) ->
    case binary:split(Buffer, <<"\r\n\r\n">>) of
        [Head, Rest] ->
            case accept(Head) of
                {ok, Response} ->
                    _ = raw_send(Response, St),
                    St1 = St#st{handshake = done, buffer = <<>>},
                    parse_frames(Rest, St1);
                {error, Status} ->
                    _ = raw_send(Status, St),
                    {stop, normal, St}
            end;
        _ ->
            {noreply, St#st{buffer = Buffer}}
    end.

accept(Head) ->
    case binary:split(Head, <<"\r\n">>, [global]) of
        [RequestLine | HeaderLines] ->
            Headers = headers(HeaderLines),
            case request_line(RequestLine) of
                ok -> validate_upgrade(Headers);
                error -> {error, bad_request()}
            end;
        _ ->
            {error, bad_request()}
    end.

request_line(<<"GET ", _/binary>>) -> ok;
request_line(_) -> error.

validate_upgrade(Headers) ->
    Upgrade = lower(maps:get(<<"upgrade">>, Headers, <<>>)),
    Connection = lower(maps:get(<<"connection">>, Headers, <<>>)),
    Version = maps:get(<<"sec-websocket-version">>, Headers, <<>>),
    Key = maps:get(<<"sec-websocket-key">>, Headers, <<>>),
    case {binary:match(Upgrade, <<"websocket">>), binary:match(Connection, <<"upgrade">>),
          Version, Key} of
        {nomatch, _, _, _} -> {error, upgrade_required()};
        {_, nomatch, _, _} -> {error, upgrade_required()};
        {_, _, <<"13">>, <<>>} -> {error, bad_request()};
        {_, _, <<"13">>, _} ->
            Accept = base64:encode(crypto:hash(sha, <<Key/binary, ?GUID/binary>>)),
            {ok, [<<"HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    "Sec-WebSocket-Accept: ">>, Accept, <<"\r\n\r\n">>]};
        {_, _, _, _} ->
            {error, <<"HTTP/1.1 426 Upgrade Required\r\n"
                      "Sec-WebSocket-Version: 13\r\n"
                      "Connection: close\r\n\r\n">>}
    end.

headers(Lines) ->
    lists:foldl(
      fun(Line, Acc) ->
              case binary:split(Line, <<":">>) of
                  [K, V] ->
                      Acc#{lower(trim(K)) => trim(V)};
                  _ ->
                      Acc
              end
      end, #{}, Lines).

trim(B) ->
    iolist_to_binary(string:trim(B, both, " \t")).

lower(B) ->
    << <<(if C >= $A, C =< $Z -> C + 32; true -> C end)>> || <<C>> <= B >>.

bad_request() ->
    <<"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n">>.

upgrade_required() ->
    <<"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n"
      "Upgrade: websocket\r\n\r\n">>.

%%%_* Frame loop -----------------------------------------------------------

parse_frames(Buffer, St) ->
    Max = max_frame_bytes(),
    case ws_frame:decode(Buffer, Max) of
        {ok, Frame, Rest} ->
            case handle_frame(Frame, St) of
                {noreply, St1} -> parse_frames(Rest, St1);
                {stop, Reason, St1} -> {stop, Reason, St1}
            end;
        {more, _Need} ->
            {noreply, St#st{buffer = Buffer}};
        {error, Reason} ->
            _ = send_close(1002, reason_bin(Reason), St),
            {stop, {frame_error, Reason}, St}
    end.

handle_frame(#{opcode := 8, payload := Payload}, St) ->
    Code = ws_frame:close_code(Payload),
    _ = send_close(Code, <<>>, St),
    {stop, normal, St#st{closing = true}};
handle_frame(#{opcode := 9, payload := Payload}, St) ->
    {noreply, send_frame(10, Payload, St)};
handle_frame(#{opcode := 10, payload := _Pong}, St) ->
    {noreply, St};
handle_frame(#{fin := false, opcode := Op, payload := Payload}, St) when Op =:= 1; Op =:= 2 ->
    {noreply, St#st{frag = {Op, [Payload]}}};
handle_frame(#{fin := false, opcode := 0, payload := Payload}, #st{frag = {Op, Acc}} = St) ->
    {noreply, St#st{frag = {Op, [Payload | Acc]}}};
handle_frame(#{fin := true, opcode := 0, payload := Payload}, #st{frag = {Op, Acc}} = St) ->
    handle_message(Op, iolist_to_binary(lists:reverse([Payload | Acc])), St#st{frag = undefined});
handle_frame(#{fin := true, opcode := Op, payload := Payload}, St) when Op =:= 1; Op =:= 2 ->
    handle_message(Op, Payload, St);
handle_frame(#{opcode := Op}, St) ->
    _ = send_close(1002, <<"unexpected opcode">>, St),
    {stop, {unexpected_opcode, Op}, St}.

handle_message(1, Payload, St) ->
    handle_json(Payload, St);
handle_message(2, <<Op:8, HLen:32, Header:HLen/binary, Body/binary>>, St) ->
    case decode_json(Header) of
        {ok, Map} -> handle_op(op_name(Op), Map#{<<"payload_raw">> => Body}, St);
        {error, _} -> {noreply, send_error(<<"bad_header">>, <<"binary header is not JSON">>, St)}
    end;
handle_message(2, _Bad, St) ->
    {noreply, send_error(<<"bad_frame">>, <<"binary frame needs op, header length, header", 
                         " and payload">>, St)}.

handle_json(Payload, St) ->
    case decode_json(Payload) of
        {ok, #{<<"op">> := Op} = Map} ->
            handle_op(op_name(Op), Map, St);
        {ok, _} ->
            {noreply, send_error(<<"bad_request">>, <<"missing op">>, St)};
        {error, Reason} ->
            {noreply, send_error(<<"bad_json">>, reason_bin(Reason), St)}
    end.

op_name(<<"connect">>) -> connect;
op_name(<<"subscribe">>) -> subscribe;
op_name(<<"unsubscribe">>) -> unsubscribe;
op_name(<<"publish">>) -> publish;
op_name(<<"ack">>) -> ack;
op_name(<<"ping">>) -> ping;
op_name(<<"disconnect">>) -> disconnect;
op_name(1) -> publish;
op_name(2) -> ack;
op_name(Other) -> {unknown, Other}.

handle_op(connect, Map, St) -> do_connect(Map, St);
handle_op(subscribe, Map, St) -> do_subscribe(Map, St);
handle_op(unsubscribe, Map, St) -> do_unsubscribe(Map, St);
handle_op(publish, Map, St) -> do_publish(Map, St);
handle_op(ack, Map, St) -> do_ack(Map, St);
handle_op(ping, _Map, St) -> {noreply, send_json(#{<<"op">> => <<"pong">>}, St)};
handle_op(disconnect, _Map, St) ->
    _ = send_close(1000, <<"normal closure">>, St),
    {stop, normal, St#st{closing = true}};
handle_op({unknown, Op}, _Map, St) ->
    {noreply, send_error(<<"unknown_op">>, reason_bin(Op), St)}.

%%%_* Operations -----------------------------------------------------------

do_connect(Map, #st{client_id = Existing} = St) when Existing =/= undefined ->
    _ = Map,
    {noreply, send_error(<<"already_connected">>, <<"one connect per connection">>, St)};
do_connect(Map, St) ->
    case maps:get(<<"client_id">>, Map, undefined) of
        <<>> ->
            {noreply, send_error(<<"bad_request">>, <<"client_id is required">>, St)};
        undefined ->
            {noreply, send_error(<<"bad_request">>, <<"client_id is required">>, St)};
        ClientId when is_binary(ClientId) ->
            %% The credential, when present, resolves against the shared
            %% broker_security identity table (S7) -- the same table MQTT
            %% uses.  The client may NOT name its own tenant: the tenant
            %% comes from the matched user record, or from the deployment
            %% default for an anonymous connection.
            case authenticate(Map) of
                {ok, Identity} ->
                    Tenant = maps:get(tenant_id, Identity),
                    Principal = maps:get(principal, Identity),
                    Clean = maps:get(<<"clean_start">>, Map, true),
                    Persist = Clean =:= false,
                    Present = case {Clean, broker_subscription:session(ClientId)} of
                                  {false, {ok, _}} -> true;
                                  _ -> false
                              end,
                    %% `broker_subscription:attach/3' returns `{ok, PrevPid}'
                    %% (its contract has no error branch); a failed takeover
                    %% is a bookkeeping concern, never a reason to reject the
                    %% connect.
                    {ok, Prev} = broker_subscription:attach(ClientId, self(), Persist),
                    maybe_takeover(Prev),
                    Subs = case Persist of
                               true -> session_subs(ClientId);
                               false -> #{}
                           end,
                    St1 = St#st{client_id = ClientId, tenant = Tenant,
                                principal = Principal,
                                clean = Clean, subs = Subs},
                    %% CONNACK first: catch-up replays offline messages, and a
                    %% client must know its session was accepted before the
                    %% first delivery arrives.
                    St2 = send_json(#{<<"op">> => <<"connack">>,
                                      <<"session_present">> => Present,
                                      <<"protocol">> => <<"unifiedmq-ws/1">>}, St1),
                    {noreply, catch_up(St2)};
                {error, Reason} ->
                    {noreply, send_error(<<"unauthorized">>, Reason, St)}
            end
    end.

%% @doc Resolve the connect frame's credential against the shared identity
%% table.  `username'/`password' is the user/password shape; `token' is the
%% bearer shape (the same one Pulsar uses).  With no credential at all the
%% connection is anonymous unless the deployment requires one.
authenticate(Map) ->
    case {maps:get(<<"username">>, Map, undefined),
          maps:get(<<"password">>, Map, undefined),
          maps:get(<<"token">>, Map, undefined)} of
        {undefined, undefined, undefined} ->
            case broker_security:auth_required(<<"ws">>) of
                true -> {error, <<"authentication required">>};
                false -> {ok, anonymous_identity()}
            end;
        {Username, Password, _} when is_binary(Username), is_binary(Password) ->
            finish(broker_security:authenticate_conn({user_pass, Username, Password}));
        {_Username, _Password, Token} when is_binary(Token) ->
            finish(broker_security:authenticate_conn({token, Token}));
        _ ->
            {error, <<"malformed credentials">>}
    end.

finish({ok, Identity}) -> {ok, Identity};
finish({error, _}) -> {error, <<"bad_username_or_password">>}.

anonymous_identity() ->
    #{principal => undefined, tenant_id => broker_security:default_tenant()}.

maybe_takeover(Pid) when is_pid(Pid), Pid =/= self() ->
    Pid ! {ws_takeover, self()},
    ok;
maybe_takeover(_) ->
    ok.

session_subs(ClientId) ->
    lists:foldl(
      fun(#{sub_ref := Ref, filter := Filter, qos := QoS}, Acc) ->
              Acc#{Ref => #{filter => Filter, qos => QoS}}
      end, #{}, broker_subscription:subscriptions(ClientId)).

do_subscribe(_Map, #st{client_id = undefined} = St) ->
    {noreply, send_error(<<"not_connected">>, <<"send connect first">>, St)};
do_subscribe(Map, St) ->
    Id = maps:get(<<"id">>, Map, null),
    case maps:get(<<"destination">>, Map, undefined) of
        Filter when is_binary(Filter), Filter =/= <<>> ->
            QoS = maps:get(<<"qos">>, Map, 1),
            Kind = maps:get(<<"kind">>, Map, <<"exclusive">>),
            case {valid_filter(Filter), valid_qos(QoS), Kind} of
                {true, true, <<"exclusive">>} ->
                    case broker_subscription:subscribe(St#st.client_id, Filter, QoS) of
                        {ok, Ref} ->
                            Subs = (St#st.subs)#{Ref => #{filter => Filter, qos => QoS}},
                            St1 = catch_up_ref(Ref, QoS, St#st{subs = Subs}),
                            {noreply, send_json(#{<<"op">> => <<"suback">>,
                                                  <<"id">> => Id,
                                                  <<"destination">> => Filter,
                                                  <<"qos">> => QoS}, St1)};
                        {error, Reason} ->
                            {noreply, send_json(#{<<"op">> => <<"suback">>, <<"id">> => Id,
                                                  <<"destination">> => Filter,
                                                  <<"granted">> => false,
                                                  <<"reason">> => reason_bin(Reason)}, St)}
                    end;
                {true, true, _} ->
                    {noreply, send_json(#{<<"op">> => <<"suback">>, <<"id">> => Id,
                                          <<"destination">> => Filter,
                                          <<"granted">> => false,
                                          <<"reason">> => <<"shared subscriptions are not supported (S5 WebSocket v1)">>}, St)};
                {false, _, _} ->
                    {noreply, send_json(#{<<"op">> => <<"suback">>, <<"id">> => Id,
                                          <<"destination">> => Filter,
                                          <<"granted">> => false,
                                          <<"reason">> => <<"invalid filter">>}, St)};
                {_, false, _} ->
                    {noreply, send_json(#{<<"op">> => <<"suback">>, <<"id">> => Id,
                                          <<"destination">> => Filter,
                                          <<"granted">> => false,
                                          <<"reason">> => <<"qos must be 0 or 1">>}, St)}
            end;
        _ ->
            {noreply, send_error(<<"bad_request">>, <<"destination is required">>, St)}
    end.

do_unsubscribe(_Map, #st{client_id = undefined} = St) ->
    {noreply, send_error(<<"not_connected">>, <<"send connect first">>, St)};
do_unsubscribe(Map, St) ->
    Id = maps:get(<<"id">>, Map, null),
    case maps:get(<<"destination">>, Map, undefined) of
        Filter when is_binary(Filter) ->
            _ = broker_subscription:unsubscribe(St#st.client_id, Filter),
            Subs = maps:filter(fun(_Ref, #{filter := F}) -> F =/= Filter end, St#st.subs),
            {noreply, send_json(#{<<"op">> => <<"unsuback">>, <<"id">> => Id,
                                  <<"destination">> => Filter}, St#st{subs = Subs})};
        _ ->
            {noreply, send_error(<<"bad_request">>, <<"destination is required">>, St)}
    end.

do_publish(_Map, #st{client_id = undefined} = St) ->
    {noreply, send_error(<<"not_connected">>, <<"send connect first">>, St)};
do_publish(Map, St) ->
    Id = maps:get(<<"id">>, Map, null),
    case maps:get(<<"destination">>, Map, undefined) of
        Destination when is_binary(Destination), Destination =/= <<>> ->
            QoS = maps:get(<<"qos">>, Map, 1),
            MaxBytes = max_publish_bytes(),
            case {valid_qos(QoS), payload(Map)} of
                {true, {ok, Payload}} when byte_size(Payload) =< MaxBytes ->
                    publish(Destination, QoS, Payload, Id, ce_headers(Map), St);
                {true, {ok, _TooBig}} ->
                    {noreply, send_error(<<"payload_too_large">>, <<"max_publish_bytes">>, St)};
                {true, {error, Reason}} ->
                    {noreply, send_error(<<"bad_payload">>, reason_bin(Reason), St)};
                {false, _} ->
                    {noreply, send_error(<<"bad_qos">>, <<"qos must be 0 or 1">>, St)}
            end;
        _ ->
            {noreply, send_error(<<"bad_request">>, <<"destination is required">>, St)}
    end.

publish(Destination, QoS, Payload, Id, Headers, #st{client_id = ClientId, tenant = Tenant} = St) ->
    MsgId = message_id(),
    Msg = #broker_message{message_id = MsgId,
                          tenant_id = Tenant,
                          destination = Destination,
                          partition_id = 0,
                          payload = Payload,
                          payload_encoding = payload_encoding(Payload),
                          headers = Headers,
                          proto_meta = #{<<"ws">> => #{<<"client_id">> => ClientId}},
                          persistence = durable,
                          reliability = fsync,
                          created_at = erlang:system_time(millisecond),
                          expires_at = undefined,
                          event = undefined},
    Ctx = #route_ctx{tenant_id = Tenant, publisher = ClientId, reliability = fsync},
    case broker_event:apply_policy(Msg) of
        {ok, Accepted} ->
            route_and_store(Destination, QoS, Id, Accepted, Ctx, St);
        {error, EventReason} ->
            {noreply, send_error(<<"cloudevent_rejected">>,
                                 reason_bin(EventReason), St)}
    end.

route_and_store(Destination, QoS, Id, Msg, Ctx, St) ->
    case broker_router:env() of
        {ok, Env} ->
            Backend = application:get_env(broker_router, backend, broker_router_hash),
            case Backend:route(Destination, Ctx, Env) of
                {ok, Routes} ->
                    case store_routes(Routes, Msg, []) of
                        {ok, Offsets} ->
                            {noreply, send_json(#{<<"op">> => <<"puback">>, <<"id">> => Id,
                                                  <<"destination">> => Destination,
                                                  <<"qos">> => QoS,
                                                  <<"offsets">> => Offsets}, St)};
                        {error, Reason} ->
                            {noreply, send_error(<<"append_failed">>, reason_bin(Reason), St)}
                    end;
                {error, Reason} ->
                    {noreply, send_error(<<"route_failed">>, reason_bin(Reason), St)}
            end;
        {error, Reason} ->
            {noreply, send_error(<<"router_unavailable">>, reason_bin(Reason), St)}
    end.

%% The WS application frame is a JSON object; CloudEvents Binary Mode rides on
%% top-level `ce-*' keys (project binding, docs/protocol-compatibility.md
%% §4.10). Only binary values are binding attributes.
ce_headers(Map) ->
    Trace = broker_event_trace:enabled(),
    maps:filter(fun(K, V) ->
                        is_binary(K) andalso is_binary(V) andalso
                            (is_ce_key(K) orelse K =:= <<"content-type">> orelse
                             (Trace andalso is_trace_key(K)))
                end, Map).

is_ce_key(<<"ce-", _/binary>>) -> true;
is_ce_key(_) -> false.

%% W3C Trace Context (README §10.2) is not a CloudEvents attribute: it rides
%% beside the `ce-*' keys in the same neutral header map.  It is only admitted
%% when the deployment opted into propagation; otherwise a client-supplied
%% `traceparent' is dropped at the door, like any other unknown header.
is_trace_key(<<"traceparent">>) -> true;
is_trace_key(<<"tracestate">>) -> true;
is_trace_key(_) -> false.

store_routes([], _Msg, Acc) ->
    {ok, lists:reverse(Acc)};
store_routes([{P, Refs} | Rest], Msg, Acc) ->
    PartitionMsg = Msg#broker_message{partition_id = P},
    case (catch broker_queue:append(P, PartitionMsg, fsync)) of
        {ok, Offset} ->
            _ = (catch broker_subscription:dispatch(Refs, {P, Offset, PartitionMsg})),
            store_routes(Rest, Msg, [Offset | Acc]);
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, {append_crashed, Other}}
    end.

do_ack(Map, #st{client_id = ClientId} = St) ->
    Id = maps:get(<<"delivery_id">>, Map, undefined),
    case maps:take(Id, St#st.pending) of
        {#{refs := Refs, partition := P, offset := O}, Pending} ->
            lists:foreach(fun(Ref) -> _ = (catch broker_subscription:ack(Ref, P, O, ClientId)) end,
                          Refs),
            {noreply, send_json(#{<<"op">> => <<"ackack">>, <<"delivery_id">> => Id}, 
                                St#st{pending = Pending})};
        error ->
            {noreply, send_error(<<"unknown_delivery">>, <<"no such delivery_id">>, St)}
    end.

%%%_* Delivery -------------------------------------------------------------

deliver_refs(Refs, P, O, Msg, St) ->
    Mine = [Ref || Ref <- Refs, maps:is_key(Ref, St#st.subs)],
    case Mine of
        [] ->
            St;
        _ ->
            QoS = lists:max([maps:get(qos, maps:get(Ref, St#st.subs)) || Ref <- Mine]),
            St1 = deliver(Mine, QoS, P, O, Msg, St),
            case QoS of
                0 ->
                    lists:foreach(
                      fun(Ref) ->
                              _ = (catch broker_subscription:ack(
                                               Ref, P, O, St#st.client_id))
                      end, Mine),
                    St1;
                _ ->
                    St1
            end
    end.

deliver(Refs, QoS, P, O, Msg, St) ->
    case already_seen(Refs, P, O, St) of
        true ->
            St;
        false ->
            Filters = [maps:get(filter, maps:get(Ref, St#st.subs)) || Ref <- Refs],
            Header = #{<<"op">> => <<"message">>,
                       <<"subscriptions">> => Filters,
                       <<"destination">> => Msg#broker_message.destination,
                       <<"partition">> => P,
                       <<"offset">> => O,
                       <<"qos">> => QoS,
                       <<"message_id">> => Msg#broker_message.message_id,
                       <<"tenant">> => Msg#broker_message.tenant_id,
                       %% The raw bytes travel in the binary frame body; the
                       %% header repeats them base64-encoded so a text-only
                       %% client can read the same delivery without decoding
                       %% the binary envelope.
                       <<"payload_b64">> => base64:encode(Msg#broker_message.payload)},
            {Id, St1} = case QoS of
                            0 -> {null, St};
                            _ ->
                                D = St#st.next_delivery,
                                {D, St#st{next_delivery = D + 1,
                                          pending = maps:put(D, #{refs => Refs,
                                                                  partition => P,
                                                                  offset => O},
                                                             St#st.pending)}}
                        end,
            Header1 = Header#{<<"delivery_id">> => Id},
            %% CloudEvents Binary Mode round-trips through the same JSON
            %% envelope: every `ce-*' header the publisher supplied is echoed
            %% with the delivery, so a consumer can rebuild the event view.
            Header2 = maps:merge(Header1, ce_headers(Msg#broker_message.headers)),
            _ = send_binary(3, Header2, Msg#broker_message.payload, St1),
            Seen = lists:foldl(fun(Ref, Acc) -> maps:put({Ref, P}, O, Acc) end,
                               St1#st.seen, Refs),
            St1#st{seen = Seen}
    end.

already_seen(Refs, P, O, St) ->
    lists:any(
      fun(Ref) ->
              case maps:get({Ref, P}, St#st.seen, -1) of
                  Last when is_integer(Last) -> O =< Last;
                  _ -> false
              end
      end, Refs).

%%%_* Catch-up -------------------------------------------------------------

catch_up(St) ->
    lists:foldl(
      fun({Ref, #{qos := QoS}}, Acc) -> catch_up_ref(Ref, QoS, Acc) end,
      St, maps:to_list(St#st.subs)).

catch_up_ref(Ref, QoS, St) ->
    case maps:is_key(Ref, St#st.subs) of
        true ->
            lists:foldl(fun(P, Acc) -> catch_up_partition(Ref, QoS, P, Acc) end,
                        St, broker_queue:partition_ids());
        false ->
            St
    end.

catch_up_partition(Ref, QoS, P, St) ->
    case broker_subscription:cursor(Ref, P) of
        {ok, From} -> catch_up_pages(Ref, QoS, P, From, St, 0);
        _ -> St
    end.

catch_up_pages(_Ref, _QoS, _P, _From, St, N) when N >= 1000 ->
    St;
catch_up_pages(Ref, QoS, P, From, St, N) ->
    case (catch broker_queue:read_from(P, From, 100)) of
        {ok, [], _Next} ->
            St;
        {ok, Rows, _Next} when is_list(Rows) ->
            St1 = lists:foldl(
                    fun({O, Msg}, Acc) -> deliver([Ref], QoS, P, O, Msg, Acc) end,
                    St, Rows),
            case QoS of
                0 ->
                    lists:foreach(
                      fun({O, _}) ->
                              _ = (catch broker_subscription:ack(
                                               Ref, P, O, St1#st.client_id))
                      end, Rows),
                    ok;
                _ ->
                    ok
            end,
            Last = element(1, lists:last(Rows)),
            catch_up_pages(Ref, QoS, P, Last + 1, St1, N + 1);
        _ ->
            St
    end.

%%%_* Wire helpers ---------------------------------------------------------

send_json(Map, St) ->
    send_frame(1, json:encode(Map), St).

send_binary(Op, Header, Payload, St) ->
    Encoded = iolist_to_binary(json:encode(Header)),
    send_frame(2, <<Op:8, (byte_size(Encoded)):32, Encoded/binary, Payload/binary>>, St).

send_frame(Op, Payload, #st{socket = undefined} = St) ->
    _ = Payload,
    _ = Op,
    St;
send_frame(Op, Payload, St) ->
    _ = raw_send(ws_frame:encode(Op, Payload), St),
    St.

send_close(Code, Reason, St) ->
    _ = raw_send(ws_frame:encode(8, ws_frame:close_payload(Code, Reason)), St),
    ok.

raw_send(IoData, #st{socket = undefined}) ->
    _ = IoData,
    ok;
raw_send(IoData, #st{transport = T, socket = S}) ->
    T:send(S, IoData).

send_error(Code, Reason, St) ->
    send_json(#{<<"op">> => <<"error">>, <<"code">> => Code,
                <<"reason">> => Reason}, St).

set_active(#st{transport = T, socket = S}) ->
    T:setopts(S, [{active, once}]).

%%%_* Helpers --------------------------------------------------------------

decode_json(Bin) ->
    try {ok, json:decode(Bin)}
    catch _:Reason -> {error, Reason}
    end.

payload(Map) ->
    case {maps:get(<<"payload">>, Map, undefined),
          maps:get(<<"payload_b64">>, Map, undefined),
          maps:get(<<"payload_raw">>, Map, undefined)} of
        {undefined, undefined, undefined} -> {ok, <<>>};
        {undefined, undefined, Raw} when is_binary(Raw) -> {ok, Raw};
        {undefined, B64, undefined} when is_binary(B64) ->
            try {ok, base64:decode(B64)} catch _:_ -> {error, invalid_base64} end;
        {P, undefined, undefined} when is_binary(P) -> {ok, P};
        _ -> {error, ambiguous_payload}
    end.

payload_encoding(<<>>) -> raw;
payload_encoding(Payload) ->
    case unicode:characters_to_binary(Payload) of
        Payload -> text;
        _ -> binary
    end.

valid_qos(Q) -> Q =:= 0 orelse Q =:= 1.

valid_filter(<<>>) -> false;
valid_filter(Filter) ->
    %% `+' matches one level; `#' matches the rest. The same rules MQTT uses,
    %% expressed locally so the WS adapter does not depend on MQTT internals.
    try
        Levels = binary:split(Filter, <<"/">>, [global]),
        valid_levels(Levels, true)
    catch _:_ -> false
    end.

valid_levels([], _Last) -> true;
valid_levels([<<"#">>], true) -> true;
valid_levels([<<"#">> | _], _) -> false;
valid_levels([<<"+">> | Rest], Last) -> valid_levels(Rest, Last);
valid_levels([Level | Rest], Last) ->
    case binary:match(Level, [<<"+">>, <<"#">>]) of
        nomatch -> valid_levels(Rest, Last);
        _ -> false
    end.

message_id() ->
    base64:encode(crypto:strong_rand_bytes(12)).

reason_bin(Reason) when is_binary(Reason) -> Reason;
reason_bin(Reason) -> iolist_to_binary(io_lib:format("~p", [Reason])).

max_frame_bytes() ->
    application:get_env(broker_proto_ws, max_frame_bytes, 16777216).

max_publish_bytes() ->
    application:get_env(broker_proto_ws, max_publish_bytes, 1048576).

cleanup(#st{client_id = undefined}) ->
    ok;
cleanup(#st{client_id = ClientId, clean = Clean}) ->
    _ = (catch broker_subscription:detach(ClientId, self())),
    _ = Clean,
    ok.
