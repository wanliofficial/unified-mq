%% @doc NATS core protocol connection (S5, README §11.1).
%%
%% Server side of the NATS text protocol: INFO / CONNECT / PING / PONG /
%% PUB / SUB / UNSUB, with MSG deliveries. Subjects are mapped onto the
%% broker's `/'-separated routing filters (`.' -> `/', `*' -> `+',
%% `>' -> `#'); every publish is appended through `broker_router' ->
%% `broker_queue' (fsync) and every subscription is a normal
%% `broker_subscription' row. Queue groups map onto `shared' subscriptions,
%% so a message is delivered to exactly one member.
%%
%% Scope of this adapter (documented in `docs/protocol-compatibility.md'
%% §4.6):
%%   * PUB / SUB / UNSUB / PING / PONG, queue groups and request/reply (the
%%     reply subject travels in `proto_meta' and is echoed on MSG);
%%   * no NATS headers (HPUB/HMSG) and no JetStream -- core NATS only;
%%   * core NATS is at-most-once: no ack handshake. The broker still stores
%%     each publish durably (a strictly stronger guarantee than NATS core)
%%     and the adapter acks its own cursor right after the MSG write.
-module(nats_conn).

-behaviour(gen_server).

-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(MAX_LINE, 65536).
-define(DEFAULT_TICK, 100).

-record(st, {
    ref :: term(),
    transport :: module() | undefined,
    socket :: term() | undefined,
    buffer = <<>> :: binary(),
    handshake = pending :: pending | done,
    client_id :: binary() | undefined,
    tenant = <<"t0">> :: binary(),
    verbose = false :: boolean(),
    echo = true :: boolean(),
    attached = false :: boolean(),
    %% Sid => #{ref := sub_ref(), filter := binary(), subject := binary(),
    %%           queue := binary() | undefined,
    %%           remaining := infinity | pos_integer()}
    subs = #{} :: map(),
    pending_pub = undefined :: undefined | {binary(), binary() | undefined, non_neg_integer()},
    tick :: reference() | undefined,
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
    St1 = St#st{transport = Transport, socket = Socket, handshake = done},
    send_info(St1),
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
handle_info({broker_deliver, ClientId, _MaxQoS, Refs, {P, O, Msg}},
            #st{client_id = ClientId} = St) ->
    {noreply, deliver_wake(Refs, P, O, Msg, St)};
handle_info(shared_tick, St) ->
    {noreply, arm_tick(pull_all_shared(St#st{tick = undefined}))};
handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Input path ------------------------------------------------------------

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

parse(Buffer, #st{pending_pub = {Subject, Reply, Size}} = St) ->
    Need = Size + 2,
    case byte_size(Buffer) of
        N when N >= Need ->
            <<Payload:Size/binary, Tail:2/binary, Rest/binary>> = Buffer,
            case Tail of
                <<"\r\n">> ->
                    St1 = St#st{pending_pub = undefined},
                    case handle_pub(Subject, Reply, Payload, St1) of
                        {noreply, St2} -> parse(Rest, St2);
                        Stop -> Stop
                    end;
                _ ->
                    err_close(<<"Protocol Violation">>, St)
            end;
        _ ->
            {noreply, St#st{buffer = Buffer}}
    end;
parse(Buffer, St) when byte_size(Buffer) > ?MAX_LINE ->
    _ = Buffer,
    err_close(<<"Maximum Payload Violation">>, St);
parse(Buffer, St) ->
    case binary:split(Buffer, <<"\r\n">>) of
        [Line, Rest] ->
            case handle_line(Line, St) of
                {noreply, St1} -> parse(Rest, St1);
                Stop -> Stop
            end;
        [_Partial] ->
            {noreply, St#st{buffer = Buffer}}
    end.

handle_line(<<"PING">>, St) ->
    {noreply, send_raw(<<"PONG\r\n">>, St)};
handle_line(<<"PONG">>, St) ->
    {noreply, St};
handle_line(<<"CONNECT ", Json/binary>>, St) ->
    handle_connect(Json, St);
handle_line(<<"SUB ", Rest/binary>>, St) ->
    handle_sub(Rest, St);
handle_line(<<"UNSUB ", Rest/binary>>, St) ->
    handle_unsub(Rest, St);
handle_line(<<"PUB ", Rest/binary>>, St) ->
    handle_pub_args(Rest, St);
handle_line(<<"HPUB ", _/binary>>, St) ->
    %% Headers are a NATS 2.2 extension; v1 of this adapter is core NATS.
    err_close(<<"Unknown Protocol Operation">>, St);
handle_line(<<>>, St) ->
    {noreply, St};
handle_line(_Line, St) ->
    err_close(<<"Unknown Protocol Operation">>, St).

%%%_* CONNECT --------------------------------------------------------------

handle_connect(Json, #st{client_id = Existing} = St) when Existing =/= undefined ->
    _ = Json,
    err_close(<<"Protocol Violation">>, St);
handle_connect(Json, St) ->
    try json:decode(Json) of
        Map when is_map(Map) ->
            case check_auth(Map) of
                ok ->
                    St1 = ensure_client(St#st{verbose = maps:get(<<"verbose">>, Map, false),
                                              echo = maps:get(<<"echo">>, Map, true)}),
                    maybe_ok(St1);
                {error, _} ->
                    err_close(<<"Authorization Violation">>, St)
            end;
        _ ->
            err_close(<<"Protocol Violation">>, St)
    catch
        _:_ ->
            err_close(<<"Protocol Violation">>, St)
    end.

check_auth(Map) ->
    case application:get_env(broker_proto_nats, auth_token, undefined) of
        undefined ->
            ok;
        Token when is_binary(Token) ->
            case maps:get(<<"auth_token">>, Map, undefined) of
                Token -> ok;
                _ -> {error, unauthorized}
            end
    end.

ensure_client(#st{client_id = undefined} = St) ->
    ClientId = <<"nats-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    {ok, _Prev} = broker_subscription:attach(ClientId, self(), false),
    arm_tick(St#st{client_id = ClientId, attached = true});
ensure_client(St) ->
    St.

%%%_* SUB / UNSUB ----------------------------------------------------------

handle_sub(Rest, St) ->
    case connected(St) of
        false ->
            err_close(<<"Protocol Violation">>, St);
        true ->
            case binary:split(Rest, <<" ">>, [global]) of
                [Subject, Sid] ->
                    do_sub(Subject, undefined, Sid, St);
                [Subject, Queue, Sid] ->
                    do_sub(Subject, Queue, Sid, St);
                _ ->
                    err_close(<<"Protocol Violation">>, St)
            end
    end.

do_sub(Subject, <<>>, Sid, St) ->
    do_sub(Subject, undefined, Sid, St);
do_sub(Subject, Queue, Sid, St) ->
    case {valid_subject(Subject, filter), valid_sid(Sid), Queue} of
        {true, true, _} ->
            case translate_filter(Subject) of
                {ok, Filter} ->
                    St1 = remove_sid(Sid, St),
                    case open_subscription(Filter, Subject, Queue, St1) of
                        {ok, Ref} ->
                            Sub = #{ref => Ref, filter => Filter, subject => Subject,
                                    queue => Queue, remaining => infinity},
                            maybe_ok(St1#st{subs = (St1#st.subs)#{Sid => Sub}});
                        {error, Reason} ->
                            err_close(reason_bin(Reason), St1)
                    end
            end;
        {false, _, _} ->
            err_close(<<"Invalid Subject">>, St);
        {_, false, _} ->
            err_close(<<"Protocol Violation">>, St)
    end.

open_subscription(Filter, _Subject, undefined, St) ->
    broker_subscription:subscribe(St#st.client_id, Filter, 1);
open_subscription(Filter, _Subject, Queue, St) ->
    broker_subscription:join(shared, Queue, St#st.client_id, Filter, 1).

handle_unsub(Rest, St) ->
    case binary:split(Rest, <<" ">>, [global]) of
        [Sid] ->
            {noreply, remove_sid(Sid, St)};
        [Sid, MaxBin] ->
            case catch binary_to_integer(MaxBin) of
                Max when is_integer(Max), Max >= 0 ->
                    set_remaining(Sid, Max, St);
                _ ->
                    err_close(<<"Protocol Violation">>, St)
            end;
        _ ->
            err_close(<<"Protocol Violation">>, St)
    end.

set_remaining(Sid, 0, St) ->
    {noreply, remove_sid(Sid, St)};
set_remaining(Sid, Max, St) ->
    case maps:find(Sid, St#st.subs) of
        {ok, Sub} ->
            {noreply, St#st{subs = (St#st.subs)#{Sid => Sub#{remaining => Max}}}};
        error ->
            {noreply, St}
    end.

%%%_* PUB ------------------------------------------------------------------

handle_pub_args(Rest, St) ->
    case connected(St) of
        false ->
            err_close(<<"Protocol Violation">>, St);
        true ->
            case binary:split(Rest, <<" ">>, [global]) of
                [Subject, SizeBin] ->
                    pub_pending(Subject, undefined, SizeBin, St);
                [Subject, Reply, SizeBin] ->
                    pub_pending(Subject, Reply, SizeBin, St);
                _ ->
                    err_close(<<"Protocol Violation">>, St)
            end
    end.

pub_pending(Subject, Reply, SizeBin, St) ->
    case {valid_subject(Subject, publish), valid_sid(Reply), catch binary_to_integer(SizeBin)} of
        {true, true, Size} when is_integer(Size), Size >= 0 ->
            case Size > max_payload() of
                true ->
                    err_close(<<"Maximum Payload Violation">>, St);
                false ->
                    {noreply, St#st{pending_pub = {Subject, Reply, Size}}}
            end;
        _ ->
            err_close(<<"Protocol Violation">>, St)
    end.

handle_pub(Subject, Reply, Payload, St) ->
    case publish(Subject, Reply, Payload, St) of
        ok ->
            maybe_ok(St);
        {error, Reason} ->
            err_close(reason_bin(Reason), St)
    end.

publish(Subject, Reply, Payload, St) ->
    Destination = subject_to_destination(Subject),
    Base = #{<<"publisher">> => St#st.client_id},
    Meta = case Reply of
               undefined -> #{<<"nats">> => Base};
               <<>> -> #{<<"nats">> => Base};
               _ -> #{<<"nats">> => Base#{<<"reply">> => Reply}}
           end,
    Msg = #broker_message{message_id = message_id(),
                          tenant_id = St#st.tenant,
                          destination = Destination,
                          partition_id = 0,
                          payload = Payload,
                          payload_encoding = payload_encoding(Payload),
                          headers = #{},
                          proto_meta = Meta,
                          persistence = durable,
                          reliability = fsync,
                          created_at = erlang:system_time(millisecond),
                          expires_at = undefined,
                          event = undefined},
    Ctx = #route_ctx{tenant_id = St#st.tenant,
                     publisher = St#st.client_id,
                     reliability = fsync},
    case broker_router:env() of
        {ok, Env} ->
            Backend = application:get_env(broker_router, backend, broker_router_hash),
            case Backend:route(Destination, Ctx, Env) of
                {ok, Routes} ->
                    store_routes(Routes, Msg);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

store_routes([], _Msg) ->
    ok;
store_routes([{P, Refs} | Rest], Msg) ->
    PartitionMsg = Msg#broker_message{partition_id = P},
    case (catch broker_queue:append(P, PartitionMsg, fsync)) of
        {ok, Offset} ->
            _ = (catch broker_subscription:dispatch(Refs, {P, Offset, PartitionMsg})),
            store_routes(Rest, Msg);
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, {append_crashed, Other}}
    end.

%%%_* Delivery -------------------------------------------------------------

deliver_wake(Refs, P, O, Msg, St) ->
    Mine = [Ref || Ref <- Refs, ref_is_mine(Ref, St)],
    {Exclusive, Shared} = lists:partition(fun(Ref) -> element(1, Ref) =:= exclusive end, Mine),
    St1 = lists:foldl(fun(Ref, Acc) -> deliver_exclusive(Ref, P, O, Msg, Acc) end,
                      St, Exclusive),
    lists:foldl(fun(Ref, Acc) -> claim_and_pull(P, Ref, Acc) end, St1, Shared).

ref_is_mine(Ref, St) ->
    lists:any(fun(#{ref := R}) -> R =:= Ref end, maps:values(St#st.subs)).

deliver_exclusive(Ref, P, O, Msg, St) ->
    case not_yet_read(Ref, P, O) andalso should_echo(Msg, St) of
        false ->
            St;
        true ->
            Sids = [Sid || {Sid, #{ref := R}} <- maps:to_list(St#st.subs), R =:= Ref],
            St1 = lists:foldl(fun(Sid, Acc) -> deliver_to_sid(Sid, P, O, Msg, Acc) end,
                              St, Sids),
            case Sids of
                [] -> St1;
                _ -> _ = (catch broker_subscription:ack(Ref, P, O, St1#st.client_id)), St1
            end
    end.

deliver_to_sid(Sid, _P, _O, Msg, St) ->
    case maps:find(Sid, St#st.subs) of
        {ok, _Sub} ->
            SubjectBin = subject_out(Msg#broker_message.destination),
            Reply = reply_of(Msg),
            _ = send_msg(SubjectBin, Sid, Reply, Msg#broker_message.payload, St),
            after_send(Sid, St);
        error ->
            St
    end.

%% CONNECT `echo': a client that asked not to receive its own publishes must
%% not get them back, even though the message still went through storage.
should_echo(Msg, #st{echo = true}) ->
    _ = Msg,
    true;
should_echo(Msg, #st{client_id = ClientId}) ->
    publisher_of(Msg) =/= ClientId.

publisher_of(#broker_message{proto_meta = #{<<"nats">> := #{<<"publisher">> := P}}}) ->
    P;
publisher_of(_Msg) ->
    undefined.

%% Shared (queue group) rows are claim-based: the worker hands out disjoint
%% offsets, the adapter reads exactly those and acks immediately (core NATS has
%% no ack handshake). Readers that lose the race get `{ok, []}'.
claim_and_pull(_P, _Ref, St) when St#st.client_id =:= undefined ->
    St;
claim_and_pull(P, Ref, St) ->
    case (catch broker_subscription:claim(Ref, P, 32, St#st.client_id)) of
        {ok, []} ->
            St;
        {ok, Offsets} ->
            {Read, Missing} = read_offsets(P, Offsets),
            Filter = element(3, Ref),
            St1 = lists:foldl(
                    fun({O, Msg}, Acc) ->
                            case Msg#broker_message.destination =:= Filter of
                                true ->
                                    deliver_shared(P, Ref, O, Msg, Acc);
                                false ->
                                    %% Another NATS subject's message shares this
                                    %% partition. A shared cursor scans the whole
                                    %% partition, so consume the offset for this
                                    %% group without delivering it.
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

deliver_shared(P, Ref, O, Msg, St) ->
    case should_echo(Msg, St) of
        false ->
            %% Suppressed echo: the offset is consumed (nature of at-most-once),
            %% so advance the cursor instead of leaving it claimed forever.
            _ = (catch broker_subscription:ack(Ref, P, O, St#st.client_id)),
            St;
        true -> deliver_shared2(P, Ref, O, Msg, St)
    end.

deliver_shared2(P, Ref, O, Msg, St) ->
    Sids = [Sid || {Sid, #{ref := R}} <- maps:to_list(St#st.subs), R =:= Ref],
    St1 = lists:foldl(fun(Sid, Acc) -> deliver_to_sid(Sid, P, O, Msg, Acc) end, St, Sids),
    _ = (catch broker_subscription:ack(Ref, P, O, St1#st.client_id)),
    St1.

not_yet_read(Ref, P, O) ->
    case broker_subscription:cursor(Ref, P) of
        {ok, Cursor} -> O >= Cursor;
        _ -> false
    end.

%% A shared member may have missed the wake (it lost the claim race, or the
%% append's wake arrived while its page was empty). The tick retries the tail.
pull_all_shared(#st{client_id = undefined} = St) ->
    St;
pull_all_shared(St) ->
    Refs = lists:usort([Ref || #{ref := Ref} <- maps:values(St#st.subs),
                               element(1, Ref) =:= shared]),
    lists:foldl(
      fun(Ref, Acc) ->
              lists:foldl(fun(P, Acc2) -> claim_and_pull(P, Ref, Acc2) end,
                          Acc, broker_queue:partition_ids())
      end, St, Refs).

%%%_* SID bookkeeping -------------------------------------------------------

after_send(Sid, St) ->
    case maps:find(Sid, St#st.subs) of
        {ok, #{remaining := infinity}} ->
            St;
        {ok, #{remaining := 1}} ->
            remove_sid(Sid, St);
        {ok, #{remaining := N} = Sub} when is_integer(N), N > 1 ->
            St#st{subs = (St#st.subs)#{Sid => Sub#{remaining => N - 1}}};
        _ ->
            St
    end.

%% Removing a SID also drops its subscription when no other SID still maps to
%% the same SubRef (NATS permits several SIDs on one subject).
remove_sid(Sid, St) ->
    case maps:take(Sid, St#st.subs) of
        {#{ref := Ref}, Rest} ->
            St1 = St#st{subs = Rest},
            case lists:any(fun(#{ref := R}) -> R =:= Ref end, maps:values(Rest)) of
                true -> St1;
                false -> drop_ref(Ref, St1)
            end;
        error ->
            St
    end.

drop_ref({exclusive, _ClientId, Filter}, St) ->
    _ = (catch broker_subscription:unsubscribe(St#st.client_id, Filter)),
    St;
drop_ref({shared, _Group, _Filter} = Ref, St) ->
    _ = (catch broker_subscription:leave(Ref, St#st.client_id)),
    St.

%%%_* Wire helpers ---------------------------------------------------------

send_info(St) ->
    Info = #{<<"server_id">> => server_id(),
             <<"server_name">> => <<"unifiedmq">>,
             <<"version">> => <<"0.1.0">>,
             <<"proto">> => 1,
             <<"host">> => <<"127.0.0.1">>,
             <<"port">> => listener_port(St),
             <<"headers">> => false,
             <<"auth_required">> => auth_required(),
             <<"max_payload">> => max_payload()},
    send_raw([<<"INFO ">>, json:encode(Info), <<"\r\n">>], St).

listener_port(#st{ref = Ref}) ->
    case catch ranch:get_port(Ref) of
        Port when is_integer(Port) -> Port;
        _ -> 0
    end.

send_msg(Subject, Sid, Reply, Payload, St) ->
    ReplyPart = case Reply of
                    undefined -> [];
                    <<>> -> [];
                    _ -> [<<" ">>, Reply]
                end,
    send_raw([<<"MSG ">>, Subject, <<" ">>, Sid, ReplyPart, <<" ">>,
              integer_to_binary(byte_size(Payload)), <<"\r\n">>, Payload, <<"\r\n">>], St).

maybe_ok(#st{verbose = true} = St) ->
    {noreply, send_raw(<<"+OK\r\n">>, St)};
maybe_ok(St) ->
    {noreply, St}.

err_close(Reason, St) ->
    _ = send_raw([<<"-ERR '">>, Reason, <<"'\r\n">>], St),
    {stop, normal, St#st{closing = true}}.

send_raw(IoData, #st{socket = undefined} = St) ->
    _ = IoData,
    St;
send_raw(IoData, #st{transport = T, socket = S} = St) ->
    _ = T:send(S, IoData),
    St.

set_active(#st{transport = T, socket = S}) ->
    T:setopts(S, [{active, once}]).

%%%_* Subjects -------------------------------------------------------------

connected(#st{client_id = undefined}) -> false;
connected(#st{attached = Attached}) -> Attached.

%% Publish subjects are concrete tokens; subscription subjects may use the
%% NATS wildcards `*' (one token) and `>' (rest, last only).
valid_subject(<<>>, _Mode) ->
    false;
valid_subject(Subject, Mode) ->
    Tokens = binary:split(Subject, <<".">>, [global]),
    valid_tokens(Tokens, Mode).

valid_tokens([], _Mode) -> true;
valid_tokens([<<>> | _], _Mode) -> false;
valid_tokens([Token | Rest], Mode) ->
    case Token of
        <<"*">> ->
            Mode =:= filter andalso valid_tokens(Rest, Mode);
        <<">">> ->
            Mode =:= filter andalso Rest =:= [];
        _ ->
            case binary:match(Token, [<<"*">>, <<">">>, <<" ">>, <<"\r">>, <<"\n">>]) of
                nomatch -> valid_tokens(Rest, Mode);
                _ -> false
            end
    end.

%% NATS subject -> broker filter: `.' separator, `*' -> `+', `>' -> `#'.
%% The broker's MQTT `#' also matches the parent subject (`foo.>' in NATS does
%% not match `foo'); this is recorded as a conversion difference in the
%% compatibility document rather than hidden.
translate_filter(Subject) ->
    Tokens = binary:split(Subject, <<".">>, [global]),
    translate_tokens(Tokens, []).

translate_tokens([], Acc) ->
    {ok, iolist_to_binary(lists:join(<<"/">>, lists:reverse(Acc)))};
translate_tokens([<<"*">> | Rest], Acc) ->
    translate_tokens(Rest, [<<"+">> | Acc]);
translate_tokens([<<">">>], Acc) ->
    {ok, iolist_to_binary(lists:join(<<"/">>, lists:reverse([<<"#">> | Acc])))};
translate_tokens([Token | Rest], Acc) ->
    translate_tokens(Rest, [Token | Acc]).

subject_to_destination(Subject) ->
    binary:replace(Subject, <<".">>, <<"/">>, [global]).

subject_out(Destination) ->
    binary:replace(Destination, <<"/">>, <<".">>, [global]).

reply_of(#broker_message{proto_meta = #{<<"nats">> := #{<<"reply">> := Reply}}})
  when is_binary(Reply), Reply =/= <<>> ->
    Reply;
reply_of(_Msg) ->
    undefined.

payload_encoding(<<>>) -> raw;
payload_encoding(Payload) ->
    case unicode:characters_to_binary(Payload) of
        Payload -> text;
        _ -> binary
    end.

%%%_* Config / misc --------------------------------------------------------

arm_tick(#st{} = St) ->
    case St#st.tick of
        undefined ->
            St#st{tick = erlang:send_after(poll_ms(), self(), shared_tick)};
        _ ->
            St
    end.

poll_ms() ->
    application:get_env(broker_proto_nats, shared_poll_ms, ?DEFAULT_TICK).

max_payload() ->
    application:get_env(broker_proto_nats, max_payload, 1048576).

auth_required() ->
    application:get_env(broker_proto_nats, auth_token, undefined) =/= undefined.

server_id() ->
    application:get_env(broker_proto_nats, server_id, <<"unifiedmq-nats">>).

valid_sid(undefined) -> true;
valid_sid(<<>>) -> false;
valid_sid(Sid) when is_binary(Sid) ->
    binary:match(Sid, [<<" ">>, <<"\r">>, <<"\n">>]) =:= nomatch.

message_id() ->
    base64:encode(crypto:strong_rand_bytes(12)).

reason_bin(Reason) when is_binary(Reason) -> Reason;
reason_bin(Reason) -> iolist_to_binary(io_lib:format("~p", [Reason])).

cleanup(#st{} = St) ->
    cancel_tick(St),
    case St#st.client_id of
        undefined -> ok;
        ClientId -> _ = (catch broker_subscription:detach(ClientId, self())), ok
    end.

cancel_tick(#st{tick = undefined}) -> ok;
cancel_tick(#st{tick = Ref}) -> _ = erlang:cancel_timer(Ref), ok.
