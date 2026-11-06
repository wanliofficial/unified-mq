%% @doc One MQTT 3.1.1 connection (design §5, §7; ADR-010). gen_statem — the
%% protocol is a hand-shake-driven machine, not a request/reply loop.
%%
%% Callback mode: `handle_event_function' + `state_enter'. `awaiting_connect'
%% and `connected' share one event matrix (they differ only in the
%% first-frame gate), so the state name is a parameter rather than three copies
%% of the same `case'.
%%
%% States:
%%   awaiting_owner — started with the socket still owned by the acceptor/test
%%       helper; the only input is the `connect' event, sent after
%%       `controlling_process' completed. Touching the socket before that is
%%       `not_owner' — hence the state exists at all.
%%   awaiting_connect — armed; the first frame must be CONNECT (§3.1).
%%   connected — session live, and the dispatcher is the pull loop inside this
%%       process (K9: a positioned `{broker_deliver, ...}' is a WAKE; cursor/2
%%       is the only read position (J6)).
%%
%% Hard contracts staked here: PUBACK only after the fsync append returned,
%% dispatch after that and never rolled back (R1a, §5.1(e)/(f)); §4.8 delivery
%% QoS = min(message publish level, max QoS of the subscriptions that HIT);
%% `mqtt_max_qos' derived from the backend capability at CONNECT (E2) and
%% executed at SUBACK/PUBLISH (E12); §4.5's rejection posture (MQTT 5 -> CONNACK
%% 0x87, QoS2/over-max PUBLISH -> close without ack, retain -> strip and log,
%% Will -> ignore and log); takeover through attach/3's `{ok, PrevPid}' with the
%% K10 pid guard as backstop.
%%
%% Stopping: a peer that broke the protocol or got refused is a fact of life and
%% stops with reason `normal' (the `log/3' call carries the detail) — otherwise
%% every fuzz case and every wrong password would print a crash report. Reasons
%% that are OUR fault (`{setopts_failed, _}', `{append_failed, _}') keep their
%% tuple so the supervisor log shows them.
-module(mqtt_conn_fsm).
-behaviour(gen_statem).

-export([start_link/2]).
-export([init/1, callback_mode/0, handle_event/4, terminate/3, code_change/4]).

-include("mqtt_frame.hrl").
-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-record(st, {socket,
             opts = #{},
             buf = <<>>,
             poll_ref = undefined,
             snapshot_ref = undefined,
             round_ref = undefined,
             ka_ref = undefined,           %% {Ref, TRef} | undefined
             partitions = [],
             max_qos = 1,                  %% effective, capability-derived (E2)
             principal = undefined,
             tenant_id = <<"default">>,
             client_id = undefined,
             keepalive_ms = 0,
             attached = false,
             persist = false,              %% = not clean_session (§4.2)
             round_no = 0,
             next_pid = 1,
             pending = #{},                %% Pid => {[SubRef], {Partition, Offset}}
             pending_rev = #{},            %% {Partition, Offset} => Pid
             sent = #{},                   %% Pid => true, SENT BY THIS CONNECTION
             inflight_dirty = false}).

%%%_* API ---------------------------------------------------------------------

start_link(Socket, Opts) ->
    gen_statem:start_link(?MODULE, {Socket, Opts}, []).

%%%_* gen_statem --------------------------------------------------------------

callback_mode() -> [handle_event_function, state_enter].

init({Socket, Opts}) ->
    {ok, awaiting_owner, #st{socket = Socket, opts = Opts}}.

%% -- state entry: the poll sentinel and the keepalive timer live only in
%% `connected'; entering it is the first moment a session can lose a wakeup.
handle_event(enter, _Old, awaiting_owner, D) -> {keep_state, D, []};
handle_event(enter, _Old, awaiting_connect, D) -> {keep_state, D, []};
handle_event(enter, _Old, connected, D) ->
    {keep_state, arm_poll(connected, ka_arm(connected, D)), []};

%% -- the socket handoff (T12's ranch on_accept / the test helper's `! connect')
%% No event-type guard: a plain `Pid ! connect' from another process is an
%% `info' event to gen_statem (only `gen_statem:cast/2' wraps into `$gen_cast'),
%% and ranch's side may hand over with either. The state machine is the gate.
handle_event(_Et, connect, awaiting_owner, #st{socket = Sock} = D) ->
    arm_socket(Sock, D);
%% ranch 2.x calls `ranch_protocol:start_link/3' BEFORE it owns the connection
%% socket, then transfers control and sends this message. The conn is started by
%% `mqtt_conn_sup:start_conn/2' with no socket (T12), so this is where it first
%% becomes safe to touch it -- the same point the test helper reaches with
%% `connect' after its own `controlling_process'.
handle_event(_Et, {handshake, _Ref, _Transport, Sock, _Timeout}, awaiting_owner, D) ->
    arm_socket(Sock, D#st{socket = Sock});

%% -- transport death is legal in every live state
handle_event(_Et, {tcp_closed, _}, _StateName, D) -> {stop, normal, D};
handle_event(_Et, {tcp_error, _, Reason}, _StateName, D) ->
    log(warning, "socket error ~p", [Reason]),
    {stop, normal, D};

%% -- anything before the handoff cannot be served (no socket ownership yet)
handle_event(_Et, Content, awaiting_owner, D) ->
    log(warning, "event before socket handoff: ~p", [Content]),
    {keep_state, D};

handle_event(_Et, Content, StateName, D0) ->
    case event(Content, StateName, D0) of
        {state, StateName, D1} ->
            %% Any processed event re-arms the poll sentinel -- whose tick just
            %% cleared its own ref, so re-arming here is the only thing that
            %% keeps a quiet connection from falling asleep for good. The
            %% keepalive deadline is NOT re-armed here: `event/3`'s {tcp,...}
            %% clause owns that, see ka_arm/2.
            {keep_state, arm_poll(StateName, D1)};
        {state, NextState, D1} ->
            {next_state, NextState, D1};
        {stop, Reason, D1} ->
            {stop, Reason, D1}
    end.

%%%_* Event matrix ------------------------------------------------------------

event({tcp, _Sock, Data}, StateName, #st{buf = Buf} = D0) ->
    D = D0#st{buf = <<Buf/binary, Data/binary>>},
    case tcp_frames(D, StateName) of
        {state, S, D1} ->
            %% exactly one re-arm per {tcp,...} delivery, including the path
            %% that consumed several frames -- arming per frame would deliver
            %% the same buffer twice.
            active_once(D1),
            {state, S, ka_arm(S, D1)};
        {stop, Reason, D1} ->
            {stop, Reason, D1}
    end;
event(connect, awaiting_connect, D) ->
    %% duplicated handoff: the socket is already armed
    {state, awaiting_connect, D};
event({new_messages, _P, _From, _To}, StateName, D) ->
    %% J6: a wake, nothing more -- the range in it is not a delivery contract.
    wake(StateName, D);
event({broker_deliver, _ClientId, _MaxQoS, _Refs, {undefined, undefined, Msg}},
      connected, D) ->
    %% K9: the only real push. QoS0 publishes never enter the log (§4.4), so
    %% there is no offset to pull and no ack to wait for: sending at QoS1 here
    %% would strand a pending entry keyed by `undefined'.
    {state, connected, send_frame(publish_bin(0, 0, Msg, undefined), D)};
event({broker_deliver, _ClientId, _MaxQoS, _Refs, {_P, _O, _Msg}}, StateName, D) ->
    %% a wake for a durable position; the tuple's Msg is not read -- the pull
    %% loop is the only reader (J6/K9).
    wake(StateName, D);
event({mqtt_takeover, NewPid}, _StateName, D) ->
    %% §4.3: we are the old connection. Close WITHOUT writing the blob -- the
    %% new conn owns the session now, and a stale blob behind it would
    %% resurrect our view of the world on the next reconnect.
    log(info, "taken over by ~p", [NewPid]),
    {stop, takeover, D};
event({poll_t, Ref}, connected, #st{poll_ref = Ref} = D) ->
    pull_round(D#st{poll_ref = undefined});
event({poll_t, _Stale}, StateName, D) ->
    {state, StateName, D};
event({round_t, Ref}, connected, #st{round_ref = Ref} = D) ->
    pull_round(D#st{round_ref = undefined});
event({round_t, _Stale}, StateName, D) ->
    {state, StateName, D};
event({snapshot_t, Ref}, StateName, #st{snapshot_ref = Ref} = D) ->
    {state, StateName, snapshotted(D#st{snapshot_ref = undefined})};
event({snapshot_t, _Stale}, StateName, D) ->
    {state, StateName, D};
event({ka_t, Ref}, _StateName, #st{ka_ref = {Ref, _}} = D) ->
    log(warning, "keepalive timeout (client_id=~p)", [D#st.client_id]),
    {stop, normal, D#st{ka_ref = undefined}};
event({ka_t, _Stale}, StateName, D) ->
    {state, StateName, D};
event(#connect{} = C, awaiting_connect, D) ->
    handle_connect(C, D);
event(#publish{qos = QoS, topic = Topic}, connected, #st{max_qos = Max} = D)
  when QoS > Max ->
    %% §4.5 + E12: close, no PUBACK. QoS 2 never reaches here as a record (the
    %% codec rejects the reserved QoS bits), so in practice this is a QoS1
    %% PUBLISH on a session whose effective max is 0.
    log(warning, "mqtt_qos2_unsupported_or_above_max: qos=~p max=~p topic=~p",
        [QoS, Max, Topic]),
    {stop, normal, D};
event(#publish{retain = 1} = P, connected, D) ->
    log(info, "mqtt_retain_ignored", []),
    event(P#publish{retain = 0}, connected, D);
event(#publish{} = P, connected, D) ->
    handle_publish(P, D);
event(#subscribe{} = S, connected, D) ->
    handle_subscribe(S, D);
event(#unsubscribe{} = U, connected, D) ->
    handle_unsubscribe(U, D);
event(#puback{packet_id = Pid}, connected, D) ->
    handle_puback(Pid, D);
event(#ping{kind = req}, connected, D) ->
    {state, connected, send_frame(mqtt_frame:pingresp(), D)};
event(#disconnect{}, connected, D) ->
    %% §4.15: a normal stop. The Will is not published (S1 has none, §4.5) and
    %% the in-flight blob stays for the reconnect.
    log(info, "client disconnect (client_id=~p)", [D#st.client_id]),
    {stop, normal, D};
event(Frame, awaiting_connect, D) when is_tuple(Frame) ->
    %% §3.1: a frame before CONNECT is a protocol violation, not a retryable
    %% condition -- the stream position is unknown to us.
    log(warning, "first frame is not CONNECT: ~p", [element(1, Frame)]),
    {stop, normal, D};
event(#connect{}, connected, D) ->
    log(warning, "CONNECT on an established session", []),
    {stop, normal, D};
event(Other, StateName, D) ->
    log(warning, "unexpected event in ~p: ~p", [StateName, Other]),
    {state, StateName, D}.

%%%_* TCP framing ---------------------------------------------------------------

%% Drain every whole frame the buffer holds. Frames arriving after CONNECT was
%% answered (MQTT allows pipelining SUBSCRIBE behind it) are dispatched through
%% the same loop with the new state name.
tcp_frames(#st{buf = Buf} = D, StateName) ->
    case (catch mqtt_frame:decode(Buf)) of
        {ok, Frame, Rest} ->
            case event(Frame, StateName, D#st{buf = Rest}) of
                {state, S1, D1} -> tcp_frames(D1, S1);
                {stop, Reason, D1} -> {stop, Reason, D1}
            end;
        {more, _} ->
            {state, StateName, D};
        {error, Reason} ->
            decode_error(Reason, StateName, D);
        {'EXIT', Reason} ->
            %% decode/1 promises never to crash; if that ever breaks, the
            %% connection dies and the codec's suite says why.
            {stop, {decode_crashed, Reason}, D}
    end.

%% §4.5's version posture is fed by the codec's distinct reasons: an MQTT 5
%% CONNECT has no 3.1.1 body shape, so it is refused at parse time and the
%% CONNACK code is derived here rather than from a #connect{} record.
%%
%% The refusal MUST be framed for the version the client asked for. A v5 client
%% parses CONNACK as `flags|reason|properties-length', so the 4-byte 3.1.1 shape
%% (`20 02 00 87') makes paho-mqtt 2.x raise inside its properties decoder
%% instead of surfacing the refusal -- the client never learns why (T15 M16).
%% Five bytes, zero properties: `20 03 00 87 00'. 3.1.1 stays byte-for-byte as
%% before for every other version (mqtt_frame:connack/2).
decode_error({unacceptable_protocol_version, 5}, awaiting_connect, D) ->
    log(warning, "mqtt 5.0 connect refused", []),
    {stop, normal, send_frame(<<16#20, 3, 0, 16#87, 0>>, D)};
decode_error({unsupported_protocol_version, Other}, awaiting_connect, D) ->
    log(warning, "unsupported protocol level ~p", [Other]),
    refuse(1, D);
decode_error({unsupported_protocol, Name}, awaiting_connect, D) ->
    log(warning, "unsupported protocol name ~p", [Name]),
    refuse(1, D);
decode_error(Reason, _StateName, D) ->
    log(warning, "malformed packet ~p", [Reason]),
    {stop, normal, D}.

%% The one arming point: both handoffs (the test helper's `connect' after its
%% own `controlling_process', and ranch's `{handshake, ...}') end up here.
arm_socket(Sock, D) when Sock =/= undefined ->
    case inet:setopts(Sock, [{active, once}]) of
        ok -> {next_state, awaiting_connect, D};
        {error, Reason} -> {stop, {setopts_failed, Reason}, D}
    end.

active_once(#st{socket = Sock}) ->
    _ = inet:setopts(Sock, [{active, once}]),
    ok.

%%%_* CONNECT -----------------------------------------------------------------

handle_connect(#connect{client_id = <<>>}, D0) ->
    %% §3.1.3.1: an empty ClientID is representable on the wire; this S1 session
    %% table is keyed by it, so it is rejected rather than given a generated id
    %% (that would make "who owns this session" unanswerable).
    log(warning, "identifier rejected (empty client_id)", []),
    refuse(2, D0);
handle_connect(#connect{clean_session = CSbit} = C, D0) ->
    case authenticate(C, D0#st{client_id = C#connect.client_id}) of
        {error, Code, D} ->
            refuse(Code, D);
        {ok, D1} ->
            %% §4.5: the Will is parsed by the codec and dropped here -- no
            %% storage, no publish on abnormal disconnect, nothing to validate.
            _ = case C#connect.will_flag of
                    1 -> log(info, "mqtt_will_ignored", []);
                    0 -> ok
                end,
            Clean = CSbit =:= 1,
            %% session_present describes the session the client is about to get
            %% back, so it has to be read before the clean-session teardown.
            SP = session_present(Clean, C#connect.client_id),
            D2 = attach_session(C#connect.client_id, Clean, D1),
            D3 = case Clean of
                     true -> clean_session_teardown(D2);
                     false -> restore_blob(D2)
                 end,
            case subscribe_partitions(D3) of
                {ok, D4} ->
                    log(info, "session up: client_id=~p principal=~p "
                              "clean=~p session_present=~p max_qos=~p "
                              "keepalive_ms=~p",
                        [C#connect.client_id, D4#st.principal, Clean, SP,
                         D4#st.max_qos, D4#st.keepalive_ms]),
                    %% E7/R7: the connect-time round is what serves a
                    %% reconnecting session its backlog and its DUP
                    %% resends -- leaving it to the poll tick would make
                    %% a catch-up arrive up to pull_poll_interval_ms late
                    %% even though we know, right here, that the session
                    %% may have unread state.
                    schedule_round(
                      send_frame(mqtt_frame:connack(SP, 0), D4));
                {error, Reason} ->
                    {stop, {partition_subscribe_failed, Reason}, D3}
            end
    end.

%% §4.3's stub, plus the E2 capability derivation: a backend whose capability
%% lacks fsync caps the session at QoS0 whatever the config claims, so a
%% contradictory `mqtt_max_qos = 1' on a memory backend is not honoured silently
%% (§3.3 freeze 3's early-failure half).
authenticate(#connect{keep_alive = KA, username = User, password = Pass}, D) ->
    MaxQos = effective_max_qos(opt(mqtt_max_qos, 1, D)),
    Principal = case {User, Pass} of
                    {undefined, undefined} ->
                        %% An anonymous CONNECT maps the client_id to itself as
                        %% principal (§4.3 freezes only the credentialed path;
                        %% registered as a deviation in design §13).
                        D#st.client_id;
                    {U, P} ->
                        case broker_security:authenticate(U, P) of
                            {ok, Authenticated} -> Authenticated;
                            {error, _} -> denied
                        end
                end,
    case Principal of
        denied ->
            log(warning, "authentication failed for client_id=~p", [D#st.client_id]),
            %% §4.3's error must not distinguish "unknown user" from "wrong
            %% password"; 0x04 covers both.
            {error, 4, D};
        Pr ->
            allow = broker_security:authorize(Pr, {connect, D#st.client_id}),
            {ok, D#st{max_qos = MaxQos, keepalive_ms = clamp_keepalive(KA, D),
                      principal = Pr,
                      tenant_id = broker_security:tenant_id(Pr)}}
    end.

effective_max_qos(ConfigMax) ->
    Ps = broker_queue:partition_ids(),
    Durable = lists:all(fun(P) ->
                               case (catch broker_queue:capability(P)) of
                                   Caps when is_list(Caps) -> lists:member(fsync, Caps);
                                   _ -> false
                               end
                        end, Ps),
    case Ps =/= [] andalso Durable of
        true -> ConfigMax;
        false -> min(ConfigMax, 0)
    end.

%% §4.5's non-zero keepalive is clamped to the server cap rather than disabled;
%% 0 takes the cap too (design §13 L3: an idle-forever session is not a thing in
%% S1).
clamp_keepalive(0, D) -> opt(max_keepalive_ms, 60000, D);
clamp_keepalive(Sec, D) when is_integer(Sec), Sec > 0 ->
    min(Sec * 1000, opt(max_keepalive_ms, 60000, D)).

session_present(false, ClientId) ->
    case broker_subscription:session(ClientId) of
        {ok, #{subs := [_ | _]}} -> 1;
        _ -> 0
    end;
session_present(true, _ClientId) ->
    %% §4.2: a clean session is never a resumed one, even when leftover state
    %% from a previous non-clean session is about to be wiped.
    0.

attach_session(ClientId, Clean, D) ->
    %% attach/3 answers `{ok, PrevPid}' or raises: a worker that cannot answer is
    %% a dead gen_server call, not an `{error, _}' return, so there is no failure
    %% shape to handle here (dialyzer proved the old `error' clause unreachable).
    {ok, Prev} = broker_subscription:attach(ClientId, self(), not Clean),
    case Prev of
        Pid when is_pid(Pid), Pid =/= self() ->
            Pid ! {mqtt_takeover, self()},
            log(info, "takeover of ~p from ~p", [ClientId, Pid]);
        _ ->
            %% undefined (no previous holder) or self (a reconnect that raced
            %% its own attach): nothing to hand the socket over to.
            ok
    end,
    D#st{attached = true, persist = not Clean}.

clean_session_teardown(D) ->
    lists:foreach(
      fun(#{sub_ref := {_, Filter}}) ->
              _ = broker_subscription:unsubscribe(D#st.client_id, Filter)
      end, broker_subscription:subscriptions(D#st.client_id)),
    %% Clearing the blob here is what makes the boot residue (an empty `.subs'
    %% with no `.inflight') harmless, and is the §4.2 "start from zero".
    _ = (catch broker_subscription:inflight(D#st.client_id, undefined)),
    D#st{pending = #{}, pending_rev = #{}, sent = #{}, next_pid = 1,
         inflight_dirty = false}.

restore_blob(#st{client_id = ClientId} = D) ->
    case broker_subscription:session(ClientId) of
        {ok, #{inflight := #{next_pid := N, pending := PList}}}
          when is_integer(N), is_list(PList) ->
            Rev = maps:from_list([{Pos, Pid} || {Pid, {_Refs, Pos}} <- PList]),
            %% `sent' stays empty on purpose: the blob is the PREVIOUS
            %% connection's in-flight set, and everything in it is owed exactly
            %% one DUP=1 resend by this one (R7, §4.1).
            D#st{next_pid = N, pending = maps:from_list(PList),
                 pending_rev = Rev};
        _ ->
            D
    end.

%% The partition notifications this conn needs to wake on (design §5.2). The
%% subscription set belongs to the queue worker, which monitors us, so an
%% abnormal death is cleaned up there as well (unsubscribe/2 stays for the
%% races §5.2 note 3 describes).
subscribe_partitions(D) ->
    Ps = broker_queue:partition_ids(),
    Results = [{P, (catch broker_queue:subscribe(P, self()))} || P <- Ps],
    case Ps =/= [] andalso length([ok || {_, ok} <- Results]) =:= length(Ps) of
        true -> {ok, D#st{partitions = Ps}};
        false -> {error, {partial, Results}}
    end.

%%%_* PUBLISH inbound ---------------------------------------------------------

handle_publish(#publish{qos = QoS, topic = Topic, packet_id = WirePid,
                        payload = Payload}, D) ->
    try
        {ok, Env} = broker_router:env(),
        %% route/3 is the ONLY partition oracle (ADR-010): even with no
        %% subscriber indexed it answers `{ok, [{Partition, []}]}' -- the hash of
        %% the destination, not a guess. The Ctx is the ADR-009 publisher view;
        %% S1's hash router does not consult it, but the frozen contract says
        %% the caller supplies one, and a real one costs nothing.
        Ctx = #route_ctx{tenant_id = D#st.tenant_id,
                         publisher = D#st.principal,
                         reliability = case QoS of 0 -> os_buffer; 1 -> fsync end},
        {ok, Routed} = broker_router_hash:route(Topic, Ctx, Env),
        %% The list always names at least the destination's own partition: ADR-010
        %% makes the hash the answer, so "nothing is subscribed" is `{ok, [{P, []}]}',
        %% never `[]'. A router that ever returns the empty list is a contract break
        %% and the surrounding try reports it as ours, not as a silently invented
        %% partition 0.
        [{Partition, Refs} | _] = Routed,
        Msg = build_message(Topic, Payload, QoS, WirePid, Partition, D),
        store_and_ack(Msg, QoS, Partition, Refs, WirePid, D)
    catch
        Class:Reason:Stack ->
            log(warning, "publish path failed ~p:~p ~p", [Class, Reason, Stack]),
            {stop, {publish_failed, Reason}, D}
    end.

store_and_ack(Msg, 0, _Partition, Refs, _WirePid, D) ->
    %% §4.4: QoS0 does not enter the log. It reaches subscribers through the
    %% unpositioned push, and no cursor is involved (the pull loop's read-and-
    %% skip only ever sees stored traffic).
    _ = (catch broker_subscription:dispatch(Refs, {undefined, undefined, Msg})),
    {state, connected, D};
store_and_ack(Msg, 1, Partition, Refs, WirePid, D) ->
    case (catch broker_queue:append(Partition, Msg, fsync)) of
        {ok, Offset} ->
            %% (e) PUBACK the moment fsync returned (ADR-007/R1a). The packet
            %% id echoes the client's own (§2.4: an ack carries the id of the
            %% PUBLISH it answers), so it never enters this conn's outbound id
            %% space -- `pending' is keyed by take_pid numbers alone.
            D1 = send_frame(mqtt_frame:puback(D#st.client_id, WirePid), D),
            %% (f) dispatch after that, and never rolled back (§5.1): a failed
            %% delivery does not undo the producer's promise.
            _ = (catch broker_subscription:dispatch(Refs, {Partition, Offset, Msg})),
            {state, connected, D1};
        Other ->
            %% The ack IS the contract: no PUBACK on failure, and the publish
            %% retry semantics belong to the caller (J7).
            log(warning, "append failed ~p, dropping connection", [Other]),
            {stop, {append_failed, Other}, D}
    end.

build_message(Topic, Payload, QoS, WirePid, Partition, D) ->
    #broker_message{
      message_id = crypto:strong_rand_bytes(16),
      tenant_id = D#st.tenant_id,
      destination = Topic,
      partition_id = Partition,
      payload = Payload,
      payload_encoding = raw,
      headers = #{},
      proto_meta = #{qos => QoS, dup => 0, retain => 0, packet_id => WirePid},
      persistence = case QoS of 0 -> transient; 1 -> durable end,
      reliability = case QoS of 0 -> os_buffer; 1 -> fsync end,
      created_at = erlang:system_time(millisecond),
      expires_at = undefined,
      event = undefined}.

%%%_* SUBSCRIBE / UNSUBSCRIBE -------------------------------------------------

handle_subscribe(#subscribe{packet_id = Pid, topics = Topics}, D) ->
    Codes = [grant(Filter, ReqQoS, D) || {Filter, ReqQoS} <- Topics],
    {state, connected, send_frame(mqtt_frame:suback(Pid, Codes), D)}.

grant(Filter, ReqQoS, D) ->
    case {mqtt_filter:validate_filter(Filter), ReqQoS =< D#st.max_qos} of
        {ok, true} ->
            case broker_subscription:subscribe(D#st.client_id, Filter, ReqQoS) of
                {ok, _Ref} -> ReqQoS;
                {error, Reason} ->
                    log(warning, "subscribe refused ~p: ~p", [Filter, Reason]),
                    128
            end;
        {ok, false} ->
            %% E12: an over-cap requested QoS is refused at the SUBACK, never
            %% silently downgraded and never turned into a live subscription.
            log(info, "subscribe ~p qos ~p above max ~p", [Filter, ReqQoS, D#st.max_qos]),
            128;
        {{error, Reason}, _} ->
            %% The codec validates filters at parse time, so reaching this clause
            %% means a frame got past `mqtt_frame' with a bad filter in it -- the
            %% 0x80 is the SUBACK's answer either way, and the second element
            %% (over-cap or not) is irrelevant next to an illegal filter.
            log(warning, "invalid filter ~p: ~p", [Filter, Reason]),
            128
    end.

handle_unsubscribe(#unsubscribe{packet_id = Pid, topics = Filters}, D) ->
    lists:foreach(fun(Filter) ->
                          _ = broker_subscription:unsubscribe(D#st.client_id, Filter)
                  end, Filters),
    %% pending is deliberately NOT pruned: a dead subscription's ack is an
    %% idempotent no-op (worker §ack), and pruning here would race the pull loop
    %% that read the subscription set one moment earlier.
    {state, connected, send_frame(mqtt_frame:unsuback(Pid), D)}.

%%%_* PUBACK inbound ----------------------------------------------------------

handle_puback(Pid, #st{pending = Pending} = D) ->
    case maps:take(Pid, Pending) of
        {{Refs, {P, O} = Pos}, Pending1} ->
            D1 = D#st{pending = Pending1,
                      pending_rev = remove_rev(D#st.pending_rev, Pos, Pid),
                      sent = maps:remove(Pid, D#st.sent),
                      inflight_dirty = true},
            %% The merged subscription set of this delivery advances together:
            %% the worker's contiguous-prefix rule is what tolerates PUBACKs
            %% arriving in any order (§ack).
            lists:foreach(fun(Ref) -> ack_skip(Ref, P, O) end, Refs),
            schedule_round(maybe_snapshot(D1));
        error ->
            %% A late ack for an already-cleared packet, or a bogus one:
            %% at-least-once delivery does not reward a disconnect.
            log(info, "puback ~p matched nothing", [Pid]),
            {state, connected, D}
    end.

remove_rev(Rev, Pos, Pid) ->
    case maps:get(Pos, Rev, undefined) of
        Pid -> maps:remove(Pos, Rev);
        _ -> Rev
    end.

%%%_* Pull round (the in-conn dispatcher) -------------------------------------

wake(connected, D) -> schedule_round(D);
wake(StateName, D) -> {state, StateName, D}.

schedule_round(#st{round_ref = undefined} = D) ->
    Ref = make_ref(),
    self() ! {round_t, Ref},
    {state, connected, D#st{round_ref = Ref}};
schedule_round(D) ->
    %% a round is already scheduled; it will see the state as it is then
    {state, connected, D}.

%% The poll tick is the lost-wakeup backstop (J6: wakes may vanish): every
%% processed event re-arms it, and a tick that raced a re-arm is dropped by the
%% ref guard.
arm_poll(connected, #st{poll_ref = undefined} = D) ->
    Ref = make_ref(),
    _ = erlang:send_after(opt(pull_poll_interval_ms, 1000, D), self(), {poll_t, Ref}),
    D#st{poll_ref = Ref};
arm_poll(_StateName, D) ->
    D.

pull_round(D0) ->
    try
        D = D0#st{round_no = D0#st.round_no + 1},
        Subs = broker_subscription:subscriptions(D#st.client_id),
        %% One partition per round, rotated by the round number: a deep backlog
        %% on P0 cannot starve P3 (plan §pull loop step 4).
        case pull_sweep(rotate(D#st.partitions, D#st.round_no), Subs, D) of
            %% The round's OWN state is what continues: a sweep that sent three
            %% deliveries and threaded {progress, D3} back into `schedule_round(D)'
            %% would forget the packet ids it just allocated and never see a
            %% PUBACK for them.
            {progress, D1} -> schedule_round(D1);
            {_verdict, D1} -> {state, connected, D1}
        end
    catch
        Class:Reason:Stack ->
            %% The worker-restart window: those protected tables briefly vanish
            %% (plan risk 3). The poll tick is the recovery path.
            log(warning, "pull round failed ~p:~p ~p", [Class, Reason, Stack]),
            {state, connected, D0}
    end.

%% The sweep carries the state through the partitions it visits: a delivery made
%% in P0 is still a delivery when P1 is looked at (and when the round stops early
%% on a verdict that is not `idle').
pull_sweep([P | Rest], Subs, D) ->
    {Verdict, D1} = pull_partition(P, subs_for(P, Subs), D),
    case Verdict of
        idle -> pull_sweep(Rest, Subs, D1);
        Other -> {Other, D1}
    end;
pull_sweep([], _Subs, D) ->
    {idle, D}.

rotate(Ps, N) ->
    Start = N rem max(1, length(Ps)),
    lists:nthtail(Start, Ps) ++ lists:sublist(Ps, Start).

%% The subscription rows this conn serves, with the one field the pull loop
%% needs per partition: that sub's cursor. `subscriptions/1' is re-read every
%% round (it is a protected ETS read), which is also how a UNSUBSCRIBE that raced
%% us gets digested.
subs_for(P, Subs) ->
    lists:filtermap(
      fun(#{sub_ref := Ref, filter := Filter, qos := QoS, cursors := Cursors}) ->
              case lists:keyfind(P, 1, Cursors) of
                  {P, Offset} -> {true, {Ref, Filter, QoS, Offset}};
                  false -> false
              end
      end, Subs).

pull_partition(_P, [], D) ->
    idle_verdict(D);
pull_partition(P, Rows, D) ->
    %% The cursor is the only read position (J6), and the union of this client's
    %% cursors in P is exactly `min' of them: everything above it has been asked
    %% for by somebody. Per-message relevance is decided by each row's own
    %% cursor, not by this start.
    Start = lists:min([Offset || {_Ref, _Filter, _QoS, Offset} <- Rows]),
    case read_page(P, Start, D) of
        {ok, [], _Next} ->
            idle_verdict(D);
        {ok, Page, _Next} ->
            {D1, Rank} = lists:foldl(
              fun({Offset, Msg}, {DAcc, RAcc}) ->
                      {DAcc2, V} = handle_stored(P, Offset, Msg, Rows, DAcc),
                      {DAcc2, max(RAcc, rank(V))}
              end, {D, 0}, Page),
            {unrank(Rank), D1};
        {error, Reason} ->
            log(warning, "read_from(~p, ~p) failed ~p", [P, Start, Reason]),
            idle_verdict(D)
    end.

idle_verdict(D) -> {idle, D}.

%% progress > stalled > idle: a page that moved any cursor earns another round,
%% a page whose only news is a full in-flight window waits for a PUBACK or the
%% poll tick, and a page of pure resends earns nothing -- that ranking is what
%% keeps a DUP storm from burning rounds.
rank(idle) -> 0;
rank(stalled) -> 1;
rank(progress) -> 2.

unrank(0) -> idle;
unrank(1) -> stalled;
unrank(2) -> progress.

read_page(P, From, D) ->
    Limit = opt(pull_page_limit, 32, D),
    case (catch broker_queue:read_from(P, From, Limit)) of
        {ok, Page, Next} when is_list(Page) -> {ok, Page, Next};
        {error, {stale_handle, _, _, _, _}} ->
            %% broker_queue repairs its registry synchronously before
            %% returning, so one immediate re-read is legitimate; if that also
            %% fails the round gives up and the poll tick retries.
            (catch broker_queue:read_from(P, From, Limit));
        Other ->
            {error, Other}
    end.

%%%_* One stored message, every subscribing filter (pull path) ----------------

handle_stored(P, O, Msg, Rows, D) ->
    Topic = Msg#broker_message.destination,
    {Hits, Others} =
        lists:foldl(
          fun({Ref, Filter, QoS, Cursor}, {H, Ot}) when Cursor =< O ->
                  case mqtt_filter:matches(Topic, Filter) of
                      true -> {[{Ref, QoS} | H], Ot};
                      false -> {H, [Ref | Ot]}
                  end;
             (_Row, Acc) ->
                  Acc
          end, {[], []}, Rows),
    case Hits of
        [] when Others =:= [] ->
            {D, idle};
        [] ->
            %% read-and-skip: non-matching traffic still has to advance every
            %% cursor in P, or the committed watermark pins at the first
            %% unrelated message forever (K6).
            lists:foreach(fun(Ref) -> ack_skip(Ref, P, O) end, Others),
            {D, progress};
        _ ->
            Refs = [Ref || {Ref, _Q} <- Hits],
            %% §4.8: the delivery level is max QoS of the subscriptions that
            %% HIT this message (merged once per client by dispatch/2's H7 rule,
            %% re-derived here from the rows), capped by the stored message's
            %% own level -- which is 1, because only QoS1 publishes are stored.
            %% So a purely QoS0 hit set delivers with no ack owed at all.
            case lists:max([Q || {_Ref, Q} <- Hits]) of
                0 ->
                    lists:foreach(fun(Ref) -> ack_skip(Ref, P, O) end,
                                  Others ++ Refs),
                    {send_frame(publish_bin(0, 0, Msg, undefined), D), progress};
                1 ->
                    deliver_qos1(P, O, Msg, Refs, Others, D)
            end
    end.

deliver_qos1(P, O, Msg, Refs, Others, D) ->
    Pos = {P, O},
    lists:foreach(fun(Ref) -> ack_skip(Ref, P, O) end, Others),
    case maps:get(Pos, D#st.pending_rev, undefined) of
        Pid when is_integer(Pid) ->
            %% Already in flight for this {Partition, Offset}: either restored
            %% from the blob after a reconnect, or held here because its PUBACK
            %% has not arrived (that sub's cursor is still at O, which is what
            %% made this page start here).
            case maps:is_key(Pid, D#st.sent) of
                true ->
                    %% This connection already put it on the wire. The cursor has
                    %% not moved, so every round lands here again -- and resending
                    %% on each of them would be a retry timer S1 does not have:
                    %% §4.1's redelivery is the RECONNECT path, which starts with
                    %% an empty `sent' set. Silent, and not progress.
                    {D, idle};
                false ->
                    %% R7: the SAME packet_id with DUP=1. A fresh id would defeat
                    %% the client's duplicate check, which is the one thing an
                    %% at-least-once redelivery has to get right.
                    {send_frame(publish_bin(1, 1, Msg, Pid), mark_sent(Pid, D)), idle}
            end;
        undefined ->
            case maps:size(D#st.pending) >= opt(max_inflight, 64, D) of
                true ->
                    %% The window is full: the cursor stays at O, so the next
                    %% round lands here again. Not backpressure -- the peer just
                    %% sees a stall (L10; S2 adds slow-consumer isolation).
                    {D, stalled};
                false ->
                    {Pid, D1} = take_pid(D),
                    D2 = mark_sent(Pid, D1#st{
                             pending = maps:put(Pid, {Refs, Pos}, D1#st.pending),
                             pending_rev = maps:put(Pos, Pid, D1#st.pending_rev),
                             inflight_dirty = true}),
                    {send_frame(publish_bin(1, 0, Msg, Pid), D2), progress}
            end
    end.

mark_sent(Pid, #st{sent = Sent} = D) ->
    D#st{sent = Sent#{Pid => true}}.

take_pid(#st{next_pid = N} = D) ->
    {N, D#st{next_pid = next_pid_after(N)}}.

next_pid_after(65535) -> 1;
next_pid_after(N) -> N + 1.

publish_bin(QoS, Dup, Msg, Pid) ->
    mqtt_frame:encode(#publish{qos = QoS, dup = Dup, retain = 0,
                               topic = Msg#broker_message.destination,
                               packet_id = Pid,
                               payload = Msg#broker_message.payload}).

%% ack/3 refuses unknown/dead subscriptions idempotently, so this is safe after
%% an UNSUBSCRIBE raced us (the cursor row is gone with the entity).
ack_skip(Ref, P, O) ->
    _ = (catch broker_subscription:ack(Ref, P, O)),
    ok.

%%%_* In-flight snapshot (R7 blob) ---------------------------------------------

maybe_snapshot(#st{inflight_dirty = true, snapshot_ref = undefined} = D) ->
    Ref = make_ref(),
    _ = erlang:send_after(opt(inflight_snapshot_ms, 20, D), self(), {snapshot_t, Ref}),
    D#st{snapshot_ref = Ref};
maybe_snapshot(D) ->
    D.

snapshotted(#st{client_id = ClientId} = D) ->
    _ = (catch broker_subscription:inflight(ClientId, blob(D))),
    D#st{inflight_dirty = false}.

%% The conn's memory is the authority; this layer stores it without inspecting
%% it (R7). Recovery may be stale, which means duplicates -- and MQTT 3.1.1 §4.1
%% already allows that.
blob(D) ->
    #{next_pid => D#st.next_pid, pending => maps:to_list(D#st.pending)}.

%%%_* Outbound frames ----------------------------------------------------------

send_frame(Bin, #st{socket = Sock} = D) ->
    case (catch gen_tcp:send(Sock, Bin)) of
        ok ->
            D;
        Other ->
            %% A dead socket shows up as {tcp_closed, _} shortly; a failed send
            %% is not a protocol error, and S1 has no backpressure to fail.
            log(warning, "send failed ~p", [Other]),
            D
    end.

refuse(Code, D) ->
    {stop, normal, send_frame(mqtt_frame:connack(0, Code), D)}.

%% The keepalive deadline measures the PEER's silence (§3.1.2.10: the server's
%% timeout is 1.5x the negotiated keepalive), so only bytes from the socket
%% restart it. gen_statem's own `state_timeout' cannot express that: it is
%% canceled by ANY received event, which would let this process's own timers --
%% the poll sentinel, a scheduled round, a snapshot tick -- keep a dead peer
%% connected forever. An explicit send_after with a {Ref, TRef} pair is the
%% shape that restarts on traffic and on nothing else.
ka_arm(connected, #st{keepalive_ms = KA} = D) when KA > 0 ->
    _ = case D#st.ka_ref of
            undefined -> ok;
            {_OldRef, TRef} -> erlang:cancel_timer(TRef)
        end,
    Ref = make_ref(),
    D#st{ka_ref = {Ref, erlang:send_after(KA * 3 div 2, self(), {ka_t, Ref})}};
ka_arm(_StateName, D) ->
    D.

%%%_* Terminate ----------------------------------------------------------------

%% Reason `takeover' is the one that must not write the blob: the new connection
%% is attaching and its view of the in-flight set wins. A session that asked for
%% clean_session has nothing to keep (detach/2 tears it down, K10 guard
%% included), so its blob write is skipped as well.
terminate(Reason, _StateName, #st{partitions = Ps} = D) ->
    Persisting = D#st.persist andalso Reason =/= takeover,
    if
        Persisting andalso D#st.inflight_dirty ->
            _ = (catch broker_subscription:inflight(D#st.client_id, blob(D))),
            ok;
        true ->
            ok
    end,
    lists:foreach(fun(P) -> _ = (catch broker_queue:unsubscribe(P, self())) end, Ps),
    if
        D#st.attached ->
            _ = (catch broker_subscription:detach(D#st.client_id, self())),
            ok;
        true ->
            ok
    end,
    _ = D#st.socket =/= undefined andalso (catch gen_tcp:close(D#st.socket)),
    ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%%%_* Helpers ------------------------------------------------------------------

%% A listener's per-listener map (T12's `ProtoOpts', the L20 seam) wins over
%% the app env; a connection started without one (the T9 test helper) gets the
%% app env, so both paths read the same knobs through one function.
opt(Key, Default, #st{opts = Opts}) when is_map(Opts) ->
    maps:get(Key, Opts, application:get_env(broker_proto_mqtt, Key, Default));
opt(Key, Default, _D) ->
    application:get_env(broker_proto_mqtt, Key, Default).

%% The levels are `logger' names, not `error_logger' aliases: logger:log(warn,
%% ...) is a function_clause in `logger_config:less_or_equal_level/2', and since
%% this is the only module in the broker that logs, that crash would look like a
%% protocol bug.
log(Level, Fmt, Args) ->
    logger:log(Level, "mqtt_conn: " ++ Fmt, Args).