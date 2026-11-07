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
             max_qos = 2,                  %% effective, capability-derived (E2)
             principal = undefined,
             tenant_id = <<"default">>,
             client_id = undefined,
             keepalive_ms = 0,
             attached = false,
             persist = false,              %% = not clean_session (§4.2)
             round_no = 0,
             next_pid = 1,
             pending = #{},                %% Pid => {[SubRef], {Partition, Offset}}
                                           %%      | {retained, Topic}
             pending_qos = #{},            %% Pid => 1 | 2 (the delivery level)
             released = #{},               %% Pid => true, QoS2 in PUBREL stage
             received = #{},               %% Pid => true, inbound QoS2 awaiting PUBREL
             retained_queue = [],          %% [{Topic, Msg, QoS}] owed after SUBACK
             pending_rev = #{},            %% {Partition, Offset} => Pid
             sent = #{},                   %% Pid => true, SENT BY THIS CONNECTION
             sent_at = #{},                %% Pid => monotonic us, for ack_latency_us
             will = undefined,             %% #{topic, payload, qos, retain} | undefined
             counted = false,              %% this conn's row in connections_current
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
    broker_metrics:inc(bytes_in, byte_size(Data)),
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
    note_delivery(Msg),
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
    %% §3.3.4 + E12: close, no ack. On a session whose effective max is 0 or 1
    %% this is a PUBLISH above the granted level; §4.5's posture is to close
    %% without acking rather than silently downgrade.
    broker_metrics:inc(protocol_errors),
    log(warning, "mqtt_qos_above_max: qos=~p max=~p topic=~p",
        [QoS, Max, Topic]),
    {stop, normal, D};
event(#publish{} = P, connected, D) ->
    handle_publish(P, D);
event(#subscribe{} = S, connected, D) ->
    handle_subscribe(S, D);
event(#unsubscribe{} = U, connected, D) ->
    handle_unsubscribe(U, D);
event(#puback{packet_id = Pid}, connected, D) ->
    handle_puback(Pid, D);
event(#pubrec{packet_id = Pid}, connected, D) ->
    handle_pubrec(Pid, D);
event(#pubrel{packet_id = Pid}, connected, D) ->
    handle_pubrel(Pid, D);
event(#pubcomp{packet_id = Pid}, connected, D) ->
    handle_pubcomp(Pid, D);
event(#ping{kind = req}, connected, D) ->
    {state, connected, send_frame(mqtt_frame:pingresp(), D)};
event(#disconnect{}, connected, D) ->
    %% §3.14: a normal disconnect DISCARDS the Will (ADR-018 §18.1). Clearing
    %% it here is what tells terminate/3 "clean goodbye": the stop reason stays
    %% `normal', because a client saying goodbye is not a supervisor event.
    log(info, "client disconnect (client_id=~p)", [D#st.client_id]),
    {stop, normal, D#st{will = undefined}};
event(Frame, awaiting_connect, D) when is_tuple(Frame) ->
    %% §3.1: a frame before CONNECT is a protocol violation, not a retryable
    %% condition -- the stream position is unknown to us.
    broker_metrics:inc(protocol_errors),
    log(warning, "first frame is not CONNECT: ~p", [element(1, Frame)]),
    {stop, normal, D};
event(#connect{}, connected, D) ->
    broker_metrics:inc(protocol_errors),
    log(warning, "CONNECT on an established session", []),
    {stop, normal, D};
event(Other, StateName, D) ->
    broker_metrics:inc(protocol_errors),
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
            broker_metrics:inc(frame_decode_errors),
            decode_error(Reason, StateName, D);
        {'EXIT', Reason} ->
            %% decode/1 promises never to crash; if that ever breaks, the
            %% connection dies and the codec's suite says why.
            broker_metrics:inc(frame_decode_errors),
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
    broker_metrics:inc(protocol_errors),
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
            %% ADR-018 §18.1: the Will is connection state, held in memory only
            %% (never persisted, never republished after a broker restart).
            D1a = D1#st{will = will_of(C)},
            Clean = CSbit =:= 1,
            %% session_present describes the session the client is about to get
            %% back, so it has to be read before the clean-session teardown.
            SP = session_present(Clean, C#connect.client_id),
            D2 = attach_session(C#connect.client_id, Clean, D1a),
            D3 = case Clean of
                     true -> clean_session_teardown(D2);
                     false -> restore_blob(D2)
                 end,
            case subscribe_partitions(D3) of
                {ok, D4} ->
                    D5 = count_connection(D4),
                    log(info, "session up: client_id=~p principal=~p "
                              "clean=~p session_present=~p max_qos=~p "
                              "keepalive_ms=~p",
                        [C#connect.client_id, D5#st.principal, Clean, SP,
                         D5#st.max_qos, D5#st.keepalive_ms]),
                    %% E7/R7: the connect-time round is what serves a
                    %% reconnecting session its backlog and its DUP
                    %% resends -- leaving it to the poll tick would make
                    %% a catch-up arrive up to pull_poll_interval_ms late
                    %% even though we know, right here, that the session
                    %% may have unread state.
                    %%
                    %% ADR-012 §12.3 / ADR-018 §18.3 R3: a restored QoS 2
                    %% entry that had already been PUBREC'd resumes with
                    %% PUBREL and must NOT be re-PUBLISHed. Doing it before
                    %% the round also keeps the two from racing for the same
                    %% packet id.
                    schedule_round(resend_released(
                                     send_frame(mqtt_frame:connack(SP, 0), D5)));
                {error, Reason} ->
                    {stop, {partition_subscribe_failed, Reason}, D3}
            end
    end.

%% ADR-018 §18.1: the CONNECT will fields, or `undefined' when there is no Will.
will_of(#connect{will_flag = 0}) ->
    undefined;
will_of(#connect{will_topic = Topic, will_payload = Payload,
                 will_qos = QoS, will_retain = Retain}) ->
    #{topic => Topic, payload => Payload, qos => QoS, retain => Retain}.

%% ADR-012 §12.3: "already PUBREC'd, not yet PUBCOMP'd" resumes with PUBREL
%% alone. The restored entries were put in `released' by restore_blob/1.
resend_released(#st{released = Released} = D) ->
    lists:foldl(fun(Pid, DAcc) ->
                        send_frame(mqtt_frame:encode(#pubrel{packet_id = Pid}), DAcc)
                end, D, lists:sort(maps:keys(Released))).

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
            broker_metrics:inc(auth_failures),
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
    D#st{pending = #{}, pending_qos = #{}, released = #{}, received = #{},
         retained_queue = [], pending_rev = #{}, sent = #{}, sent_at = #{},
         next_pid = 1, inflight_dirty = false}.

%% The blob is `vsn 2' (ADR-018 §18.3 R3) and a `vsn 1' blob from S1 is read
%% as "pending, all QoS 1, no release state" -- semantically identical to S1's
%% world, so the upgrade is safe in one direction only (D9).
restore_blob(#st{client_id = ClientId} = D) ->
    case broker_subscription:session(ClientId) of
        {ok, #{inflight := #{pending := PList} = Blob}}
          when is_list(PList) ->
            Pending = maps:from_list(PList),
            case maps:get(vsn, Blob, 1) of
                1 ->
                    restore_with(Pending, maps:map(fun(_, _) -> 1 end, Pending),
                                 #{}, #{}, Blob, D);
                _ ->
                    restore_with(Pending,
                                 maps:get(qos, Blob, #{Pid => 1 || {Pid, _} <- PList}),
                                 maps:get(released, Blob, #{}),
                                 maps:get(received, Blob, #{}),
                                 Blob, D)
            end;
        _ ->
            D
    end.

restore_with(Pending, Qos, Released, Received, Blob, D) ->
    %% `sent' stays empty on purpose: the blob is the PREVIOUS connection's
    %% in-flight set, and everything in it is owed exactly one DUP=1 resend by
    %% this one (R7, §4.1). Entries in `released' get PUBREL instead.
    %%
    %% The pattern already destructures `{Refs, {P, O}}', so every restored entry
    %% is positional by construction -- blob/1 filtered the retained replays out
    %% before writing. No second predicate is needed (and one would be dead code).
    Rev = maps:from_list([{Pos, Pid} || {Pid, {_Refs, Pos}} <- maps:to_list(Pending)]),
    D#st{next_pid = maps:get(next_pid, Blob, 1),
         pending = Pending,
         pending_qos = Qos,
         released = maps:filter(fun(Pid, _) -> maps:is_key(Pid, Pending) end,
                                Released),
         received = Received,
         pending_rev = Rev}.

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
                        payload = Payload, retain = Retain}, D) ->
    broker_metrics:inc(publish_total),
    %% ADR-018 §18.3 R5: a repeated QoS 2 PUBLISH (same packet id, same
    %% connection) is answered with PUBREC and never dispatched twice. This
    %% check runs before any storage work so the dedupe costs nothing.
    case QoS =:= 2 andalso maps:is_key(WirePid, D#st.received) of
        true ->
            log(info, "qos2 duplicate publish pid=~p, re-PUBREC", [WirePid]),
            {state, connected, send_frame(
                                mqtt_frame:encode(#pubrec{packet_id = WirePid}), D)};
        false ->
            handle_publish1(Topic, Payload, QoS, WirePid, Retain, D)
    end.

handle_publish1(Topic, Payload, QoS, WirePid, Retain, D) ->
    try
        {ok, Env} = broker_router:env(),
        %% route/3 is the ONLY partition oracle (ADR-010): even with no
        %% subscriber indexed it answers `{ok, [{Partition, []}]}' -- the hash of
        %% the destination, not a guess. The Ctx is the ADR-009 publisher view;
        %% S1's hash router does not consult it, but the frozen contract says
        %% the caller supplies one, and a real one costs nothing.
        Ctx = #route_ctx{tenant_id = D#st.tenant_id,
                         publisher = D#st.principal,
                         reliability = reliability_of(QoS)},
        {ok, Routed} = broker_router_hash:route(Topic, Ctx, Env),
        %% The list always names at least the destination's own partition: ADR-010
        %% makes the hash the answer, so "nothing is subscribed" is `{ok, [{P, []}]}',
        %% never `[]'. A router that ever returns the empty list is a contract break
        %% and the surrounding try reports it as ours, not as a silently invented
        %% partition 0.
        [{Partition, Refs} | _] = Routed,
        Msg = build_message(Topic, Payload, QoS, WirePid, Partition, Retain, D),
        store_and_ack(Msg, QoS, Partition, Refs, WirePid, Retain, D)
    catch
        Class:Reason:Stack ->
            log(warning, "publish path failed ~p:~p ~p", [Class, Reason, Stack]),
            {stop, {publish_failed, Reason}, D}
    end.

store_and_ack(Msg, 0, _Partition, Refs, _WirePid, Retain, D) ->
    %% §4.4: QoS0 does not enter the log. It reaches subscribers through the
    %% unpositioned push, and no cursor is involved (the pull loop's read-and-
    %% skip only ever sees stored traffic). ADR-018 §18.2: the retain tree is
    %% updated first, in async mode -- QoS0 has no durability promise, so the
    %% ack window is irrelevant here.
    D1 = retain_update(Msg, Retain, async, D),
    _ = (catch broker_subscription:dispatch(Refs, {undefined, undefined, Msg})),
    {state, connected, D1};
store_and_ack(Msg, QoS, Partition, Refs, WirePid, Retain, D)
  when QoS =:= 1; QoS =:= 2 ->
    T0 = erlang:monotonic_time(microsecond),
    case (catch broker_queue:append(Partition, Msg, fsync)) of
        {ok, Offset} ->
            broker_metrics:observe(
              publish_latency_us,
              erlang:monotonic_time(microsecond) - T0),
            %% ADR-018 §18.2 order: append(fsync) -> retain store(fsync) -> ACK
            %% -> dispatch. A failed retain store withholds the ACK (the
            %% publish's durability promise covers the retained state too) but
            %% the WAL record stays: a duplicate after reconnect is allowed,
            %% a lost retained value is not.
            case retain_store(Msg, Retain, sync) of
                ok ->
                    ack_and_dispatch(Msg, QoS, Partition, Offset, Refs,
                                     WirePid, D);
                {error, Reason} ->
                    log(warning, "retain store failed ~p, dropping connection",
                        [Reason]),
                    {stop, {retain_store_failed, Reason}, D}
            end;
        Other ->
            %% The ack IS the contract: no PUBACK on failure, and the publish
            %% retry semantics belong to the caller (J7).
            log(warning, "append failed ~p, dropping connection", [Other]),
            {stop, {append_failed, Other}, D}
    end.

%% (e) The ack goes out the moment fsync returned (ADR-007/R1a). The packet id
%% echoes the client's own (§2.4: an ack carries the id of the PUBLISH it
%% answers), so it never enters this conn's outbound id space -- `pending' is
%% keyed by take_pid numbers alone.
ack_and_dispatch(Msg, 1, Partition, Offset, Refs, WirePid, D) ->
    D1 = send_frame(mqtt_frame:puback(D#st.client_id, WirePid), D),
    _ = (catch broker_subscription:dispatch(Refs, {Partition, Offset, Msg})),
    {state, connected, D1};
ack_and_dispatch(Msg, 2, Partition, Offset, Refs, WirePid, D0) ->
    %% ADR-018 §18.3 R5 (Method A): the message is delivered on PUBLISH, and
    %% `received' only remembers the id so a repeat is not delivered twice.
    %% R4: the cap is checked BEFORE the state grows; over the cap is a
    %% deliberate disconnect, never a silent drop of the record.
    case remember_received(WirePid, D0) of
        {ok, D1} ->
            D2 = send_frame(mqtt_frame:encode(#pubrec{packet_id = WirePid}), D1),
            _ = (catch broker_subscription:dispatch(Refs, {Partition, Offset, Msg})),
            {state, connected, D2};
        {overflow, D1} ->
            {stop, qos2_state_overflow, D1}
    end.

%% The retain tree holds the message itself. `retain_store/3' is the checked,
%% fsync'd path (QoS > 0) and `retain_update/4' the fire-and-forget one (QoS 0).
%%
%% Three shapes, per §3.3.1.3 / ADR-018 §18.2:
%%   * retain = 0     -> the tree is not touched at all (a normal publish).
%%   * retain = 1, payload = <<>> -> DELETE the topic's entry; the empty message
%%     still reaches live subscribers through dispatch.
%%   * retain = 1, payload non-empty -> overwrite the topic's entry.
retain_store(_Msg, 0, _Mode) ->
    ok;
retain_store(#broker_message{payload = <<>>} = Msg, 1, Mode) ->
    case broker_retain:delete(Msg#broker_message.destination, Mode) of
        ok -> ok;
        {error, _} = Err -> Err
    end;
retain_store(Msg, 1, Mode) ->
    case broker_retain:put(Msg, Mode) of
        ok -> ok;
        %% Bounded refusal (ADR-018 §18.2): the message still goes out to live
        %% subscribers and is still acked; only the retained state is skipped.
        {error, overflow} -> ok;
        {error, _} = Err -> Err
    end.

retain_update(Msg, Retain, Mode, D) ->
    _ = retain_store(Msg, Retain, Mode),
    D.

remember_received(Pid, #st{received = R} = D) ->
    Cap = opt(max_qos2_state_per_session, 100, D),
    case maps:size(R) + maps:size(D#st.released) >= Cap of
        true ->
            broker_metrics:inc(qos2_state_overflow),
            log(warning, "mqtt_qos2_state_overflow (received=~p released=~p cap=~p)",
                [maps:size(R), maps:size(D#st.released), Cap]),
            {overflow, D};
        false ->
            {ok, D#st{received = R#{Pid => true}, inflight_dirty = true}}
    end.

reliability_of(0) -> os_buffer;
reliability_of(_) -> fsync.

build_message(Topic, Payload, QoS, WirePid, Partition, Retain, D) ->
    #broker_message{
      message_id = crypto:strong_rand_bytes(16),
      tenant_id = D#st.tenant_id,
      destination = Topic,
      partition_id = Partition,
      payload = Payload,
      payload_encoding = raw,
      headers = #{},
      proto_meta = #{qos => QoS, dup => 0, retain => Retain, packet_id => WirePid},
      persistence = case QoS of 0 -> transient; _ -> durable end,
      reliability = reliability_of(QoS),
      created_at = erlang:system_time(millisecond),
      expires_at = undefined,
      event = undefined}.

%%%_* SUBSCRIBE / UNSUBSCRIBE -------------------------------------------------

handle_subscribe(#subscribe{packet_id = Pid, topics = Topics}, D) ->
    Pairs = [{Filter, grant(Filter, ReqQoS, D)} || {Filter, ReqQoS} <- Topics],
    Codes = [Code || {_Filter, Code} <- Pairs],
    Granted = [{Filter, Code} || {Filter, Code} <- Pairs, Code =/= 128],
    D1 = send_frame(mqtt_frame:suback(Pid, Codes), D),
    %% ADR-018 §18.2: the replay comes AFTER the SUBACK (the client must know
    %% its filter was granted before retained traffic arrives) and is queued
    %% rather than sent inline, because it shares the `max_inflight' window
    %% with ordinary delivery.
    drain_retained(enqueue_retained(Granted, D1)).

%% One queue entry per topic even when several granted filters match it (MQTT
%% §3.3.5 lets a server send duplicates; one message at the strongest granted
%% level is the useful answer). `min(retained qos, granted qos)' is §4.8's rule
%% applied to the replay path.
enqueue_retained([], D) ->
    D;
enqueue_retained(Granted, D) ->
    Entries = (catch broker_retain:entries()),
    case is_list(Entries) of
        false ->
            %% The retain app is not up (a bare conn in a unit test): nothing
            %% to replay, and a missing SUBACK-adjacent message is better than
            %% a crashed connection.
            log(warning, "retained replay skipped: ~p", [Entries]),
            D;
        true ->
            Wanted =
                lists:foldl(
                  fun({Filter, GrantedQoS}, Acc0) ->
                          lists:foldl(
                            fun({Topic, Msg}, Acc) ->
                                    case mqtt_filter:matches(Topic, Filter) of
                                        true ->
                                            Q = min(GrantedQoS, qos_of(Msg)),
                                            case maps:find(Topic, Acc) of
                                                {ok, {OldMsg, OldQ}} ->
                                                    Acc#{Topic => {OldMsg, max(OldQ, Q)}};
                                                error ->
                                                    Acc#{Topic => {Msg, Q}}
                                            end;
                                        false ->
                                            Acc
                                    end
                            end, Acc0, Entries)
                  end, #{}, Granted),
            Queue = [{Topic, Msg, Q}
                     || {Topic, {Msg, Q}} <- lists:sort(maps:to_list(Wanted))],
            D#st{retained_queue = D#st.retained_queue ++ Queue}
    end.

qos_of(#broker_message{proto_meta = Meta}) ->
    maps:get(qos, Meta, 0).

%% Send as much of the retained backlog as the in-flight window allows. QoS 0
%% entries never occupy the window, so they always go out in this pass; a full
%% window leaves the rest queued and the next PUBACK/PUBCOMP resumes it.
drain_retained(#st{retained_queue = []} = D) ->
    {state, connected, D};
drain_retained(#st{retained_queue = [{_Topic, Msg, 0} | Rest]} = D) ->
    note_delivery(Msg),
    D1 = send_frame(publish_bin(0, 0, 1, Msg, undefined), D),
    drain_retained(D1#st{retained_queue = Rest});
drain_retained(#st{retained_queue = [{Topic, Msg, QoS} | Rest]} = D) ->
    case maps:size(D#st.pending) >= opt(max_inflight, 64, D) of
        true ->
            %% The peer's window is full. Keep the entry (and the rest) queued.
            {state, connected, D};
        false ->
            {Pid, D1} = take_pid(D),
            %% A retained replay has no {Partition, Offset}, so it never
            %% enters `pending_rev' and can never advance a cursor. It is
            %% marked so a PUBACK/PUBCOMP releases the window only.
            Pos = {retained, Topic},
            D2 = mark_sent_at(Pid, mark_sent(Pid, D1#st{
                     pending = maps:put(Pid, Pos, D1#st.pending),
                     pending_qos = maps:put(Pid, QoS, D1#st.pending_qos),
                     retained_queue = Rest,
                     inflight_dirty = true})),
            note_delivery(Msg),
            drain_retained(send_frame(publish_bin(QoS, 0, 1, Msg, Pid), D2))
    end.

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

%%%_* PUBACK / PUBREC / PUBREL / PUBCOMP inbound ------------------------------
%%
%% Four frames, three regimes (ADR-012 + ADR-018 §18.3):
%%
%%   * PUBACK   -- closes an outbound QoS 1 delivery; only then may the cursor
%%                 move. A PUBACK for a QoS 2 entry is a peer bug: warn, ignore.
%%   * PUBREC   -- outbound QoS 2 reaches its second stage: reply PUBREL, keep
%%                 the entry (the cursor still waits for PUBCOMP).
%%   * PUBREL   -- inbound QoS 2 reaches its last stage: reply PUBCOMP.
%%   * PUBCOMP  -- outbound QoS 2 done: NOW the cursor moves.
%%
%% Unknown packet ids follow R2: answer idempotently, never disconnect, never
%% advance a cursor. A crash that lost the handshake state must not turn a
%% recoverable duplicate into a permanent reconnect loop.

handle_puback(Pid, #st{pending = Pending} = D) ->
    broker_metrics:inc(ack_total),
    case maps:find(Pid, Pending) of
        error ->
            %% A late ack for an already-cleared packet, or a bogus one:
            %% at-least-once delivery does not reward a disconnect.
            log(info, "puback ~p matched nothing", [Pid]),
            {state, connected, D};
        {ok, {retained, _Topic} = Pos} ->
            %% A retained replay: release the window, never a cursor.
            _ = Pos,
            finish_ack(Pid, D);
        {ok, {Refs, {P, O} = Pos}} ->
            case maps:get(Pid, D#st.pending_qos, 1) of
                2 ->
                    %% The peer answered a QoS 2 PUBLISH with PUBACK. Its state
                    %% machine is wrong; moving the cursor here would drop the
                    %% message it has not completed.
                    log(warning, "puback for a QoS 2 delivery pid=~p ignored", [Pid]),
                    {state, connected, D};
                1 ->
                    D1 = release_pid(Pid, Pos, D),
                    lists:foreach(fun(Ref) -> ack_skip(Ref, P, O) end, Refs),
                    after_release(D1)
            end
    end.

%% Outbound QoS 2, stage 1 -> 2. The entry stays in `pending' so PUBCOMP can
%% still find the cursor it owes.
handle_pubrec(Pid, #st{pending = Pending} = D) ->
    case maps:find(Pid, Pending) of
        error ->
            %% R2: the peer believes we sent a QoS 2 PUBLISH it has already
            %% PUBREC'd. Answering PUBREL lets it finish instead of retrying
            %% into a closed connection forever.
            log(info, "pubrec ~p matched nothing, replying pubrel", [Pid]),
            {state, connected, send_frame(mqtt_frame:encode(#pubrel{packet_id = Pid}), D)};
        {ok, _Pos} ->
            case maps:get(Pid, D#st.pending_qos, 1) of
                2 ->
                    reply_pubrel(Pid, D);
                1 ->
                    %% We never sent a QoS 2 PUBLISH under this id: advancing
                    %% the peer into PUBREL would complete a handshake that
                    %% does not exist on our side.
                    log(warning, "pubrec ~p for a QoS 1 delivery ignored", [Pid]),
                    {state, connected, D}
            end
    end.

reply_pubrel(Pid, #st{released = Released} = D) ->
    D1 = D#st{released = Released#{Pid => true}, inflight_dirty = true},
    {state, connected, send_frame(mqtt_frame:encode(#pubrel{packet_id = Pid}), D1)}.

handle_pubrel(Pid, #st{received = Received} = D) ->
    case maps:take(Pid, Received) of
        {true, Rest} ->
            %% The inbound QoS 2 message completed. It was delivered at
            %% PUBLISH time (Method A, R5), so there is no cursor here.
            D1 = D#st{received = Rest, inflight_dirty = true},
            {state, connected,
             maybe_snapshot(send_frame(mqtt_frame:encode(#pubcomp{packet_id = Pid}), D1))};
        error ->
            %% R1: a PUBREL for an id we no longer know (a crash ate the
            %% `received' entry, or a duplicate after reconnect). PUBCOMP is
            %% its idempotent answer; disconnecting would loop the client.
            log(info, "pubrel ~p matched nothing, replying pubcomp", [Pid]),
            {state, connected, send_frame(mqtt_frame:encode(#pubcomp{packet_id = Pid}), D)}
    end.

handle_pubcomp(Pid, #st{pending = Pending} = D) ->
    broker_metrics:inc(ack_total),
    case {maps:find(Pid, Pending), maps:is_key(Pid, D#st.released)} of
        {{ok, {retained, _Topic}}, true} ->
            finish_ack(Pid, D);
        {{ok, {Refs, {P, O} = Pos}}, true} ->
            D1 = release_pid(Pid, Pos, D),
            lists:foreach(fun(Ref) -> ack_skip(Ref, P, O) end, Refs),
            after_release(D1);
        _ ->
            %% R2: unknown, or a PUBCOMP for an entry that never entered the
            %% released stage. Nothing to advance; never a disconnect.
            log(info, "pubcomp ~p matched nothing", [Pid]),
            {state, connected, D}
    end.

%% Drop every trace of an outbound delivery: the entry, its level, its release
%% mark and its reverse index. Nothing here touches a cursor.
release_pid(Pid, Pos, D) ->
    observe_ack(Pid, D),
    D#st{pending = maps:remove(Pid, D#st.pending),
         pending_qos = maps:remove(Pid, D#st.pending_qos),
         released = maps:remove(Pid, D#st.released),
         pending_rev = remove_rev(D#st.pending_rev, Pos, Pid),
         sent = maps:remove(Pid, D#st.sent),
         sent_at = maps:remove(Pid, D#st.sent_at),
         inflight_dirty = true}.

%% Window-only release (retained replay) followed by the rest of the backlog.
finish_ack(Pid, D) ->
    D1 = release_pid(Pid, maps:get(Pid, D#st.pending), D),
    drain_retained(maybe_snapshot(D1)).

%% A freed window slot is the only thing that can resume a retained backlog
%% that stopped on a full window, so every ack path funnels through here.
after_release(D) ->
    %% `drain_retained/1' only ever answers `{state, connected, _}' -- it either
    %% sends what fits or stops draining with the queue intact -- so there is one
    %% shape to handle, not two.
    {state, connected, D1} = drain_retained(D),
    schedule_round(maybe_snapshot(D1)).

remove_rev(Rev, Pos, Pid) ->
    case maps:get(Pos, Rev, undefined) of
        Pid -> maps:remove(Pos, Rev);
        _ -> Rev
    end.

observe_ack(Pid, #st{sent_at = At}) ->
    case maps:get(Pid, At, undefined) of
        T when is_integer(T) ->
            broker_metrics:observe(
              ack_latency_us, max(0, erlang:monotonic_time(microsecond) - T));
        _ ->
            ok
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
            %%
            %% R7: a new delivery is new in-flight state, so the round schedules
            %% the blob write (coalesced by `maybe_snapshot'). Without this the
            %% snapshot would only be written when an ack arrived or at
            %% terminate/3, and a held PUBACK would leave the blob stale.
            {progress, D1} -> schedule_round(maybe_snapshot(D1));
            {_verdict, D1} -> {state, connected, maybe_snapshot(D1)}
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
            %% §4.8: the delivery level is the message's own level capped by the
            %% max QoS of the subscriptions that HIT it. Since S2-C the stored
            %% message's level can be 0, 1 or 2, so this is a real `min', not
            %% the old constant 1.
            SubMax = lists:max([Q || {_Ref, Q} <- Hits]),
            case min(SubMax, qos_of(Msg)) of
                0 ->
                    lists:foreach(fun(Ref) -> ack_skip(Ref, P, O) end,
                                  Others ++ Refs),
                    note_delivery(Msg),
                    {send_frame(publish_bin(0, 0, Msg, undefined), D), progress};
                DeliverQoS ->
                    deliver(P, O, Msg, DeliverQoS, Refs, Others, D)
            end
    end.

%% One delivery at the level this client granted. The three cases differ only in
%% what the peer owes back: nothing (QoS 0), PUBACK (QoS 1), or
%% PUBREC->PUBCOMP (QoS 2).
deliver(P, O, Msg, DeliverQoS, Refs, Others, D) ->
    Pos = {P, O},
    lists:foreach(fun(Ref) -> ack_skip(Ref, P, O) end, Others),
    case maps:get(Pos, D#st.pending_rev, undefined) of
        Pid when is_integer(Pid) ->
            %% Already in flight for this {Partition, Offset}: either restored
            %% from the blob after a reconnect, or held here because its ack has
            %% not arrived (that sub's cursor is still at O, which is what made
            %% this page start here).
            case maps:is_key(Pid, D#st.sent) of
                true ->
                    %% This connection already put it on the wire. The cursor has
                    %% not moved, so every round lands here again -- and resending
                    %% on each of them would be a retry timer S1 does not have:
                    %% §4.1's redelivery is the RECONNECT path, which starts with
                    %% an empty `sent' set. Silent, and not progress.
                    {D, idle};
                false ->
                    resend(Pid, P, O, Msg, D)
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
                    D2 = mark_sent_at(Pid, mark_sent(Pid, D1#st{
                             pending = maps:put(Pid, {Refs, Pos}, D1#st.pending),
                             pending_qos = maps:put(Pid, DeliverQoS,
                                                    D1#st.pending_qos),
                             pending_rev = maps:put(Pos, Pid, D1#st.pending_rev),
                             inflight_dirty = true})),
                    note_delivery(Msg),
                    {send_frame(publish_bin(DeliverQoS, 0, Msg, Pid), D2), progress}
            end
    end.

%% R7: the SAME packet_id with DUP=1 -- a fresh id would defeat the client's
%% duplicate check. Two shapes:
%%
%%   * restored and already PUBREC'd (`released'): the peer is past PUBLISH, so
%%     only PUBREL is owed. Re-PUBLISHing would restart a handshake it has
%%     already half-completed (ADR-018 §18.3 R3).
%%   * everything else: re-PUBLISH at the level recorded in `pending_qos', which
%%     is what keeps a QoS 2 delivery from being downgraded to QoS 1 on resend.
resend(Pid, _P, _O, Msg, D) ->
    case maps:is_key(Pid, D#st.released) of
        true ->
            %% Mark it sent so the next round does not repeat the PUBREL every
            %% poll tick; the peer's PUBCOMP is the only thing that can finish it.
            {send_frame(mqtt_frame:encode(#pubrel{packet_id = Pid}),
                        mark_sent_at(Pid, mark_sent(Pid, D))), idle};
        false ->
            QoS = maps:get(Pid, D#st.pending_qos, 1),
            note_delivery(Msg),
            {send_frame(publish_bin(QoS, 1, Msg, Pid),
                        mark_sent_at(Pid, mark_sent(Pid, D))), idle}
    end.

mark_sent(Pid, #st{sent = Sent} = D) ->
    D#st{sent = Sent#{Pid => true}}.

mark_sent_at(Pid, #st{sent_at = At} = D) ->
    D#st{sent_at = At#{Pid => erlang:monotonic_time(microsecond)}}.

%% A PUBLISH frame hit the wire. `created_at' is the broker's receive time in
%% milliseconds, so the latency is (now - created) in microseconds -- the path
%% from accept to wire, not just the pull loop.
note_delivery(Msg) ->
    broker_metrics:inc(deliver_total),
    Created = Msg#broker_message.created_at,
    broker_metrics:observe(
      deliver_latency_us,
      max(0, erlang:system_time(microsecond) - Created * 1000)).

%% ADR-018 §18.3 R4: the outbound id space is `1..65535' and wraps. A wrapped
%% id that is still live in `pending' would alias two deliveries; scan forward
%% for a free one, and treat exhaustion (every id outstanding, i.e. max_inflight
%% >= 65535) as a programming error rather than a silent alias.
take_pid(#st{pending = Pending} = D) ->
    take_pid(D#st.next_pid, Pending, 0, D).

take_pid(_N, _Pending, 65536, _D) ->
    erlang:error(no_free_packet_id);
take_pid(N, Pending, Tries, D) ->
    case maps:is_key(N, Pending) of
        false -> {N, D#st{next_pid = next_pid_after(N)}};
        true -> take_pid(next_pid_after(N), Pending, Tries + 1, D)
    end.

next_pid_after(65535) -> 1;
next_pid_after(N) -> N + 1.

publish_bin(QoS, Dup, Msg, Pid) ->
    publish_bin(QoS, Dup, 0, Msg, Pid).

%% `RetainFlag' is 1 only on a retained replay (§3.3.1.3: a live subscriber
%% always sees retain=0, a replay always sees retain=1).
publish_bin(QoS, Dup, RetainFlag, Msg, Pid) ->
    mqtt_frame:encode(#publish{qos = QoS, dup = Dup, retain = RetainFlag,
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
%%
%% Shape is `vsn 2' (ADR-018 §18.3 R3): `pending' carries both QoS 1 and QoS 2
%% entries with `qos' as the companion map, `released' marks the entries that
%% already saw PUBREC (resume with PUBREL, not a re-PUBLISH), and `received'
%% covers the inbound direction. Retained replays are filtered out: they have no
%% {Partition, Offset} to resume from and the client re-subscribes to get them,
%% so persisting them would only strand window slots.
blob(D) ->
    Positional = [{Pid, Entry}
                  || {Pid, Entry} <- maps:to_list(D#st.pending),
                     is_position_entry(Entry)],
    Pids = [Pid || {Pid, _} <- Positional],
    #{vsn => 2,
      next_pid => D#st.next_pid,
      pending => Positional,
      qos => maps:with(Pids, D#st.pending_qos),
      released => maps:with(Pids, D#st.released),
      received => D#st.received}.

is_position_entry({_Refs, {P, O}}) -> is_integer(P) andalso is_integer(O);
is_position_entry(_) -> false.

%%%_* Outbound frames ----------------------------------------------------------

send_frame(Bin, #st{socket = Sock} = D) ->
    case (catch gen_tcp:send(Sock, Bin)) of
        ok ->
            broker_metrics:inc(bytes_out, byte_size(Bin)),
            D;
        Other ->
            %% A dead socket shows up as {tcp_closed, _} shortly; a failed send
            %% is not a protocol error, and S1 has no backpressure to fail.
            log(warning, "send failed ~p", [Other]),
            D
    end.

%% connections_current is a gauge tracking a live count, so it needs the atomic
%% read-modify-write of gauge_add/2; the peak is a best-effort max (a lost
%% update under a connect storm can only under-report, never over-report).
count_connection(D) ->
    case broker_metrics:gauge_add(connections_current, 1) of
        N when is_integer(N), N >= 1 ->
            case broker_metrics:gauge_add(connections_peak, 0) of
                Peak when is_integer(Peak), N > Peak ->
                    broker_metrics:gauge(connections_peak, N);
                _ ->
                    ok
            end,
            D#st{counted = true};
        _ ->
            %% metrics disabled: nothing was added, so nothing may be removed
            D
    end.

uncount_connection(#st{counted = true} = D) ->
    _ = broker_metrics:gauge_add(connections_current, -1),
    D#st{counted = false};
uncount_connection(D) ->
    D.

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
%%
%% Will (ADR-018 §18.1): published on every abnormal ending -- TCP death,
%% keepalive timeout, protocol error, internal failure, takeover -- and NOT on
%% `mqtt_disconnect' (the client said goodbye) or `shutdown' (the broker is
%% stopping; its queue workers may already be gone, and a Will storm during a
%% restart is not useful). It runs BEFORE detach so a subscriber observing the
%% Will still sees this session as the owner of the client id.
terminate(Reason, _StateName, #st{partitions = Ps} = D) ->
    _ = uncount_connection(D),
    publish_will(Reason, D),
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

publish_will(mqtt_disconnect, _D) ->
    ok;
publish_will(shutdown, _D) ->
    ok;
publish_will(_Reason, #st{will = undefined}) ->
    ok;
publish_will(Reason, #st{will = Will, tenant_id = Tenant, principal = Principal}) ->
    %% Everything about publishing a Will can fail (the queue may be down, the
    %% retain app may be restarting). A dying process must not log a crash on
    %% top of the reason it is dying, so the failure is contained here.
    _ = (catch mqtt_will:publish(Will, Tenant, Principal)),
    log(warning, "will published (reason=~p, client_id=~p)", [Reason, self()]),
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