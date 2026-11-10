%% @doc One MQTT-SN 1.2 session (one client, one endpoint).
%%
%% This process is the whole protocol state machine: CONNECT and the Will
%% exchange, the three kinds of topic id, QoS -1/0/1/2 in both directions, sleep
%% and wake, keepalive. It deliberately does **not** reuse `mqtt_conn_fsm':
%% MQTT-SN's wire layer, its topic-id model and its sleep semantics differ
%% enough that forcing both into one FSM would put two sets of MQTT semantics in
%% one state record. What it does reuse is everything below the wire --
%% `broker_router' for the partition, `broker_queue' for storage,
%% `broker_subscription' for cursors and wakeups, `broker_retain' for retained
%% state, `broker_event' for the CloudEvents policy seam -- and the session key
%% is the MQTT-SN `client_id', so a sensor and an MQTT client naming the same id
%% share one session space, takeover included.
%%
%% Transport shape: the listener owns the UDP socket and forwards decoded
%% packets. Replies go straight out through `gen_udp:send/4'; any process may
%% write a UDP socket, so there is no controlling-process hand-off here.
%%
%% Two timers, no more:
%%   * keepalive -- after 1.5x the client's declared duration of silence the
%%     gateway asks "are you there" with a PINGREQ; at 2x the session is dead and
%%     its Will fires. A live client sends its own PINGREQ long before that.
%%   * sleep -- a client that DISCONNECTs with a duration is detached (nothing is
%%     pushed at it) and woken by its next PINGREQ, or dropped when the duration
%%     runs out.
%% Retransmission is not a third timer: the client owns it (DUP), and this
%% process only has to answer a repeated packet id idempotently.
-module(mqtt_sn_conn).

-behaviour(gen_server).

-export([start_link/4, publish_stateless/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

%% MQTT-SN 1.2 §5.4.8 return codes.
-define(RC_ACCEPTED, 16#00).
-define(RC_INVALID_TOPIC_ID, 16#02).
-define(RC_NOT_SUPPORTED, 16#03).

-define(MAX_MSG_ID, 65535).

-record(st, {
    listener :: pid(),
    socket :: gen_udp:socket(),
    endpoint :: {inet:ip_address(), inet:port_number()},
    cfg :: map(),
    client_id :: binary() | undefined,
    tenant :: binary(),
    principal :: binary() | undefined,
    clean = true :: boolean(),
    %% Keepalive period the client asked for, in seconds; 0 disables it.
    duration = 0 :: non_neg_integer(),
    %% SubRef => #{filter, qos}. Mirrors broker_subscription so a delivery only
    %% has to look at ids this session actually consumes.
    subs = #{} :: map(),
    %% Topic name => normal topic id (per client, per session).
    topics = #{} :: map(),
    %% MsgId => #{refs, partition, offset, qos} for downlink QoS 1/2.
    pending = #{} :: map(),
    %% MsgId => true for uplink QoS 2 that has been delivered and awaits PUBREL.
    received = #{} :: map(),
    next_id = 1 :: pos_integer(),
    will :: undefined | {binary(), binary(), 0..2, boolean()},
    will_stage = none :: none | want_topic | want_msg,
    asleep = false :: boolean(),
    counted = false :: boolean(),
    sleep_timer :: reference() | undefined,
    keepalive_timer :: reference() | undefined
}).

%%%_* API -------------------------------------------------------------------

start_link(Listener, Socket, Endpoint, Cfg) ->
    gen_server:start_link(?MODULE, {Listener, Socket, Endpoint, Cfg}, []).

%% @doc A QoS -1 publish from an endpoint with no session.
%%
%% QoS -1 exists precisely so a device can publish without connecting, so
%% dropping those would drop the feature. With no session there is no per-client
%% id table, which leaves the two stateless topic forms: predefined
%% (gateway-wide by definition) and short (the name travels in the packet). A
%% `normal' id is session-scoped and therefore meaningless here.
%%
%% It runs in the caller (the listener) and does no storage: QoS -1 is
%% fire-and-forget, exactly like MQTT's QoS 0 which never enters the log, so
%% there is nothing to block on.
-spec publish_stateless(map(), map()) -> ok.
publish_stateless(#{topic_id_type := Type, topic_id := Ref, payload := Payload,
                    retain := Retain} = Packet, Cfg) ->
    case stateless_topic(Type, Ref, Cfg) of
        {ok, Topic} ->
            Msg = new_message(broker_security:default_tenant(), Topic, Payload,
                              -1, Retain),
            case publish_routed(Msg, -1, Retain) of
                ok ->
                    ok;
                {error, Reason} ->
                    broker_metrics:inc(protocol_errors),
                    logger:warning("mqtt_sn stateless publish dropped: ~p",
                                   [Reason])
            end;
        {error, Reason} ->
            broker_metrics:inc(protocol_errors),
            logger:debug("mqtt_sn qos -1 without session dropped: ~p (~p)",
                         [Reason, maps:get(topic_id, Packet, undefined)])
    end.

stateless_topic(predefined, Id, Cfg) when is_integer(Id) ->
    case maps:find(Id, maps:get(predefined, Cfg)) of
        {ok, Topic} -> {ok, Topic};
        error -> {error, {unknown_predefined_topic, Id}}
    end;
stateless_topic(short, Name, _Cfg) when is_binary(Name) ->
    {ok, Name};
stateless_topic(normal, Id, _Cfg) ->
    {error, {session_scoped_topic_id, Id}};
stateless_topic(_Type, Ref, _Cfg) ->
    {error, {bad_topic_ref, Ref}}.

%%%_* Lifecycle -------------------------------------------------------------

init({Listener, Socket, Endpoint, Cfg}) ->
    %% Trap exits so `terminate/2' runs on a supervisor shutdown: the Will
    %% decision lives there, and a node going down must not publish anybody's
    %% Will.
    process_flag(trap_exit, true),
    {ok, #st{listener = Listener, socket = Socket, endpoint = Endpoint,
             cfg = Cfg, tenant = broker_security:default_tenant()}}.

terminate(Reason, #st{} = St) ->
    case abnormal(Reason) of
        true -> publish_will(St);
        false -> ok
    end,
    case St#st.counted of
        true -> _ = broker_metrics:gauge_add(connections_current, -1);
        false -> ok
    end,
    detach(St),
    ok.

%% `normal' is a clean DISCONNECT or a takeover, `shutdown' is the node going
%% down. In none of those did the client die, so the Will must not fire.
abnormal(normal) -> false;
abnormal(shutdown) -> false;
abnormal({shutdown, _}) -> false;
abnormal(_) -> true.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* gen_server ------------------------------------------------------------

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({sn_packet, Packet}, St) ->
    handle_packet(Packet, St);
handle_info({broker_deliver, ClientId, MaxQoS, Refs, Delivery},
            #st{client_id = ClientId} = St) ->
    on_deliver(Refs, MaxQoS, Delivery, St);
handle_info({broker_deliver, _Other, _MaxQoS, _Refs, _Delivery}, St) ->
    %% A delivery addressed to a session this process no longer owns (it was
    %% taken over and the attachment moved on). Ignoring it is the whole point of
    %% the client-id guard.
    {noreply, St};
handle_info({sn_endpoint, Endpoint}, St) ->
    {noreply, St#st{endpoint = Endpoint}};
handle_info({sn_takeover, _NewPid}, St) ->
    %% The client reconnected from elsewhere. Not a death: no Will.
    {stop, normal, St};
handle_info({sn_sleep_granted, _Listener}, St) ->
    {noreply, enter_sleep(St)};
handle_info({sn_sleep_denied, _Listener}, St) ->
    %% The sleeping census is full. The DISCONNECT reply already went out; the
    %% session now ends like a normal disconnect instead of pretending to sleep
    %% and silently losing everything published meanwhile.
    {stop, normal, St};
handle_info({timeout, Ref, keepalive_probe}, #st{keepalive_timer = Ref} = St) ->
    {noreply, on_keepalive_probe(St)};
handle_info({timeout, Ref, keepalive_dead}, #st{keepalive_timer = Ref} = St) ->
    broker_metrics:inc(mqtt_sn_keepalive_timeout),
    logger:info("mqtt_sn client ~p silent for 2x keepalive, dropping",
                [St#st.client_id]),
    {stop, keepalive_timeout, St};
handle_info({timeout, Ref, sleep_expired}, #st{sleep_timer = Ref} = St) ->
    %% The client asked to sleep until now and never came back. The session is
    %% over; nothing was lost that a longer sleep would have kept, because the
    %% messages are still in the partitions for its next CONNECT.
    {stop, normal, St};
handle_info({timeout, _Stale, _Tag}, St) ->
    %% A timer we already cancelled; the mailbox can always deliver one more.
    {noreply, St};
handle_info(_Other, St) ->
    {noreply, St}.

%%%_* Packet dispatch -------------------------------------------------------

handle_packet(#{type := connect} = P, St) ->
    on_connect(P, St);
handle_packet(#{type := willtopic} = P, #st{will_stage = want_topic} = St) ->
    on_will_topic(P, St);
handle_packet(#{type := willtopic}, St) ->
    {noreply, St};
handle_packet(#{type := willmsg} = P, #st{will_stage = want_msg} = St) ->
    on_will_msg(P, St);
handle_packet(#{type := willmsg}, St) ->
    {noreply, St};
handle_packet(#{type := publish} = P, St) ->
    on_publish(P, St);
handle_packet(#{type := puback} = P, St) ->
    on_puback(P, St);
handle_packet(#{type := pubrec} = P, St) ->
    on_pubrec(P, St);
handle_packet(#{type := pubrel} = P, St) ->
    on_pubrel(P, St);
handle_packet(#{type := pubcomp} = P, St) ->
    on_pubcomp(P, St);
handle_packet(#{type := subscribe} = P, St) ->
    on_subscribe(P, St);
handle_packet(#{type := unsubscribe} = P, St) ->
    on_unsubscribe(P, St);
handle_packet(#{type := register} = P, St) ->
    on_register(P, St);
handle_packet(#{type := pingreq} = P, St) ->
    on_pingreq(P, St);
handle_packet(#{type := disconnect} = P, St) ->
    on_disconnect(P, St);
handle_packet(#{type := willtopicupd} = P, St) ->
    on_will_topic_upd(P, St);
handle_packet(#{type := willmsgupd} = P, St) ->
    on_will_msg_upd(P, St);
handle_packet(#{type := regack}, St) ->
    %% The client acknowledging a REGISTER *we* sent. The id is already in our
    %% table, so there is nothing to learn; a client that answers with a
    %% different id is ignored rather than allowed to rewrite the table.
    {noreply, St};
handle_packet(Packet, St) ->
    %% A gateway-only packet arrived at a gateway: ADVERTISE/GWINFO/CONNACK from
    %% a client, or a response we are not waiting for. There is no correct reply
    %% for "you sent the wrong direction", so count and drop.
    broker_metrics:inc(protocol_errors),
    logger:debug("mqtt_sn ignored inbound ~p", [maps:get(type, Packet, unknown)]),
    {noreply, St}.

%%%_* CONNECT + Will --------------------------------------------------------

on_connect(#{client_id := ClientId, protocol_id := ProtocolId,
             clean_session := Clean, duration := Duration, will := Will}, St) ->
    case ProtocolId of
        1 ->
            MaxDuration = maps:get(max_duration_s, St#st.cfg),
            Dur = min(Duration, MaxDuration),
            Persist = not Clean,
            %% `attach/3' answers the pid that was attached before us, if any
            %% (takeover). Its contract has no error branch.
            {ok, Prev} = broker_subscription:attach(ClientId, self(), Persist),
            maybe_takeover(Prev),
            %% §6.2: a clean session starts from zero. Dropping our local map
            %% alone would leave the entities (and their cursors) attached to
            %% this client id, so the next publish would still be routed here.
            case Clean of
                true -> clean_session_teardown(ClientId);
                false -> ok
            end,
            Subs = case Persist of
                       true -> session_subs(ClientId);
                       false -> #{}
                   end,
            St1 = St#st{client_id = ClientId, clean = Clean, duration = Dur,
                        subs = Subs,
                        %% A fresh CONNECT clears the topic table: ids handed out
                        %% in a previous session are not something a client may
                        %% carry across a reconnect (it re-REGISTERs, which is
                        %% the contract).
                        topics = #{},
                        counted = true},
            _ = broker_metrics:gauge_add(connections_current, 1),
            gen_server:cast(St#st.listener, {sn_bind, self(), ClientId}),
            case Will of
                true ->
                    %% The Will exchange runs *before* CONNACK: a client must not
                    %% consider itself connected until its Will is stored, or a
                    %% death in between would lose it (§6.10).
                    {noreply, reply(St1#st{will_stage = want_topic},
                                    #{type => willtopicreq})};
                false ->
                    finish_connect(Dur, St1)
            end;
        _Other ->
            %% MQTT-SN 1.2 is protocol id 1. Anything else gets "not supported"
            %% rather than a guess.
            {stop, normal, reply(St, #{type => connack,
                                       return_code => ?RC_NOT_SUPPORTED})}
    end.

finish_connect(Duration, St) ->
    St1 = reply(St, #{type => connack, return_code => ?RC_ACCEPTED}),
    %% CONNACK goes first: a reconnecting client must know its session was
    %% accepted before the first replayed message arrives.
    case catch_up(St1) of
        {ok, St2} ->
            {noreply, arm_keepalive(Duration, St2)};
        {stop, Reason, St2} ->
            {stop, Reason, St2}
    end.

on_will_topic(#{topic := <<>>}, St) ->
    %% An empty topic cancels the Will (MQTT-SN §6.10).
    {noreply, reply(St#st{will = undefined, will_stage = none},
                    #{type => willmsgreq})};
on_will_topic(#{topic := Topic, qos := QoS, retain := Retain}, St) ->
    case mqtt_sn_filter:validate_topic(Topic) of
        ok ->
            Will = {Topic, <<>>, QoS, Retain},
            {noreply, reply(St#st{will = Will, will_stage = want_msg},
                            #{type => willmsgreq})};
        {error, _Reason} ->
            %% An unusable Will topic cannot be signalled mid-handshake. Drop the
            %% Will and finish the exchange: connecting a client whose death
            %% would publish to a garbage topic helps nobody.
            {noreply, reply(St#st{will = undefined, will_stage = none},
                            #{type => willmsgreq})}
    end.

on_will_msg(#{payload := Payload}, St) ->
    Will = case St#st.will of
               {Topic, _Old, QoS, Retain} -> {Topic, Payload, QoS, Retain};
               undefined -> undefined
           end,
    finish_connect(St#st.duration, St#st{will = Will, will_stage = none}).

on_will_topic_upd(#{topic := <<>>}, St) ->
    %% An empty topic deletes the Will.
    {noreply, reply(St#st{will = undefined},
                    #{type => willtopicresp, return_code => ?RC_ACCEPTED})};
on_will_topic_upd(#{topic := Topic, qos := QoS, retain := Retain}, St) ->
    case mqtt_sn_filter:validate_topic(Topic) of
        ok ->
            Payload = case St#st.will of
                          {_, P, _, _} -> P;
                          undefined -> <<>>
                      end,
            {noreply, reply(St#st{will = {Topic, Payload, QoS, Retain}},
                            #{type => willtopicresp,
                              return_code => ?RC_ACCEPTED})};
        {error, _} ->
            {noreply, reply(St, #{type => willtopicresp,
                                  return_code => ?RC_NOT_SUPPORTED})}
    end.

on_will_msg_upd(#{payload := Payload}, St) ->
    case St#st.will of
        {Topic, _Old, QoS, Retain} ->
            {noreply, reply(St#st{will = {Topic, Payload, QoS, Retain}},
                            #{type => willmsgresp,
                              return_code => ?RC_ACCEPTED})};
        undefined ->
            %% No Will topic means there is nothing to attach a message to.
            {noreply, reply(St, #{type => willmsgresp,
                                  return_code => ?RC_NOT_SUPPORTED})}
    end.

publish_will(#st{will = undefined}) ->
    ok;
publish_will(#st{will = {Topic, Payload, QoS, Retain}} = St) ->
    case store_publish(Topic, Payload, QoS, Retain, St) of
        ok ->
            broker_metrics:inc(will_published);
        {error, Reason} ->
            logger:warning("mqtt_sn could not publish will for ~p: ~p",
                           [St#st.client_id, Reason])
    end,
    ok.

%%%_* PUBLISH (uplink) ------------------------------------------------------

on_publish(#{topic_id_type := Type, topic_id := Ref, msg_id := MsgId,
             qos := QoS, retain := Retain, payload := Payload},
           #st{client_id = undefined} = St) ->
    %% Only reachable for a packet that names no session; the listener already
    %% dealt with stateless QoS -1, so this is a client bug.
    _ = {Type, Ref, MsgId, QoS, Retain, Payload},
    {noreply, St};
on_publish(#{topic_id_type := Type, topic_id := Ref, msg_id := MsgId,
             qos := QoS, retain := Retain, payload := Payload}, St) ->
    case resolve_topic(Type, Ref, St) of
        {ok, Topic} ->
            publish_allowed(Topic, Payload, QoS, MsgId, Type, Ref, Retain, St);
        {error, Reason} ->
            reject_publish(QoS, MsgId, Type, Ref, Reason, St)
    end.

publish_allowed(Topic, Payload, QoS, MsgId, Type, Ref, Retain, St) ->
    case {acl_publish(Topic, St), mqtt_sn_filter:validate_topic(Topic)} of
        {allow, ok} ->
            case byte_size(Payload) =< broker_limits:max_message_bytes() of
                true ->
                    do_publish(Topic, Payload, QoS, MsgId, Type, Ref, Retain, St);
                false ->
                    reject_publish(QoS, MsgId, Type, Ref, payload_too_large, St)
            end;
        {deny, _} ->
            broker_metrics:inc(authz_failures),
            logger:warning("mqtt_sn publish not authorized topic=~p tenant=~p",
                           [Topic, St#st.tenant]),
            reject_publish(QoS, MsgId, Type, Ref, not_authorized, St);
        {_, {error, Reason}} ->
            reject_publish(QoS, MsgId, Type, Ref, Reason, St)
    end.

do_publish(Topic, Payload, 2, MsgId, _Type, _Ref, Retain,
           #st{received = Received} = St) ->
    case maps:is_key(MsgId, Received) of
        true ->
            %% Second attempt for an id already delivered: answer PUBREC again
            %% and do NOT store a second copy. The client's PUBREL then
            %% completes the handshake exactly once.
            {noreply, reply(St, #{type => pubrec, msg_id => MsgId})};
        false ->
            case store_publish(Topic, Payload, 2, Retain, St) of
                ok ->
                    St1 = St#st{received = maps:put(MsgId, true, Received)},
                    {noreply, reply(St1, #{type => pubrec, msg_id => MsgId})};
                {error, Reason} ->
                    %% No PUBREC: the client retries, which is the only honest
                    %% signal this protocol has for "not accepted".
                    logger:warning("mqtt_sn publish (qos2) failed: ~p", [Reason]),
                    {noreply, St}
            end
    end;
do_publish(Topic, Payload, QoS, MsgId, Type, Ref, Retain, St) ->
    case store_publish(Topic, Payload, QoS, Retain, St) of
        ok ->
            {noreply, ack_uplink(QoS, MsgId, Type, Ref, St)};
        {error, Reason} ->
            logger:warning("mqtt_sn publish (qos~p) failed: ~p", [QoS, Reason]),
            {noreply, St}
    end.

%% QoS 1 gets a PUBACK naming the topic id it answered; QoS 0 and -1 get nothing
%% at all (that *is* their contract).
ack_uplink(1, MsgId, Type, Ref, St) ->
    reply(St, #{type => puback, topic_id => topic_field(Type, Ref),
                msg_id => MsgId, return_code => ?RC_ACCEPTED});
ack_uplink(_, _MsgId, _Type, _Ref, St) ->
    St.

%% A rejected uplink has one honest answer per QoS, and none for the levels
%% whose contract is "no answer": QoS 1 carries the code in its PUBACK, QoS 2
%% gets nothing so the client retries rather than believe it landed, QoS 0 and
%% -1 are already fire-and-forget.
reject_publish(1, MsgId, Type, Ref, Reason, St) ->
    broker_metrics:inc(protocol_errors),
    RC = case Reason of
             invalid_topic_id -> ?RC_INVALID_TOPIC_ID;
             _ -> ?RC_NOT_SUPPORTED
         end,
    {noreply, reply(St, #{type => puback, topic_id => topic_field(Type, Ref),
                          msg_id => MsgId, return_code => RC})};
reject_publish(_QoS, _MsgId, _Type, _Ref, Reason, St) ->
    broker_metrics:inc(protocol_errors),
    logger:debug("mqtt_sn publish dropped: ~p", [Reason]),
    {noreply, St}.

%% PUBLISH/PUBACK/REGACK carry the topic-id field in its wire shape: the
%% two-character name for a short topic, a 16-bit number otherwise.
topic_field(short, Name) when is_binary(Name) ->
    case Name of
        <<A, B>> -> (A bsl 8) bor B;
        _ -> 0
    end;
topic_field(_Type, Id) when is_integer(Id) ->
    Id;
topic_field(_Type, _Other) ->
    0.

%%%_* QoS 1/2 handshake (downlink) -----------------------------------------

on_puback(#{msg_id := MsgId}, St) ->
    case maps:take(MsgId, St#st.pending) of
        {#{qos := 1} = Entry, Pending} ->
            {noreply, ack_delivery(Entry, St#st{pending = Pending})};
        {#{qos := 2}, _} ->
            %% A PUBACK for a QoS 2 delivery is not part of the handshake; let
            %% PUBREC/PUBCOMP drive it.
            {noreply, St};
        error ->
            %% Unknown id: acked twice, or acked after we already released it.
            {noreply, St}
    end.

on_pubrec(#{msg_id := MsgId}, St) ->
    case maps:find(MsgId, St#st.pending) of
        {ok, #{qos := 2} = Entry} ->
            {noreply, reply(St#st{pending = maps:put(MsgId, Entry, St#st.pending)},
                            #{type => pubrel, msg_id => MsgId})};
        _ ->
            {noreply, St}
    end.

on_pubcomp(#{msg_id := MsgId}, St) ->
    case maps:take(MsgId, St#st.pending) of
        {#{qos := 2} = Entry, Pending} ->
            {noreply, ack_delivery(Entry, St#st{pending = Pending})};
        _ ->
            {noreply, St}
    end.

%% The client finished a QoS 2 uplink. PUBCOMP closes it; the message was
%% already delivered on PUBLISH (METHOD A, same as the MQTT side).
on_pubrel(#{msg_id := MsgId}, St) ->
    Received = maps:remove(MsgId, St#st.received),
    {noreply, reply(St#st{received = Received}, #{type => pubcomp,
                                                  msg_id => MsgId})}.

%%%_* SUBSCRIBE / UNSUBSCRIBE / REGISTER ------------------------------------

on_subscribe(#{topic_id_type := Type, topic := Ref, msg_id := MsgId,
               qos := QoS}, St) ->
    case resolve_filter(Type, Ref, St) of
        {ok, Filter, St1} ->
            case valid_qos(QoS) of
                true ->
                    subscribe_allowed(Filter, QoS, Type, MsgId, St1);
                false ->
                    %% QoS -1 is not a subscription level.
                    suback(MsgId, Type, Ref, QoS, ?RC_NOT_SUPPORTED, St1)
            end;
        {error, Reason} ->
            broker_metrics:inc(protocol_errors),
            logger:warning("mqtt_sn subscribe with bad topic: ~p", [Reason]),
            suback(MsgId, Type, Ref, QoS, ?RC_INVALID_TOPIC_ID, St)
    end.

subscribe_allowed(Filter, QoS, Type, MsgId, St) ->
    case {acl_subscribe(Filter, St), mqtt_sn_filter:validate_filter(Filter)} of
        {allow, ok} ->
            case broker_subscription:subscribe(St#st.client_id, Filter, QoS,
                                               St#st.tenant) of
                {ok, SubRef} ->
                    grant(SubRef, Filter, QoS, Type, MsgId, St);
                {error, Reason} ->
                    logger:warning("mqtt_sn subscribe ~p refused: ~p",
                                   [Filter, Reason]),
                    suback(MsgId, Type, 0, QoS, ?RC_NOT_SUPPORTED, St)
            end;
        {deny, _} ->
            broker_metrics:inc(authz_failures),
            logger:warning("mqtt_sn subscribe not authorized ~p", [Filter]),
            suback(MsgId, Type, 0, QoS, ?RC_NOT_SUPPORTED, St);
        {_, {error, _Reason}} ->
            suback(MsgId, Type, 0, QoS, ?RC_NOT_SUPPORTED, St)
    end.

grant(SubRef, Filter, QoS, Type, MsgId, St) ->
    Subs = maps:put(SubRef, #{filter => Filter, qos => QoS}, St#st.subs),
    {Type1, Ref1} = granted_ref(Filter, Type, St),
    St1 = suback(MsgId, Type1, Ref1, QoS, ?RC_ACCEPTED, St#st{subs = Subs}),
    %% Retained messages are owed *after* SUBACK: the client must know its
    %% subscription exists before the first replay arrives.
    case replay_retained(SubRef, Filter, QoS, St1) of
        {ok, St2} -> {noreply, St2};
        {stop, Reason, St2} -> {stop, Reason, St2}
    end.

%% The granted topic id goes back in SUBACK. For a normal topic it is the id the
%% client must use in PUBLISH (allocated while resolving the filter); for
%% predefined and short nothing is allocated and the field echoes the client's.
granted_ref(Filter, normal, St) ->
    case maps:find(Filter, St#st.topics) of
        {ok, Id} -> {normal, Id};
        error -> {normal, 0}
    end;
granted_ref(_Filter, predefined, Ref) ->
    {predefined, Ref};
granted_ref(_Filter, short, Ref) ->
    {short, Ref}.

on_unsubscribe(#{topic_id_type := Type, topic := Ref, msg_id := MsgId}, St) ->
    case resolve_filter(Type, Ref, St) of
        {ok, Filter, St1} ->
            _ = broker_subscription:unsubscribe(St1#st.client_id, Filter),
            Subs = maps:filter(fun(_R, #{filter := F}) -> F =/= Filter end,
                               St1#st.subs),
            {noreply, reply(St1#st{subs = Subs},
                            #{type => unsuback, msg_id => MsgId})};
        {error, Reason} ->
            broker_metrics:inc(protocol_errors),
            logger:warning("mqtt_sn unsubscribe with bad topic: ~p", [Reason]),
            {noreply, reply(St, #{type => unsuback, msg_id => MsgId})}
    end.

%% The client registering a topic name: we assign the id and return it in
%% REGACK. The client's proposed id is ignored -- §5.4.12 makes the id the
%% gateway's to assign, and honouring a client-chosen one would let two names
%% collide.
on_register(#{topic := Topic, msg_id := MsgId}, St) ->
    case mqtt_sn_filter:validate_topic(Topic) of
        ok ->
            case assign_topic(Topic, St) of
                {ok, Id, St1} ->
                    {noreply, reply(St1, #{type => regack, topic_id => Id,
                                           msg_id => MsgId,
                                           return_code => ?RC_ACCEPTED})};
                {error, exhausted} ->
                    {noreply, reply(St, #{type => regack, topic_id => 0,
                                          msg_id => MsgId,
                                          return_code => ?RC_NOT_SUPPORTED})}
            end;
        {error, _} ->
            {noreply, reply(St, #{type => regack, topic_id => 0,
                                  msg_id => MsgId,
                                  return_code => ?RC_NOT_SUPPORTED})}
    end.

suback(MsgId, Type, Ref, QoS, RC, St) ->
    reply(St, #{type => suback, qos => max(QoS, 0),
                topic_id => topic_field(Type, Ref),
                msg_id => MsgId, return_code => RC}).

%%%_* PINGREQ / DISCONNECT --------------------------------------------------

on_pingreq(#{client_id := Named}, #st{client_id = ClientId} = St)
  when Named =:= <<>>; Named =:= ClientId ->
    {noreply, wake_and_answer(St)};
on_pingreq(_P, St) ->
    %% A named client that is not this session: a stale ping from a client that
    %% believes it is still connected here. The listener already handled "known
    %% elsewhere"; reaching here means we are the wrong session.
    {stop, normal, reply(St, #{type => disconnect, duration => 0})}.

wake_and_answer(#st{asleep = true} = St) ->
    St1 = cancel_sleep(St),
    gen_server:cast(St1#st.listener, {sn_wake, self()}),
    _ = broker_subscription:attach(St1#st.client_id, self(), not St1#st.clean),
    _ = broker_metrics:gauge_add(connections_current, 1),
    %% Wake means "I am back": replay what piled up while detached, then answer
    %% the ping. The client sees PINGRESP after its backlog, which is the order
    %% the sleep contract implies (the backlog is owed to it either way).
    St2 = (St1#st{asleep = false, counted = true}),
    case catch_up(St2) of
        {ok, St3} -> reply(arm_keepalive(St3#st.duration, St3), #{type => pingresp});
        {stop, _Reason, St3} -> St3
    end;
wake_and_answer(St) ->
    %% Not asleep: an ordinary liveness ping. Reset the keepalive clock, then
    %% use the round trip as the pull tick -- if the in-flight window refused a
    %% delivery earlier, this is what retries it from the cursor.
    case catch_up(St) of
        {ok, St1} ->
            reply(arm_keepalive(St1#st.duration, St1), #{type => pingresp});
        {stop, _Reason, St1} ->
            St1
    end.

on_disconnect(#{duration := 0}, St) ->
    %% Clean disconnect. `normal' tells terminate/2 not to publish the Will.
    {stop, normal, reply(St, #{type => disconnect, duration => 0})};
on_disconnect(#{duration := Duration}, #st{client_id = undefined} = St) ->
    _ = Duration,
    {stop, normal, St};
on_disconnect(#{duration := Duration}, St) ->
    %% Ask the listener for a sleeping slot; it answers asynchronously. Reply to
    %% the client first so it stops retrying the DISCONNECT.
    gen_server:cast(St#st.listener, {sn_sleep, self()}),
    {noreply, reply(St#st{duration = Duration},
                    #{type => disconnect, duration => Duration})}.

enter_sleep(St) ->
    %% Sleep = detach. Nothing is buffered per session: the partitions already
    %% hold QoS 1/2 and the cursor says where this client got to. QoS 0 is gone
    %% by definition (it never entered storage), which the capability matrix
    %% states rather than hides.
    _ = broker_subscription:detach(St#st.client_id, self()),
    _ = broker_metrics:gauge_add(connections_current, -1),
    TRef = erlang:start_timer(St#st.duration * 1000, self(), sleep_expired),
    cancel_keepalive(St#st{asleep = true, counted = false, sleep_timer = TRef}).

cancel_sleep(#st{sleep_timer = undefined} = St) ->
    St;
cancel_sleep(#st{sleep_timer = TRef} = St) ->
    _ = erlang:cancel_timer(TRef),
    St#st{sleep_timer = undefined}.

detach(#st{client_id = undefined}) ->
    ok;
detach(#st{client_id = ClientId}) ->
    %% `detach/2' is a no-op when this pid is no longer the attachment (a
    %% takeover moved it), which is exactly the guard we want here.
    _ = broker_subscription:detach(ClientId, self()),
    ok.

%%%_* Keepalive -------------------------------------------------------------

arm_keepalive(0, St) ->
    cancel_keepalive(St);
arm_keepalive(Duration, St) ->
    St1 = cancel_keepalive(St),
    Ms = round(Duration * 1500),
    St1#st{keepalive_timer = erlang:start_timer(Ms, self(), keepalive_probe)}.

cancel_keepalive(#st{keepalive_timer = undefined} = St) ->
    St;
cancel_keepalive(#st{keepalive_timer = TRef} = St) ->
    _ = erlang:cancel_timer(TRef),
    St#st{keepalive_timer = undefined}.

on_keepalive_probe(#st{duration = Duration} = St) ->
    %% 1.5x silence: ask. A compliant client answers with its own PINGREQ long
    %% before this; one that does not gets the remaining half period and then the
    %% session is declared dead.
    _ = reply(St, #{type => pingreq}),
    Ms = round(Duration * 500),
    St#st{keepalive_timer = erlang:start_timer(Ms, self(), keepalive_dead)}.

%%%_* Delivery (downlink) ---------------------------------------------------

on_deliver(Refs, MaxQoS, {Partition, Offset, Msg}, St) ->
    Mine = [Ref || Ref <- Refs, maps:is_key(Ref, St#st.subs)],
    case Mine of
        [] ->
            %% Every matching subscription belongs to someone else, or was
            %% removed after the wake was queued.
            {noreply, St};
        _ ->
            QoS = delivery_qos(Mine, MaxQoS, Msg, St),
            case deliver(Mine, QoS, {Partition, Offset}, Msg, false, St) of
                {ok, St1} -> {noreply, St1};
                {stop, Reason, St1} -> {stop, Reason, St1}
            end
    end.

delivery_qos(Refs, MaxQoS, Msg, St) ->
    Granted = lists:max([maps:get(qos, maps:get(Ref, St#st.subs)) || Ref <- Refs]),
    %% MQTT §4.8: the delivery level never exceeds what the subscription asked
    %% for, and never exceeds what was published. A QoS -1 publish is delivered
    %% at QoS 0 -- the only level below it that exists on the wire.
    Published = max(maps:get(qos, Msg#broker_message.proto_meta, 0), 0),
    min(min(MaxQoS, Granted), Published).

-spec deliver([term()], 0..2, {integer() | undefined, integer() | undefined},
              #broker_message{}, boolean(), #st{}) ->
          {ok, #st{}} | {stop, term(), #st{}}.
deliver(Refs, QoS, Pos, Msg, Retain, St) when QoS > 0 ->
    %% MQTT §4.4's in-flight window, the same one the MQTT side keeps: an
    %% unacked PUBLISH holds its cursor, so refusing to send more does not lose
    %% anything -- the message is still in the partition and the next catch-up
    %% (PINGREQ) retries it. Without this a slow client would let one session
    %% grow `pending' without bound.
    case inflight_full(St) of
        true -> {ok, St};
        false -> deliver1(Refs, QoS, Pos, Msg, Retain, St)
    end;
deliver(Refs, QoS, Pos, Msg, Retain, St) ->
    deliver1(Refs, QoS, Pos, Msg, Retain, St).

deliver1(Refs, QoS, Pos, Msg, Retain, St) ->
    Topic = Msg#broker_message.destination,
    case topic_for_delivery(Topic, St) of
        {ok, Type, Ref, Fresh, St1} ->
            St2 = case Fresh of
                      true ->
                          %% The client has never seen this id. REGISTER first:
                          %% the PUBLISH that follows names only the id, so a
                          %% client that has not read REGISTER cannot know what
                          %% topic it is looking at.
                          send_registration(Type, Ref, Topic, St1);
                      false ->
                          St1
                  end,
            St3 = send_publish(QoS, Msg, Ref, Retain, Type, Pos, Refs, St2),
            case QoS of
                0 -> {ok, ack_delivery_pos(Refs, Pos, St3)};
                _ -> {ok, St3}
            end;
        {error, exhausted} ->
            %% Decision 4: a full topic table on the *downlink* side cannot be
            %% papered over -- the gateway itself cannot encode a legal PUBLISH
            %% for this topic, so the session ends instead of pretending.
            logger:warning("mqtt_sn topic table exhausted for ~p, closing session",
                           [St#st.client_id]),
            {stop, topic_id_exhausted, St}
    end.

inflight_full(#st{pending = Pending, cfg = Cfg}) ->
    maps:size(Pending) >= maps:get(max_inflight, Cfg, 20).

%% Which (type, id) names `Topic' for this client? A predefined id wins (it is
%% the same for every client and needs no REGISTER); a two-character topic can
%% travel as itself; everything else gets a normal id, allocated on first use.
topic_for_delivery(Topic, St) ->
    Predefined = maps:get(predefined, St#st.cfg),
    case [Id || {Id, T} <- maps:to_list(Predefined), T =:= Topic] of
        [Id | _] ->
            {ok, predefined, Id, false, St};
        [] ->
            case {byte_size(Topic), maps:find(Topic, St#st.topics)} of
                {2, error} ->
                    {ok, short, Topic, false, St};
                {_, {ok, Id}} ->
                    {ok, normal, Id, false, St};
                {_, error} ->
                    case assign_topic(Topic, St) of
                        {ok, Id, St1} -> {ok, normal, Id, true, St1};
                        {error, exhausted} -> {error, exhausted}
                    end
            end
    end.

assign_topic(Topic, St) ->
    Max = maps:get(max_topic_ids, St#st.cfg),
    case mqtt_sn_topics:assign(St#st.topics, Topic, Max) of
        {ok, Id, Topics} -> {ok, Id, St#st{topics = Topics}};
        {error, exhausted} -> {error, exhausted};
        {error, empty_topic} -> {error, empty_topic}
    end.

send_registration(Type, Ref, Topic, St) ->
    {MsgId, St1} = take_msg_id(St),
    reply(St1, #{type => register, topic_id => topic_field(Type, Ref),
                 msg_id => MsgId, topic => Topic}).

%% Downlink PUBLISH. QoS 1/2 need a message id and a `pending' entry so the ack
%% can advance the cursor; QoS 0 carries neither.
send_publish(0, Msg, Ref, Retain, Type, _Pos, _Refs, St) ->
    reply(St, #{type => publish, dup => false, qos => 0, retain => Retain,
                topic_id_type => Type, topic_id => Ref, msg_id => 0,
                payload => Msg#broker_message.payload});
send_publish(QoS, Msg, Ref, Retain, Type, {Partition, Offset}, Refs, St) ->
    {MsgId, St1} = take_msg_id(St),
    Entry = #{refs => Refs, partition => Partition, offset => Offset,
              qos => QoS},
    St2 = St1#st{pending = maps:put(MsgId, Entry, St#st.pending)},
    reply(St2, #{type => publish, dup => false, qos => QoS, retain => Retain,
                 topic_id_type => Type, topic_id => Ref, msg_id => MsgId,
                 payload => Msg#broker_message.payload}).

ack_delivery(#{refs := Refs, partition := P, offset := O}, St) ->
    ack_delivery_pos(Refs, {P, O}, St).

ack_delivery_pos(_Refs, {undefined, _Offset}, St) ->
    %% A retained replay has no position: there is no cursor to advance.
    St;
ack_delivery_pos(Refs, {Partition, Offset}, St) ->
    lists:foreach(
      fun(Ref) ->
              _ = (catch broker_subscription:ack(Ref, Partition, Offset,
                                                 St#st.client_id))
      end, Refs),
    St.

%% Retained replay after SUBACK: one message per matching topic, at the lower of
%% the granted QoS and what was retained.
replay_retained(SubRef, Filter, GrantedQoS, St) ->
    case catch broker_retain:entries() of
        Entries when is_list(Entries) ->
            deliver_all(
              [{Msg, min(GrantedQoS,
                         max(maps:get(qos, Msg#broker_message.proto_meta, 0), 0))}
               || {_Topic, Msg} <- Entries,
                  mqtt_sn_filter:matches(Msg#broker_message.destination, Filter)],
              SubRef, St);
        _ ->
            %% The retain app is not running (a session in a unit test): no
            %% replay is better than a crashed connection.
            {ok, St}
    end.

deliver_all([], _SubRef, St) ->
    {ok, St};
deliver_all([{Msg, QoS} | Rest], SubRef, St) ->
    case deliver([SubRef], QoS, {undefined, undefined}, Msg, true, St) of
        {ok, St1} -> deliver_all(Rest, SubRef, St1);
        {stop, Reason, St1} -> {stop, Reason, St1}
    end.

%%%_* Catch-up --------------------------------------------------------------

%% Replay everything this client's cursors have not consumed yet. Used on
%% CONNECT with a persistent session, on waking from sleep, and on an ordinary
%% PINGREQ -- all three mean "this client exists but has not seen the tail".
%%
%% The log is contiguous and shared (ADR-011), so one partition's page contains
%% other clients' traffic. A row is delivered only for the subscriptions whose
%% cursor has not passed it *and* whose filter matches; every other row is acked
%% away, because a skipped row that nobody acks pins this client's cursor
%% forever.
%%
%% Bounded in pages: a session far behind does not monopolise its own process,
%% and the next PINGREQ resumes from the cursor. `max_inflight' bounds how much
%% of that backlog is on the wire at once; a full window simply leaves the rest
%% for the next pass, exactly like MQTT's inflight window.
catch_up(#st{client_id = undefined} = St) ->
    {ok, St};
catch_up(St) ->
    lists:foldl(
      fun(Partition, {ok, Acc}) -> catch_up_partition(Partition, Acc);
         (_Partition, Stop) -> Stop
      end, {ok, St}, broker_queue:partition_ids()).

catch_up_partition(Partition, St) ->
    case positions(Partition, St) of
        [] ->
            {ok, St};
        Positions ->
            From = lists:min([C || #{cursor := C} <- Positions]),
            catch_up_pages(Partition, From, Positions, St, 0)
    end.

positions(Partition, St) ->
    lists:foldl(
      fun({Ref, #{filter := Filter, qos := QoS}}, Acc) ->
              case broker_subscription:cursor(Ref, Partition) of
                  {ok, Cursor} ->
                      [#{ref => Ref, filter => Filter, qos => QoS,
                         cursor => Cursor} | Acc];
                  _ ->
                      Acc
              end
      end, [], maps:to_list(St#st.subs)).

catch_up_pages(_Partition, _From, _Positions, St, N) when N >= 8 ->
    {ok, St};
catch_up_pages(Partition, From, Positions, St, N) ->
    case catch broker_queue:read_from(Partition, From, 100) of
        {ok, [], _Next} ->
            {ok, St};
        {ok, Page, _Next} when is_list(Page) ->
            case catch_up_page(Partition, Page, Positions, St) of
                {ok, St1} ->
                    Last = element(1, lists:last(Page)),
                    catch_up_pages(Partition, Last + 1, Positions, St1, N + 1);
                {stop, _Reason, St1} ->
                    {ok, St1}
            end;
        _ ->
            {ok, St}
    end.

catch_up_page(_Partition, [], _Positions, St) ->
    {ok, St};
catch_up_page(Partition, [{Offset, Msg} | Rest], Positions, St) ->
    case maps:is_key({Partition, Offset}, pending_positions(St)) of
        true ->
            %% Already on the wire and waiting for its ack. Re-sending it here
            %% would be a second in-flight copy under a fresh message id, which
            %% is not a retransmission -- the pending entry's own ack is what
            %% releases this offset.
            catch_up_page(Partition, Rest, Positions, St);
        false ->
            catch_up_row(Partition, Offset, Msg, Rest, Positions, St)
    end.

pending_positions(#st{pending = Pending}) ->
    maps:from_list(
      [{ {maps:get(partition, E), maps:get(offset, E)}, true}
       || E <- maps:values(Pending),
          maps:get(partition, E, undefined) =/= undefined]).

catch_up_row(Partition, Offset, Msg, Rest, Positions, St) ->
    Topic = Msg#broker_message.destination,
    {Hits, Skips} =
        lists:foldl(
          fun(#{ref := Ref, filter := Filter, cursor := Cursor} = Pos, {H, S})
                when Cursor =< Offset ->
                  case mqtt_sn_filter:matches(Topic, Filter) of
                      true -> {[Pos | H], S};
                      false -> {H, [Ref | S]}
                  end;
             (_Pos, Acc) ->
                  Acc
          end, {[], []}, Positions),
    lists:foreach(fun(Ref) -> ack_skip(Ref, Partition, Offset, St) end, Skips),
    Refs = [Ref || #{ref := Ref} <- Hits],
    case Refs of
        [] ->
            catch_up_page(Partition, Rest, Positions, St);
        _ ->
            QoS = catch_up_qos(Hits, Msg),
            case deliver(Refs, QoS, {Partition, Offset}, Msg, false, St) of
                {stop, Reason, St1} ->
                    {stop, Reason, St1};
                {ok, St1} ->
                    %% `deliver/6' acks QoS 0 itself (there is no handshake to
                    %% wait for); QoS 1/2 keep the cursor pinned until their ack.
                    catch_up_page(Partition, Rest, Positions, St1)
            end
    end.

catch_up_qos(Hits, Msg) ->
    Granted = lists:max([QoS || #{qos := QoS} <- Hits]),
    Published = max(maps:get(qos, Msg#broker_message.proto_meta, 0), 0),
    min(Granted, Published).

ack_skip(Ref, Partition, Offset, St) ->
    _ = (catch broker_subscription:ack(Ref, Partition, Offset, St#st.client_id)),
    ok.

%%%_* Publish path (shared by PUBLISH and the Will) -------------------------

store_publish(Topic, Payload, QoS, Retain, St) ->
    Msg = new_message(St#st.tenant, Topic, Payload, QoS, Retain),
    publish_routed(Msg, QoS, Retain).

publish_routed(Msg0, QoS, Retain) ->
    try
        {ok, Env} = broker_router:env(),
        Ctx = #route_ctx{tenant_id = Msg0#broker_message.tenant_id,
                         publisher = maps:get(publisher,
                                              Msg0#broker_message.proto_meta,
                                              undefined),
                         reliability = reliability_of(QoS)},
        %% ADR-010: the hash always names at least one partition, so the head is
        %% the only branch there is (dialyzer proves the rest unreachable).
        {ok, [{Partition, Refs} | _]} =
            broker_router_hash:route(Msg0#broker_message.destination, Ctx, Env),
        Msg = Msg0#broker_message{partition_id = Partition},
        case broker_event:apply_policy(Msg) of
            {ok, Accepted} -> store(Accepted, QoS, Partition, Refs, Retain);
            {error, Reason} -> {error, {event_policy, Reason}}
        end
    catch
        Class:Err:Stack ->
            logger:warning("mqtt_sn publish path failed ~p:~p ~p",
                           [Class, Err, Stack]),
            {error, {crash, Err}}
    end.

store(Msg, QoS, _Partition, Refs, Retain) when QoS =:= 0; QoS =:= -1 ->
    %% QoS 0 / -1 never enter the log (MQTT §4.4's rule, and the reason QoS -1
    %% exists): subscribers get the live push and nothing is owed to a client
    %% that was not listening. Retained state is still updated, async, because
    %% there is no durability promise to wait for.
    _ = retain_update(Msg, Retain, async),
    _ = (catch broker_subscription:dispatch(Refs, {undefined, undefined, Msg})),
    ok;
store(Msg, _QoS, Partition, Refs, Retain) ->
    case catch broker_queue:append(Partition, Msg, fsync) of
        {ok, Offset} ->
            %% ADR-018 §18.2 order: append(fsync) -> retain(fsync) -> ACK ->
            %% dispatch. The retained state is part of the publish's durability
            %% promise, so a failed retain store must not be acked.
            case retain_update(Msg, Retain, sync) of
                ok ->
                    _ = (catch broker_subscription:dispatch(
                                 Refs, {Partition, Offset, Msg})),
                    ok;
                {error, Reason} ->
                    {error, {retain_store, Reason}}
            end;
        {error, Reason} ->
            {error, {append, Reason}};
        Other ->
            {error, {append_crashed, Other}}
    end.

retain_update(_Msg, false, _Mode) ->
    ok;
retain_update(#broker_message{payload = <<>>} = Msg, true, Mode) ->
    broker_retain:delete(Msg#broker_message.destination, Mode);
retain_update(Msg, true, Mode) ->
    case broker_retain:put(Msg, Mode) of
        ok -> ok;
        {error, overflow} ->
            %% Bounded refusal: live subscribers still get it and the publisher
            %% is still acked; only the retained copy is skipped.
            ok;
        {error, _} = Err -> Err
    end.

new_message(Tenant, Topic, Payload, QoS, Retain) ->
    #broker_message{message_id = crypto:strong_rand_bytes(16),
                    tenant_id = Tenant,
                    destination = Topic,
                    partition_id = 0,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{qos => QoS, dup => 0, retain => Retain,
                                   client => <<"mqtt-sn">>},
                    persistence = case QoS of
                                      0 -> transient;
                                      -1 -> transient;
                                      _ -> durable
                                  end,
                    reliability = reliability_of(QoS),
                    created_at = erlang:system_time(millisecond),
                    expires_at = undefined,
                    event = undefined}.

reliability_of(0) -> os_buffer;
reliability_of(-1) -> os_buffer;
reliability_of(_) -> broker_queue:reliability().

%%%_* Topic resolution ------------------------------------------------------

resolve_topic(normal, Id, St) when is_integer(Id) ->
    case mqtt_sn_topics:topic_of(St#st.topics, Id) of
        {ok, Topic} -> {ok, Topic};
        error -> {error, invalid_topic_id}
    end;
resolve_topic(predefined, Id, St) when is_integer(Id) ->
    case maps:find(Id, maps:get(predefined, St#st.cfg)) of
        {ok, Topic} -> {ok, Topic};
        error -> {error, invalid_topic_id}
    end;
resolve_topic(short, Name, _St) when is_binary(Name) ->
    {ok, Name};
resolve_topic(_Type, _Ref, _St) ->
    {error, invalid_topic_id}.

%% SUBSCRIBE/UNSUBSCRIBE name their topic with a variable field: a name for
%% normal and short, a numeric predefined id otherwise. A normal name is
%% registered here, because that is the only way the client can later PUBLISH to
%% the id the SUBACK hands back.
resolve_filter(normal, Topic, St) when is_binary(Topic) ->
    case assign_topic(Topic, St) of
        {ok, _Id, St1} -> {ok, Topic, St1};
        {error, Reason} -> {error, Reason}
    end;
resolve_filter(predefined, Id, St) when is_integer(Id) ->
    case maps:find(Id, maps:get(predefined, St#st.cfg)) of
        {ok, Topic} -> {ok, Topic, St};
        error -> {error, {unknown_predefined_topic, Id}}
    end;
resolve_filter(short, Name, _St) when is_binary(Name) ->
    %% §5.4.16: a short topic is exactly two characters, because those two
    %% bytes travel in the id field. Any other length has no encoding on the
    %% wire, so refuse it here rather than store a filter no client can name.
    case mqtt_sn_topics:short(Name) of
        {ok, Topic} -> {ok, Topic, _St};
        error -> {error, {invalid_short_topic, Name}}
    end;
resolve_filter(_Type, _Ref, _St) ->
    {error, invalid_topic_id}.

%%%_* Misc helpers ----------------------------------------------------------

valid_qos(QoS) when QoS >= 0, QoS =< 2 -> true;
valid_qos(_) -> false.

acl_publish(Topic, St) ->
    broker_security:authorize(
      St#st.principal,
      #{action => publish, tenant_id => St#st.tenant, destination => Topic}).

acl_subscribe(Filter, St) ->
    broker_security:authorize(
      St#st.principal,
      #{action => subscribe, tenant_id => St#st.tenant, destination => Filter}).

maybe_takeover(Pid) when is_pid(Pid), Pid =/= self() ->
    Pid ! {sn_takeover, self()},
    ok;
maybe_takeover(_) ->
    ok.

clean_session_teardown(ClientId) ->
    lists:foreach(
      fun(#{filter := Filter}) ->
              _ = broker_subscription:unsubscribe(ClientId, Filter)
      end, broker_subscription:subscriptions(ClientId)),
    %% The same "start from zero" also drops any persisted inflight snapshot.
    _ = (catch broker_subscription:inflight(ClientId, undefined)),
    ok.

session_subs(ClientId) ->
    lists:foldl(
      fun(#{sub_ref := Ref, filter := Filter, qos := QoS}, Acc) ->
              Acc#{Ref => #{filter => Filter, qos => QoS}}
      end, #{}, broker_subscription:subscriptions(ClientId)).

take_msg_id(#st{next_id = Id} = St) ->
    Next = case Id >= ?MAX_MSG_ID of
               true -> 1;
               false -> Id + 1
           end,
    {Id, St#st{next_id = Next}}.

%%%_* Wire ------------------------------------------------------------------

reply(St, Packet) ->
    Data = mqtt_sn_codec:encode(Packet),
    {IP, Port} = St#st.endpoint,
    _ = gen_udp:send(St#st.socket, IP, Port, Data),
    broker_metrics:inc(bytes_out, byte_size(Data)),
    St.
