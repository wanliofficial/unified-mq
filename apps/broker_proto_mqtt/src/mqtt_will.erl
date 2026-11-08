%% @doc Publish a connection's Will (MQTT 3.1.1 §3.1.2.5; ADR-018 §18.1).
%%
%% A Will is published by a connection that is dying, so this module has one
%% hard rule: **it never raises**. Every step (route, append, retain, dispatch)
%% is wrapped, and a failure is logged rather than propagated into
%% `terminate/3', where it would replace the real termination reason with a
%% crash report about the cleanup path.
%%
%% No ack exists for a Will: the publisher is gone. `reliability' still follows
%% the normal mapping (QoS 0 -> `os_buffer', QoS 1/2 -> `fsync'), so a QoS 1
%% Will is at least as durable as a QoS 1 publish by a live client, but nobody
%% is told whether the append succeeded -- a failed Will is lost, which is the
%% same posture mosquitto takes.
-module(mqtt_will).

-export([publish/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-type will() :: #{topic := binary(), payload := binary(),
                  qos := 0 | 1 | 2, retain := 0 | 1}.

%% @doc Publish `Will' on behalf of `Principal' in `Tenant'. Returns `ok' when
%% the message reached the log (or, for QoS 0, the dispatcher); any failure is
%% logged and reported but never raised.
-spec publish(will(), binary(), principal() | undefined) -> ok | {error, term()}.
publish(#{topic := Topic, payload := Payload, qos := QoS, retain := Retain},
        Tenant, Principal) ->
    try
        Ctx = #route_ctx{tenant_id = Tenant, publisher = Principal,
                         reliability = reliability_of(QoS)},
        {ok, Env} = broker_router:env(),
        {ok, [{Partition, Refs} | _]} = broker_router_hash:route(Topic, Ctx, Env),
        Msg = build(Topic, Payload, QoS, Retain, Partition, Tenant),
        case QoS of
            0 ->
                %% §4.4: QoS 0 never enters the log, so there is no offset to
                %% dispatch and no cursor to leave behind.
                _ = (catch broker_retain_put(Msg, Retain, async)),
                _ = (catch broker_subscription:dispatch(
                               Refs, {undefined, undefined, Msg})),
                broker_metrics:inc(will_published),
                ok;
            _ ->
                case (catch broker_queue:append(Partition, Msg, fsync)) of
                    {ok, Offset} ->
                        _ = (catch broker_retain_put(Msg, Retain, sync)),
                        _ = (catch broker_subscription:dispatch(
                                       Refs, {Partition, Offset, Msg})),
                        broker_metrics:inc(will_published),
                        ok;
                    Other ->
                        log(warning, "will append failed ~p", [Other]),
                        {error, Other}
                end
        end
    catch
        Class:Reason:Stack ->
            log(warning, "will publish failed ~p:~p ~p", [Class, Reason, Stack]),
            {error, {Class, Reason}}
    end.

%%%_* Internal ----------------------------------------------------------------

%% §3.3.1.3: an empty payload on a retained publish clears the topic instead of
%% storing it. A Will with an empty payload and `retain=1' is a legal way to say
%% "my retained state is gone", so it must take the delete branch.
broker_retain_put(_Msg, 0, _Mode) ->
    ok;
broker_retain_put(#broker_message{payload = <<>>} = Msg, 1, Mode) ->
    broker_retain:delete(Msg#broker_message.destination, Mode);
broker_retain_put(Msg, 1, Mode) ->
    %% `{error, overflow}' is a bounded refusal, not a failure: the Will is still
    %% delivered to live subscribers, it just does not become retained state.
    case broker_retain:put(Msg, Mode) of
        {error, overflow} -> ok;
        Other -> Other
    end.

build(Topic, Payload, QoS, Retain, Partition, Tenant) ->
    Created = erlang:system_time(millisecond),
    #broker_message{
      message_id = crypto:strong_rand_bytes(16),
      tenant_id = Tenant,
      destination = Topic,
      partition_id = Partition,
      payload = Payload,
      payload_encoding = raw,
      headers = #{},
      proto_meta = #{qos => QoS, dup => 0, retain => Retain, will => true},
      persistence = case QoS of 0 -> transient; _ -> durable end,
      reliability = reliability_of(QoS),
      created_at = Created,
      expires_at = expiry(Created),
      event = undefined}.

%% Same deadline rule as a live publish (`mqtt_conn_fsm:build_message/7'): a
%% Will is an ordinary message once it is published, so it carries the same TTL
%% (S2-E E-5).
expiry(Created) ->
    case application:get_env(broker_proto_mqtt, message_ttl_ms, 0) of
        TTL when is_integer(TTL), TTL > 0 -> Created + TTL;
        _ -> undefined
    end.

reliability_of(0) -> os_buffer;
reliability_of(_) -> fsync.

log(Level, Fmt, Args) ->
    logger:log(Level, "mqtt_will: " ++ Fmt, Args).
