%% @doc MQTT-SN topic-id rules (MQTT-SN 1.2 §5.4.16 and §6).
%%
%% Three kinds of topic id exist, and they are not interchangeable:
%%
%%   * **predefined** -- configured once for the gateway (`predefined_topics'),
%%     the same id means the same topic for every client. Read-only at runtime.
%%   * **normal** -- allocated per client, in both directions. The gateway sends
%%     REGISTER to hand out an id it allocated for a downlink topic, and accepts
%%     REGISTER from the client for an uplink one.
%%   * **short** -- a two-character topic name that IS the id. Nothing is
%%     allocated, so nothing can be exhausted, and the name travels in the
%%     PUBLISH/SUBSCRIBE field itself.
%%
%% This module owns only the rules; the per-client `normal' map lives in the
%% session process, because it dies with that client and must not be visible to
%% any other. That is also why there is no ETS table here: two clients must be
%% able to use id 1 for different topics.
%%
%% Id 0 and the two highest values are reserved by the spec, so a valid id is
%% `1..16#FFFD'.
-module(mqtt_sn_topics).

-export([predefined/0, validate_topic_id/1, short/1, assign/3, topic_of/2,
         max_id/0]).

-define(MAX_ID, 16#FFFD).

%% @doc The configured predefined table, validated. A bad configuration is a
%% startup error, not something to paper over: an id that means one topic in
%% `sys.config' and another on the wire is unfixable at runtime.
-spec predefined() -> #{1..16#FFFD => binary()}.
predefined() ->
    case application:get_env(broker_proto_mqtt_sn, predefined_topics, #{}) of
        Map when is_map(Map) ->
            maps:fold(fun validate_predefined/3, #{}, Map);
        Other ->
            error({invalid_predefined_topics, Other})
    end.

validate_predefined(Id, Topic, Acc) when is_integer(Id), is_binary(Topic) ->
    case validate_topic_id(Id) of
        ok when byte_size(Topic) > 0 -> maps:put(Id, Topic, Acc);
        ok -> error({invalid_predefined_topics, {empty_topic, Id}});
        {error, Reason} -> error({invalid_predefined_topics, {Id, Reason}})
    end;
validate_predefined(Key, Value, _Acc) ->
    error({invalid_predefined_topics, {Key, Value}}).

%% @doc Is this a legal topic id at all? Says nothing about whether the id is
%% currently assigned -- that is the caller's map.
-spec validate_topic_id(integer()) -> ok | {error, reserved_topic_id | invalid_topic_id}.
validate_topic_id(0) -> {error, reserved_topic_id};
validate_topic_id(16#FFFE) -> {error, reserved_topic_id};
validate_topic_id(16#FFFF) -> {error, reserved_topic_id};
validate_topic_id(Id) when is_integer(Id), Id > 0, Id =< ?MAX_ID -> ok;
validate_topic_id(_) -> {error, invalid_topic_id}.

%% @doc A short topic name is exactly two characters (MQTT-SN 1.2 §5.4.16): the
%% two bytes travel in the id field, so any other length has no encoding.
-spec short(binary()) -> {ok, binary()} | error.
short(<<A, B>> = Topic) when is_integer(A), is_integer(B) -> {ok, Topic};
short(_) -> error.

%% @doc Allocate (or reuse) a normal topic id for `Topic'.
%%
%% Reuse is what keeps ids stable for the life of a session: a client that
%% publishes to the same topic a thousand times must see one REGISTER, not a
%% thousand. The lowest free id is chosen so the map stays compact.
-spec assign(#{binary() => 1..16#FFFD}, binary(), pos_integer()) ->
          {ok, 1..16#FFFD, #{binary() => 1..16#FFFD}}
              | {error, exhausted | empty_topic}.
assign(_Topics, <<>>, _Max) ->
    %% An empty topic has no encoding in either direction, so handing out an id
    %% for it would only move the failure to the encoder.
    {error, empty_topic};
assign(Topics, Topic, Max) when is_binary(Topic) ->
    case maps:find(Topic, Topics) of
        {ok, Id} ->
            {ok, Id, Topics};
        error ->
            %% One pass over the values, then O(1) membership: allocating is
            %% not hot, but it happens up to `Max' times per session and the
            %% naive scan would be quadratic in the number of topics.
            Used = maps:from_list([{I, true} || I <- maps:values(Topics)]),
            case next_free(Used, 1, Max) of
                {ok, Id} -> {ok, Id, maps:put(Topic, Id, Topics)};
                exhausted -> {error, exhausted}
            end
    end.

next_free(_Used, Id, Max) when Id > Max ->
    exhausted;
next_free(Used, Id, Max) ->
    case maps:is_key(Id, Used) of
        true -> next_free(Used, Id + 1, Max);
        false -> {ok, Id}
    end.

%% @doc Reverse lookup: which topic does this id mean in this client's normal
%% map?
-spec topic_of(#{binary() => 1..16#FFFD}, integer()) -> {ok, binary()} | error.
topic_of(Topics, Id) ->
    case [T || {T, I} <- maps:to_list(Topics), I =:= Id] of
        [Topic | _] -> {ok, Topic};
        [] -> error
    end.

%% @doc The largest legal id; `8#FFFE' and `8#FFFF' are reserved.
-spec max_id() -> 16#FFFD.
max_id() -> ?MAX_ID.
