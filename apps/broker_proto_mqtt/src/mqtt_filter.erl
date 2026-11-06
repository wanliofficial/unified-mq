%% @doc MQTT 3.1.1 §4.7 topic filters: validation and matching, protocol side.
%%
%% The Router also matches filters (broker_router_hash:match/2), but its
%% matching is over its own index; the conn_fsm needs to decide, for one stored
%% message, which of *this client's* filters it hits (the pull path, design
%% §5.2) — a local call that must not go through the Router at all. Same
%% semantics as the router's, deliberately re-implemented here rather than
%% reached across an app boundary (design §13 L9; OQ-4's "Router 暂定" is about
%% the *index* direction).
-module(mqtt_filter).

-export([matches/2, validate_filter/1, validate_topic/1]).

%% MQTT §4.8 delivery level must never exceed what the subscription asked for;
%% matching here is pure §4.7 semantics.
-spec matches(binary(), binary()) -> boolean().
matches(Topic, Filter) ->
    match(levels(Topic), levels(Filter)).

%% @doc Annex A / §4.7: `#' only as a complete final level, `+' only as a
%% complete level, non-empty, within the wire length limit of a UTF-8 string.
-spec validate_filter(binary()) -> ok | {error, term()}.
validate_filter(<<>>) ->
    {error, empty_filter};
validate_filter(Filter) when byte_size(Filter) > 65535 ->
    {error, filter_too_long};
validate_filter(Filter) ->
    case valid_levels(levels(Filter)) of
        ok -> ok;
        {error, Reason} -> {error, {Reason, Filter}}
    end.

%% @doc A topic name (as opposed to a filter) must contain no wildcards.
-spec validate_topic(binary()) -> ok | {error, term()}.
validate_topic(<<>>) ->
    {error, empty_topic};
validate_topic(Topic) when byte_size(Topic) > 65535 ->
    {error, topic_too_long};
validate_topic(Topic) ->
    case has_wildcard(Topic) of
        true -> {error, {wildcard_in_topic, Topic}};
        false -> ok
    end.

%%%_* Internal -------------------------------------------------------------

levels(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

%% Identical rules to broker_router_hash:match/2 (kept in sync deliberately:
%% §4.7 frozen in ADR-010 §10.3).
match(_TopicLevels, [<<"#">>]) ->
    true;
match([], []) ->
    true;
match([_ | Topic], [<<"+">> | Filter]) ->
    match(Topic, Filter);
match([Level | Topic], [Level | Filter]) ->
    match(Topic, Filter);
match(_Topic, _Filter) ->
    false.

valid_levels([]) ->
    ok;
valid_levels([<<"#">>]) ->
    ok;
valid_levels([<<"#">> | _]) ->
    {error, wildcard_not_final_level};
valid_levels([<<"+">> | Rest]) ->
    valid_levels(Rest);
valid_levels([Level | Rest]) ->
    case has_wildcard(Level) of
        true -> {error, wildcard_inside_level};
        false -> valid_levels(Rest)
    end.

has_wildcard(Level) ->
    binary:match(Level, [<<"+">>, <<"#">>]) =/= nomatch.
