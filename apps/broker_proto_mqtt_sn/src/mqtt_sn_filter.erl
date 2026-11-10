%% @doc MQTT topic filters, for the MQTT-SN gateway only.
%%
%% Same rules as `mqtt_filter' (MQTT 3.1.1 §4.7) and, like `mqtt_filter' itself,
%% deliberately re-implemented on this side of the app boundary rather than
%% reached across it (design §13 L9): `broker_proto_mqtt_sn' must not depend on
%% `broker_proto_mqtt', and a wildcard matcher is 20 lines. Only the two
%% functions the gateway needs are here -- `validate_filter/1' for SUBSCRIBE and
%% `matches/2' for replaying retained messages at SUBACK time.
-module(mqtt_sn_filter).

-export([validate_filter/1, matches/2, validate_topic/1]).

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

%% @doc A topic *name* must contain no wildcards; MQTT-SN publishes a name only
%% in REGISTER, and the gateway turns that into a filter-less destination.
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

-spec matches(binary(), binary()) -> boolean().
matches(Topic, Filter) ->
    match(levels(Topic), levels(Filter)).

%%%_* Internal --------------------------------------------------------------

levels(Bin) ->
    binary:split(Bin, <<"/">>, [global]).

match([], []) ->
    true;
match([_ | _], [<<"#">>]) ->
    %% `#' matches the parent level too: `sport/#' matches `sport'.
    true;
match([H | T], [H | FT]) ->
    match(T, FT);
match([_ | T], [<<"+">> | FT]) ->
    match(T, FT);
match(_, _) ->
    false.

valid_levels([_ | _] = Levels) ->
    valid_levels(Levels, true).

valid_levels([], _Last) ->
    ok;
valid_levels([<<"#">>], true) ->
    ok;
valid_levels([<<"#">> | _], _Last) ->
    {error, multi_level_wildcard_not_last};
valid_levels([<<"+">> | Rest], Last) ->
    valid_levels(Rest, Last);
valid_levels([Level | Rest], Last) ->
    case has_wildcard(Level) of
        true -> {error, {wildcard_in_level, Level}};
        false -> valid_levels(Rest, Last)
    end.

has_wildcard(Bin) ->
    binary:match(Bin, [<<"+">>, <<"#">>]) =/= nomatch.
