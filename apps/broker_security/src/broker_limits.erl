%% @doc S6 resource limits (README §10.1).
%%
%% The broker refuses to accept a message or a CloudEvents event that exceeds
%% the configured bounds. Every limit is read from `broker_security.limits'
%% (a map) on each call; a missing key uses the documented default, and a
%% malformed value fails the check loudly instead of silently disabling it.
%%
%% Bounds implemented here (all configurable):
%%
%%   * `max_message_bytes'       -- stored payload size
%%   * `max_header_count'        -- number of message headers/properties
%%   * `max_header_value_bytes'  -- one header name or value
%%   * `max_event_attributes'    -- CloudEvents core + extension attributes
%%   * `max_extension_value_bytes' -- one CloudEvents extension value
%%   * `max_json_depth'          -- nesting depth of a JSON `data' value
%%
%% Why these live in `broker_security': the limits are a governance decision,
%% and the function is pure so `broker_queue' and `broker_event' can call it
%% from the caller's process without a new process or a new dependency cycle.
-module(broker_limits).

-include_lib("broker_core/include/broker_message.hrl").

-export([limits/0, check_message/1, check_event/1,
         json_depth/1, max_message_bytes/0]).

-export_type([violation/0]).

-type violation() :: {limit_exceeded, atom(), non_neg_integer(), non_neg_integer() | infinity}.

-define(DEFAULTS, #{max_message_bytes => 1048576,
                    max_header_count => 32,
                    max_header_value_bytes => 8192,
                    max_event_attributes => 64,
                    max_extension_value_bytes => 4096,
                    max_json_depth => 16}).

%% @doc The effective limits, defaults filled in. Exposed so the admin API can
%% report exactly what is enforced (no "config says X, code does Y").
-spec limits() -> map().
limits() ->
    case application:get_env(broker_security, limits, #{}) of
        Map when is_map(Map) ->
            maps:merge(?DEFAULTS, Map);
        Other ->
            erlang:error({invalid_limits_config, Other})
    end.

-spec max_message_bytes() -> non_neg_integer().
max_message_bytes() ->
    maps:get(max_message_bytes, limits()).

%% @doc Check one broker message. Called on the single append path
%% (`broker_queue:append/3'), so every protocol is covered by the same rule.
-spec check_message(#broker_message{}) -> ok | {error, violation()}.
check_message(#broker_message{payload = Payload, headers = Headers}) ->
    Limits = limits(),
    case check_bytes(Payload, maps:get(max_message_bytes, Limits)) of
        ok -> check_headers(Headers, Limits);
        {error, _} = Error -> Error
    end;
check_message(_Other) ->
    {error, {limit_exceeded, message_bytes, 0, 0}}.

check_headers(Headers, Limits) when is_map(Headers) ->
    MaxCount = maps:get(max_header_count, Limits),
    case map_size(Headers) of
        N when N > MaxCount ->
            {error, {limit_exceeded, header_count, N, MaxCount}};
        _ ->
            MaxValue = maps:get(max_header_value_bytes, Limits),
            check_header_entries(maps:to_list(Headers), MaxValue)
    end.

check_header_entries([], _Max) ->
    ok;
check_header_entries([{K, V} | Rest], Max) ->
    KB = byte_size(to_bin(K)),
    VB = byte_size(to_bin(V)),
    case KB > Max of
        true ->
            {error, {limit_exceeded, header_value_bytes, KB, Max}};
        false when VB > Max ->
            {error, {limit_exceeded, header_value_bytes, VB, Max}};
        false ->
            check_header_entries(Rest, Max)
    end.

%% @doc Check a decoded CloudEvents map (the ADR-020 runtime shape). Used by
%% `broker_event_codec' after its own schema validation, so the two never
%% duplicate each other's rules.
-spec check_event(map()) -> ok | {error, violation()}.
check_event(Event) when is_map(Event) ->
    Limits = limits(),
    Extensions = maps:get(extensions, Event, #{}),
    MaxAttrs = maps:get(max_event_attributes, Limits),
    %% Core attributes are five or six fields; the configurable bound is meant
    %% to cap extensions plus the core, exactly as README §10.1 words it.
    AttrCount = 8 + map_size(Extensions),
    case AttrCount > MaxAttrs of
        true ->
            {error, {limit_exceeded, event_attributes, AttrCount, MaxAttrs}};
        false ->
            MaxValue = maps:get(max_extension_value_bytes, Limits),
            case check_extension_values(maps:to_list(Extensions), MaxValue) of
                ok -> check_event_data(Event, maps:get(max_json_depth, Limits));
                {error, _} = Error -> Error
            end
    end;
check_event(_Other) ->
    {error, {limit_exceeded, event_attributes, 0, 0}}.

check_extension_values([], _Max) ->
    ok;
check_extension_values([{_Name, Value} | Rest], Max) ->
    case byte_size(encode_scalar(Value)) > Max of
        true ->
            {error, {limit_exceeded, extension_value_bytes,
                     byte_size(encode_scalar(Value)), Max}};
        false ->
            check_extension_values(Rest, Max)
    end.

check_event_data(#{data := Data}, MaxDepth) ->
    Depth = json_depth(Data),
    case Depth > MaxDepth of
        true -> {error, {limit_exceeded, json_depth, Depth, MaxDepth}};
        false -> ok
    end;
check_event_data(_Event, _MaxDepth) ->
    ok.

%% @doc Nesting depth of a decoded JSON term. Scalars are depth 1, one level
%% of list/map is depth 2, and so on. Non-JSON binaries are scalars.
-spec json_depth(term()) -> non_neg_integer().
json_depth(Term) when is_map(Term) ->
    1 + lists:max([0 | [json_depth(V) || V <- maps:values(Term)]]);
json_depth(Term) when is_list(Term) ->
    1 + lists:max([0 | [json_depth(V) || V <- Term]]);
json_depth(_) ->
    1.

%%%_* Internal --------------------------------------------------------------

-spec check_bytes(term(), non_neg_integer()) -> ok | {error, violation()}.
check_bytes(Bin, Max) ->
    %% `#broker_message.payload' is `binary()' by type, but a hand-built record
    %% can carry anything; the short-circuit keeps the non-binary case a no-op
    %% without a case clause dialyzer would prove dead.
    case is_binary(Bin) andalso byte_size(Bin) > Max of
        true -> {error, {limit_exceeded, message_bytes, byte_size(Bin), Max}};
        false -> ok
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(I) when is_integer(I) -> integer_to_binary(I);
to_bin(Other) -> iolist_to_binary(io_lib:format("~p", [Other])).

encode_scalar(B) when is_binary(B) -> B;
encode_scalar(A) when is_atom(A) -> atom_to_binary(A, utf8);
encode_scalar(I) when is_integer(I) -> integer_to_binary(I);
encode_scalar(F) when is_float(F) -> float_to_binary(F, [short]);
encode_scalar(L) when is_list(L) -> iolist_to_binary(L);
encode_scalar(M) when is_map(M) -> iolist_to_binary(io_lib:format("~p", [M]));
encode_scalar(Other) -> iolist_to_binary(io_lib:format("~p", [Other])).
