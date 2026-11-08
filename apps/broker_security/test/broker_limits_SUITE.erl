%% @doc S6 (README §10.1): resource limits.
%%
%% The limit checks are pure and read `broker_security.limits' per call, so the
%% suite can flip one bound at a time and assert the exact
%% `{limit_exceeded, Name, Actual, Bound}' answer. The default values are also
%% pinned: a silent default drift is the kind of thing that only shows up under
%% a real oversized message.
-module(broker_limits_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [defaults_are_the_documented_bounds,
     message_payload_over_the_limit_is_refused,
     message_at_the_limit_is_accepted,
     header_count_over_the_limit_is_refused,
     header_value_over_the_limit_is_refused,
     event_attribute_count_over_the_limit_is_refused,
     extension_value_over_the_limit_is_refused,
     json_depth_over_the_limit_is_refused,
     json_depth_counts_nesting,
     malformed_limits_config_fails_loudly,
     non_binary_payload_is_ignored].

init_per_suite(Config) ->
    Saved = application:get_env(broker_security, limits, undefined),
    [{saved_limits, Saved} | Config].

end_per_suite(Config) ->
    case proplists:get_value(saved_limits, Config) of
        undefined -> ok;
        Saved -> application:set_env(broker_security, limits, Saved)
    end,
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_security, limits, #{}),
    Config.

%%%_* Defaults -------------------------------------------------------------

defaults_are_the_documented_bounds(_Config) ->
    Limits = broker_limits:limits(),
    1048576 = maps:get(max_message_bytes, Limits),
    32 = maps:get(max_header_count, Limits),
    8192 = maps:get(max_header_value_bytes, Limits),
    64 = maps:get(max_event_attributes, Limits),
    4096 = maps:get(max_extension_value_bytes, Limits),
    16 = maps:get(max_json_depth, Limits),
    1048576 = broker_limits:max_message_bytes(),
    ok.

%%%_* Messages ------------------------------------------------------------

message_payload_over_the_limit_is_refused(_Config) ->
    set_limits(#{max_message_bytes => 8}),
    Msg = msg(<<"123456789">>),
    {error, {limit_exceeded, message_bytes, 9, 8}} =
        broker_limits:check_message(Msg),
    ok.

message_at_the_limit_is_accepted(_Config) ->
    set_limits(#{max_message_bytes => 9}),
    ok = broker_limits:check_message(msg(<<"123456789">>)),
    ok.

header_count_over_the_limit_is_refused(_Config) ->
    set_limits(#{max_header_count => 2}),
    Msg = (msg(<<"x">>))#broker_message{
            headers = #{<<"a">> => <<"1">>, <<"b">> => <<"2">>,
                        <<"c">> => <<"3">>}},
    {error, {limit_exceeded, header_count, 3, 2}} =
        broker_limits:check_message(Msg),
    ok.

header_value_over_the_limit_is_refused(_Config) ->
    set_limits(#{max_header_value_bytes => 4}),
    Msg = (msg(<<"x">>))#broker_message{headers = #{<<"k">> => <<"toolong">>}},
    {error, {limit_exceeded, header_value_bytes, 7, 4}} =
        broker_limits:check_message(Msg),
    ok.

%%%_* CloudEvents ---------------------------------------------------------

event_attribute_count_over_the_limit_is_refused(_Config) ->
    set_limits(#{max_event_attributes => 8}),
    Event = event(#{ext => <<"v">>}),
    {error, {limit_exceeded, event_attributes, 9, 8}} =
        broker_limits:check_event(Event),
    ok.

extension_value_over_the_limit_is_refused(_Config) ->
    set_limits(#{max_extension_value_bytes => 3}),
    Event = event(#{ext => <<"four">>}),
    {error, {limit_exceeded, extension_value_bytes, 4, 3}} =
        broker_limits:check_event(Event),
    ok.

json_depth_over_the_limit_is_refused(_Config) ->
    set_limits(#{max_json_depth => 2}),
    %% depth 1 = scalar; {a: [1,2]} is depth 3.
    Event = maps:put(data, #{<<"a">> => [1, 2]}, event(#{})),
    {error, {limit_exceeded, json_depth, 3, 2}} =
        broker_limits:check_event(Event),
    ok.

json_depth_counts_nesting(_Config) ->
    1 = broker_limits:json_depth(42),
    1 = broker_limits:json_depth(<<"scalar">>),
    2 = broker_limits:json_depth([1, 2, 3]),
    2 = broker_limits:json_depth(#{<<"a">> => 1}),
    3 = broker_limits:json_depth(#{<<"a">> => [1]}),
    ok.

%%%_* Config --------------------------------------------------------------

malformed_limits_config_fails_loudly(_Config) ->
    application:set_env(broker_security, limits, not_a_map),
    {'EXIT', {{invalid_limits_config, not_a_map}, _Stack}} =
        (catch broker_limits:limits()),
    ok.

non_binary_payload_is_ignored(_Config) ->
    %% A message built by hand (not by a protocol adapter) may carry a non-binary
    %% payload; the size check is a no-op rather than a crash.
    ok = broker_limits:check_message(msg(undefined)),
    ok.

%%%_* Helpers -------------------------------------------------------------

set_limits(Map) ->
    application:set_env(broker_security, limits, Map).

msg(Payload) ->
    #broker_message{destination = <<"t/x">>, payload = Payload,
                    headers = #{}, tenant_id = <<"t">>}.

event(Exts) ->
    #{specversion => <<"1.0">>, id => <<"id">>, source => <<"s">>,
      type => <<"t">>, extensions => Exts}.
