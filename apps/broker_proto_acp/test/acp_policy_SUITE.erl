%% @doc S9-E: the permission policy and the session state machine (both pure).
%%
%% Grouping:
%%   * `policy'  -- default deny, allowlist matching on both axes, wildcard,
%%                  timeout refusal, audit body shape, option selection;
%%   * `session' -- the lifecycle gates (`initialize' first, one turn at a time,
%%                  cancel returns to ready, gone is absorbing).
-module(acp_policy_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

-define(POLICY0, #{default => deny,
                  allow => [#{tool => <<"edit">>, title => any},
                            #{tool => any, title => <<"Run tests">>}],
                  timeout_ms => 1000,
                  notify_dest => <<"$acp/permission">>}).

policy() -> ?POLICY0.

all() ->
    [{group, policy}, {group, session}].

groups() ->
    [{policy, [sequence],
      [empty_allowlist_denies,
       matching_kind_grants,
       matching_title_grants,
       neither_axis_denies,
       malformed_entry_is_skipped,
       timeout_refuses,
       grant_picks_allow_option,
       audit_body_shape,
       wire_result_is_nested,
       default_policy_is_deny]},
     {session, [sequence],
      [initialize_moves_to_ready,
       initialize_twice_is_refused,
       prompt_needs_a_session_id,
       prompt_moves_to_prompting,
       second_concurrent_turn_is_refused,
       end_turn_returns_to_ready,
       cancel_returns_to_ready,
       failed_is_gone,
       gone_refuses_everything]}].

%%%_* policy -----------------------------------------------------------------

params(Kind, Title) ->
    #{<<"toolCall">> => #{<<"kind">> => Kind, <<"title">> => Title},
      <<"options">> => [#{<<"optionId">> => <<"no">>, <<"kind">> => <<"reject_once">>},
                        #{<<"optionId">> => <<"yes">>, <<"kind">> => <<"allow_once">>}]}.

empty_allowlist_denies(_Config) ->
    D = acp_policy:decide(params(<<"edit">>, <<"t">>),
                          (policy())#{allow => []}),
    ?assertEqual(<<"cancelled">>, maps:get(outcome, D)).

matching_kind_grants(_Config) ->
    D = acp_policy:decide(params(<<"edit">>, <<"anything">>), (policy())),
    ?assertEqual(<<"selected">>, maps:get(outcome, D)),
    ?assertEqual(<<"yes">>, maps:get(optionId, D)).

matching_title_grants(_Config) ->
    D = acp_policy:decide(params(<<"execute">>, <<"Run tests">>), (policy())),
    ?assertEqual(<<"selected">>, maps:get(outcome, D)).

neither_axis_denies(_Config) ->
    D = acp_policy:decide(params(<<"delete">>, <<"rm -rf /">>), (policy())),
    ?assertEqual(<<"cancelled">>, maps:get(outcome, D)),
    ?assertEqual(<<"not in allowlist">>, maps:get(reason, D)).

malformed_entry_is_skipped(_Config) ->
    Policy = (policy())#{allow => [not_a_map | maps:get(allow, (policy()))]},
    ?assertEqual(<<"selected">>,
                 maps:get(outcome, acp_policy:decide(params(<<"edit">>, <<"x">>),
                                                     Policy))).

timeout_refuses(_Config) ->
    D = acp_policy:decide(params(<<"edit">>, <<"x">>), (policy()), 1001),
    ?assertEqual(<<"cancelled">>, maps:get(outcome, D)),
    ?assertEqual(<<"timeout">>, maps:get(reason, D)),
    OnTime = acp_policy:decide(params(<<"edit">>, <<"x">>), (policy()), 1000),
    ?assertEqual(<<"selected">>, maps:get(outcome, OnTime)).

grant_picks_allow_option(_Config) ->
    %% `allow_once' is preferred over the first listed option (`reject_once').
    D = acp_policy:decide(params(<<"edit">>, <<"x">>), (policy())),
    ?assertEqual(<<"yes">>, maps:get(optionId, D)),
    %% No allow option at all: fall back to the first, never invent an id.
    NoAllow = (params(<<"edit">>, <<"x">>))#{<<"options">> =>
                                                 [#{<<"optionId">> => <<"only">>,
                                                    <<"kind">> => <<"reject_once">>}]},
    D2 = acp_policy:decide(NoAllow, (policy())),
    ?assertEqual(<<"only">>, maps:get(optionId, D2)).

audit_body_shape(_Config) ->
    D = acp_policy:decide(params(<<"edit">>, <<"x">>), (policy())),
    Body = acp_policy:audit_body(<<"agent-a">>, <<"s1">>,
                                 params(<<"edit">>, <<"x">>), D),
    ?assertEqual(<<"agent-a">>, maps:get(<<"agent">>, Body)),
    ?assertEqual(<<"s1">>, maps:get(<<"sessionId">>, Body)),
    ?assertEqual(<<"selected">>, maps:get(<<"decision">>, Body)),
    ?assertEqual(<<"edit">>,
                 maps:get(<<"kind">>, maps:get(<<"toolCall">>, Body))).

%% ACP v1 `RequestPermissionResponse.result' is a nested outcome object. A
%% conforming agent (the official SDK) validates it as a discriminated union, so
%% a flat `{"outcome": "selected"}' is rejected outright -- this is the defect the
%% real SDK client surfaced in S9-F.
wire_result_is_nested(_Config) ->
    Granted = acp_policy:wire(acp_policy:decide(params(<<"edit">>, <<"x">>),
                                               (policy()))),
    ?assertEqual(<<"selected">>,
                 maps:get(<<"outcome">>, maps:get(<<"outcome">>, Granted))),
    ?assertEqual(<<"yes">>, maps:get(<<"optionId">>,
                                     maps:get(<<"outcome">>, Granted))),
    %% `_meta' and `reason' ride along; a peer ignores the extra members.
    ?assertEqual(<<"matched allowlist">>, maps:get(<<"reason">>, Granted)),
    ?assertMatch(#{<<"unified-mq">> := #{<<"decision">> := <<"allow">>}},
                 maps:get(<<"_meta">>, Granted)),
    %% A denial carries no `optionId' at all (the `DeniedOutcome' has none).
    Denied = acp_policy:wire(acp_policy:decide(params(<<"delete">>, <<"x">>),
                                               (policy()))),
    DeniedOutcome = maps:get(<<"outcome">>, Denied),
    ?assertEqual(<<"cancelled">>, maps:get(<<"outcome">>, DeniedOutcome)),
    ?assertEqual(false, maps:is_key(<<"optionId">>, DeniedOutcome)).

default_policy_is_deny(_Config) ->
    Default = acp_policy:default(),
    ?assertEqual(deny, maps:get(default, Default)),
    ?assertEqual([], maps:get(allow, Default)),
    %% Even with `default => allow' configured, the frozen posture refuses.
    ?assertEqual(<<"cancelled">>,
                 maps:get(outcome,
                          acp_policy:decide(params(<<"edit">>, <<"x">>),
                                            Default#{default => allow}))).

%%%_* session ----------------------------------------------------------------

new() ->
    acp_session:new(#{agent_name => <<"agent-a">>}).

initialize_ok(S) ->
    {ok, S1} = acp_session:initialize(S, #{<<"protocolVersion">> => 1}),
    S1.

with_session() ->
    acp_session:set_session_id(initialize_ok(new()), <<"s1">>).

initialize_moves_to_ready(_Config) ->
    S = new(),
    ?assertEqual(initializing, acp_session:state(S)),
    S1 = initialize_ok(S),
    ?assertEqual(ready, acp_session:state(S1)),
    ?assertEqual(1, acp_session:version(S1)).

initialize_twice_is_refused(_Config) ->
    S = initialize_ok(new()),
    ?assertMatch({error, {already_initialized, ready}},
                 acp_session:initialize(S, #{<<"protocolVersion">> => 1})),
    %% A non-integer version never reaches `ready'.
    ?assertMatch({error, unsupported_protocol_version},
                 acp_session:initialize(new(), #{<<"protocolVersion">> => <<"1">>})).

prompt_needs_a_session_id(_Config) ->
    Ready = initialize_ok(new()),
    ?assertNot(acp_session:can(Ready, <<"session/prompt">>)),
    ?assertEqual({error, {not_promptable, no_session_id}},
                 acp_session:prompt(Ready, <<"s1">>)),
    WithId = acp_session:set_session_id(Ready, <<"s1">>),
    ?assert(acp_session:can(WithId, <<"session/prompt">>)),
    %% A turn must run against the negotiated session, not a caller-invented id.
    ?assertMatch({error, {session_mismatch, _, _}},
                 acp_session:prompt(WithId, <<"other">>)).

prompt_moves_to_prompting(_Config) ->
    {ok, S} = acp_session:prompt(with_session(), <<"s1">>),
    ?assertEqual(prompting, acp_session:state(S)),
    ?assert(acp_session:can(S, <<"session/cancel">>)),
    ?assertNot(acp_session:can(S, <<"session/prompt">>)).

second_concurrent_turn_is_refused(_Config) ->
    {ok, S} = acp_session:prompt(with_session(), <<"s1">>),
    ?assertEqual({error, turn_already_active},
                 acp_session:prompt(S, <<"s1">>)),
    ?assertMatch({error, {session_busy, <<"s1">>}},
                 acp_session:prompt(S, <<"s2">>)).

end_turn_returns_to_ready(_Config) ->
    {ok, S} = acp_session:prompt(with_session(), <<"s1">>),
    Done = acp_session:end_turn(S, <<"end_turn">>),
    ?assertEqual(ready, acp_session:state(Done)),
    ?assert(acp_session:can(Done, <<"session/prompt">>)).

cancel_returns_to_ready(_Config) ->
    {ok, S} = acp_session:prompt(with_session(), <<"s1">>),
    Cancelled = acp_session:cancel(S),
    ?assertEqual(ready, acp_session:state(Cancelled)),
    %% Cancelling an idle session is a no-op, not an error.
    ?assertEqual(ready, acp_session:state(acp_session:cancel(Cancelled))).

failed_is_gone(_Config) ->
    Gone = acp_session:failed(with_session(), <<"refusal">>, <<"permission">>),
    ?assertEqual(gone, acp_session:state(Gone)).

gone_refuses_everything(_Config) ->
    Gone = acp_session:close(with_session()),
    ?assertEqual(gone, acp_session:state(Gone)),
    ?assertNot(acp_session:can(Gone, <<"session/prompt">>)),
    ?assertNot(acp_session:can(Gone, <<"initialize">>)),
    ?assertEqual(gone, acp_session:state(acp_session:close(Gone))).
