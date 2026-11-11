%% @doc `A2A-Version' negotiation (spec §3.2.6/§9.2).
-module(agent_rpc_version_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [missing_header_is_0_3,
     empty_header_is_0_3,
     parses_major_minor,
     trims_whitespace,
     rejects_major_only,
     rejects_three_parts,
     rejects_non_numeric,
     rejects_negative,
     negotiate_accepts_supported,
     negotiate_rejects_unsupported,
     negotiate_skips_unparsable_entries,
     to_binary_roundtrip].

missing_header_is_0_3(_Config) ->
    ?assertEqual({ok, {0, 3}}, agent_rpc_version:parse(undefined)).

empty_header_is_0_3(_Config) ->
    ?assertEqual({ok, {0, 3}}, agent_rpc_version:parse(<<>>)).

parses_major_minor(_Config) ->
    ?assertEqual({ok, {1, 0}}, agent_rpc_version:parse(<<"1.0">>)),
    ?assertEqual({ok, {0, 3}}, agent_rpc_version:parse(<<"0.3">>)),
    ?assertEqual({ok, {12, 34}}, agent_rpc_version:parse(<<"12.34">>)).

trims_whitespace(_Config) ->
    ?assertEqual({ok, {1, 0}}, agent_rpc_version:parse(<<" 1.0 ">>)),
    ?assertEqual({ok, {1, 0}}, agent_rpc_version:parse(<<"\t1.0\t">>)).

rejects_major_only(_Config) ->
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:parse(<<"1">>)).

rejects_three_parts(_Config) ->
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:parse(<<"1.0.0">>)).

rejects_non_numeric(_Config) ->
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:parse(<<"one.zero">>)),
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:parse(<<"1.x">>)),
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:parse(<<"1.">>)),
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:parse(<<".0">>)).

rejects_negative(_Config) ->
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:parse(<<"-1.0">>)).

negotiate_accepts_supported(_Config) ->
    ?assertEqual(ok, agent_rpc_version:negotiate({1, 0}, [<<"1.0">>])),
    ?assertEqual(ok, agent_rpc_version:negotiate({0, 3}, [<<"1.0">>, <<"0.3">>])).

negotiate_rejects_unsupported(_Config) ->
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:negotiate({2, 0}, [<<"1.0">>])),
    ?assertEqual({error, version_not_supported},
                 agent_rpc_version:negotiate({1, 0}, [])).

negotiate_skips_unparsable_entries(_Config) ->
    %% A typo in sys.config must not crash the request path; the entry is simply
    %% not offered.
    ?assertEqual(ok, agent_rpc_version:negotiate({1, 0}, [<<"1">>, <<"1.0">>])).

to_binary_roundtrip(_Config) ->
    ?assertEqual(<<"1.0">>, agent_rpc_version:to_binary({1, 0})),
    ?assertEqual(<<"0.3">>, agent_rpc_version:to_binary({0, 3})).
