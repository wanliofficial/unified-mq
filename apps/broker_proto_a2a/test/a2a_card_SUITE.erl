%% @doc Unit tests for `a2a_card`'s pure logic: path routing, card validation,
%% and ETag generation/matching (ADR-026 §D3, RFC 8615/7232).
%%
%% `a2a_listener_SUITE' already drives the well-known and catalog surfaces
%% end-to-end through a live socket and a seeded `broker_meta'. This suite pins
%% the pieces that need neither: `route/1' classifies a GET path (and keeps the
%% query string for the JSON-RPC router); `validate/1' enforces the required
%% Agent Card fields and shapes (R-D1); `etag/2' builds a stable
%% `"<version>-<content hash>"'; and `matches_etag/2' implements the
%% If-None-Match comparison real caches send (undefined, `*', exact, weak `W/',
%% and comma-separated lists).
-module(a2a_card_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(WELL_KNOWN, <<"/.well-known/agent-card.json">>).

all() ->
    [route_classifies_well_known,
     route_classifies_catalog,
     route_unknown_path_is_error,
     route_keeps_query_string_for_well_known,
     route_keeps_query_string_for_catalog,
     validate_requires_all_fields,
     validate_reports_every_missing_field,
     validate_rejects_non_object,
     validate_rejects_empty_required_lists,
     validate_accepts_a_well_formed_card,
     etag_is_version_and_content_hash,
     etag_changes_with_content,
     etag_changes_with_version,
     etag_is_quoted,
     matches_etag_undefined_never_matches,
     matches_etag_wildcard_matches_anything,
     matches_etag_exact_match,
     matches_etag_weak_prefix_matches_strong,
     matches_etag_picks_one_from_a_comma_list,
     matches_etag_trims_surrounding_whitespace,
     matches_etag_mismatch_is_false].

%%%_* route/1 -----------------------------------------------------------------

route_classifies_well_known(_Config) ->
    ?assertEqual({ok, well_known, #{}}, a2a_card:route(?WELL_KNOWN)),
    ok.

route_classifies_catalog(_Config) ->
    ?assertEqual({ok, catalog, #{}}, a2a_card:route(<<"/agents">>)),
    ok.

route_unknown_path_is_error(_Config) ->
    ?assertEqual(error, a2a_card:route(<<"/nope">>)),
    %% The query string does not rescue an unknown path.
    ?assertEqual(error, a2a_card:route(<<"/nope?x=1">>)),
    ok.

%% The query string stays with the caller so the JSON-RPC router can keep
%% matching exact paths; route/1 only splits it off to parse the filters.
route_keeps_query_string_for_well_known(_Config) ->
    ?assertEqual({ok, well_known, #{<<"agent">> => <<"echo">>}},
                 a2a_card:route(<<"/.well-known/agent-card.json?agent=echo">>)),
    ok.

route_keeps_query_string_for_catalog(_Config) ->
    ?assertEqual({ok, catalog, #{<<"tag">> => <<"billing">>}},
                 a2a_card:route(<<"/agents?tag=billing">>)),
    ok.

%%%_* validate/1 --------------------------------------------------------------

validate_requires_all_fields(_Config) ->
    ?assertEqual({error, {missing_fields,
                          [<<"name">>, <<"description">>, <<"supportedInterfaces">>,
                           <<"version">>, <<"capabilities">>, <<"defaultInputModes">>,
                           <<"defaultOutputModes">>, <<"skills">>]}},
                 a2a_card:validate(#{})),
    ok.

%% Every absent required key is reported at once, not one per call.
validate_reports_every_missing_field(_Config) ->
    {error, {missing_fields, Missing}} = a2a_card:validate(#{<<"name">> => <<"x">>}),
    ?assert(lists:member(<<"description">>, Missing)),
    ?assert(lists:member(<<"skills">>, Missing)),
    ?assertNot(lists:member(<<"name">>, Missing)),
    ok.

validate_rejects_non_object(_Config) ->
    ?assertEqual({error, not_an_object}, a2a_card:validate(<<"not a map">>)),
    ?assertEqual({error, not_an_object}, a2a_card:validate([1, 2, 3])),
    ok.

%% supportedInterfaces, defaultInputModes and defaultOutputModes must be
%% non-empty lists; skills must be a list (an empty one is allowed) and
%% capabilities a map.
validate_rejects_empty_required_lists(_Config) ->
    ?assertEqual({error, invalid_shapes},
                 a2a_card:validate(valid_card(#{<<"supportedInterfaces">> => []}))),
    ?assertEqual({error, invalid_shapes},
                 a2a_card:validate(valid_card(#{<<"defaultInputModes">> => []}))),
    ?assertEqual({error, invalid_shapes},
                 a2a_card:validate(valid_card(#{<<"defaultOutputModes">> => []}))),
    %% capabilities must be a map, not a list.
    ?assertEqual({error, invalid_shapes},
                 a2a_card:validate(valid_card(#{<<"capabilities">> => []}))),
    %% An empty skills list is still a list, so it validates.
    ?assertEqual(ok, a2a_card:validate(valid_card(#{<<"skills">> => []}))),
    ok.

validate_accepts_a_well_formed_card(_Config) ->
    ?assertEqual(ok, a2a_card:validate(valid_card(#{}))),
    ok.

%%%_* etag/2 ------------------------------------------------------------------

%% "<version>-<base64url(sha256(card)[0..12])>", quoted.
etag_is_version_and_content_hash(_Config) ->
    Card = valid_card(#{}),
    ETag = a2a_card:etag(7, Card),
    %% Quoted, and begins with the version.
    ?assertEqual($\", binary:at(ETag, 0)),
    ?assertEqual($\", binary:at(ETag, byte_size(ETag) - 1)),
    ?assertEqual(<<"\"7-">>, binary:part(ETag, 0, 3)),
    ok.

%% Same version, different content, different ETag.
etag_changes_with_content(_Config) ->
    A = a2a_card:etag(1, valid_card(#{<<"name">> => <<"a">>})),
    B = a2a_card:etag(1, valid_card(#{<<"name">> => <<"b">>})),
    ?assertNotEqual(A, B),
    ok.

%% Same content, different version, different ETag (a manual edit invalidates).
etag_changes_with_version(_Config) ->
    Card = valid_card(#{}),
    ?assertNotEqual(a2a_card:etag(1, Card), a2a_card:etag(2, Card)),
    ok.

%% The ETag is stable for identical inputs.
etag_is_quoted(_Config) ->
    Card = valid_card(#{}),
    ?assertEqual(a2a_card:etag(3, Card), a2a_card:etag(3, Card)),
    ok.

%%%_* matches_etag/2 ----------------------------------------------------------

matches_etag_undefined_never_matches(_Config) ->
    ?assertNot(a2a_card:matches_etag(undefined, <<"\"1-abc\"">>)),
    ok.

matches_etag_wildcard_matches_anything(_Config) ->
    ?assert(a2a_card:matches_etag(<<"*">>, <<"\"1-abc\"">>)),
    ok.

matches_etag_exact_match(_Config) ->
    ?assert(a2a_card:matches_etag(<<"\"1-abc\"">>, <<"\"1-abc\"">>)),
    ok.

%% A weak `W/' client tag matches a strong stored ETag.
matches_etag_weak_prefix_matches_strong(_Config) ->
    ?assert(a2a_card:matches_etag(<<"W/\"1-abc\"">>, <<"\"1-abc\"">>)),
    ok.

%% A comma-separated list: any member matching is enough (the common cache case).
matches_etag_picks_one_from_a_comma_list(_Config) ->
    Header = <<"\"2-xyz\", \"1-abc\"">>,
    ?assert(a2a_card:matches_etag(Header, <<"\"1-abc\"">>)),
    ok.

matches_etag_trims_surrounding_whitespace(_Config) ->
    ?assert(a2a_card:matches_etag(<<"  \"1-abc\"  ">>, <<"\"1-abc\"">>)),
    ok.

matches_etag_mismatch_is_false(_Config) ->
    ?assertNot(a2a_card:matches_etag(<<"\"1-abc\"">>, <<"\"9-zzz\"">>)),
    ok.

%%%_* Helpers -----------------------------------------------------------------

%% A card with every required field; override individual keys to mutate it.
valid_card(Overrides) ->
    Base = #{<<"name">> => <<"echo">>,
             <<"description">> => <<"an echo agent">>,
             <<"supportedInterfaces">> => [#{<<"url">> => <<"https://x">>}],
             <<"version">> => <<"1">>,
             <<"capabilities">> => #{},
             <<"defaultInputModes">> => [<<"text">>],
             <<"defaultOutputModes">> => [<<"text">>],
             <<"skills">> => [#{<<"id">> => <<"echo">>, <<"name">> => <<"Echo">>}]},
    maps:merge(Base, Overrides).
