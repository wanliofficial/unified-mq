%% @doc The CoAP resource layer: RFC 7252 methods mapped onto the message kernel.
%%
%% ADR-031 §D4 froze this mapping: a resource path IS a `broker_router'
%% destination, `a/b/c' with no namespace of its own. `coap_resource:handle/3'
%% is a pure function of (request, config, ctx), so these cases call it
%% directly rather than going through the UDP listener -- the listener's own
%% suite (`coap_listener_SUITE') covers the datagram path, dedup, Block1 and the
%% token bucket. Keeping the resource cases socket-free makes them fast and
%% deterministic: the only moving parts are the retained store and the ACL.
%%
%% The contract pinned here is the table in the module header:
%%   GET    /a/b  -> broker_retain:lookup, 2.05 Content or 4.04
%%   PUT    /a/b  -> publish (retain),     2.04 Changed
%%   POST   /a/b  -> publish (not retained), 2.01 Created
%%   DELETE /a/b  -> broker_retain:delete, 2.02 Deleted or 4.04
%%   GET    /.well-known/core -> link format over this adapter, 2.05
%%
%% And the two posture rules ADR-031 fixed: a NON write is dispatched live and
%% NOT appended to the log (§D3), and plain UDP carries no credential so the
%% principal is `undefined' and `broker_security' treats that as anonymous-allowed
%% (§D5) until S11-C's DTLS identity exists.
-module(coap_resource_SUITE).

-compile([export_all, nowarn_export_all]).
-compile({no_auto_import, [put/2]}).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(N, 2).
-define(HOST, {127, 0, 0, 1}).

%% ctx used by every case: plain UDP has no credential, so the principal is
%% `undefined' and `broker_security' allows the anonymous caller.
-define(CTX, #{endpoint => {?HOST, 0},
               tenant => <<"coap-test">>,
               principal => undefined}).
-define(CFG, #{}).

all() ->
    [get_on_empty_resource_is_404,
     put_then_get_returns_the_retained_payload,
     put_replaces_the_representation,
     put_answers_204_changed,
     post_answers_201_created_and_does_not_retain,
     delete_removes_retained_state_and_answers_202,
     delete_is_idempotent_on_absent_resource,
     put_to_root_destination_is_400,
     well_known_lists_retained_destinations,
     well_known_includes_static_resources,
     well_known_filters_retained_by_acl,
     well_known_deny_posture_lists_no_retained,
     non_write_dispatches_without_appending,
     non_policy_rejection_has_no_side_effects,
     non_policy_rejection_preserves_old_representation,
     non_policy_rejection_can_quarantine,
     con_policy_rejection_is_500,
     non_normalize_dispatches_accepted_event,
     non_post_dispatches_without_retaining,
     get_reads_only_retained_not_the_log,
     observe_option_is_ignored_not_rejected,
     observe_1_returns_representation_without_option].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    %% Load every kernel app up front so the per-case `set_env' calls below
    %% override the loaded (app.src default) environment instead of being
    %% overwritten when `ensure_all_started' later loads the .app file. This is
    %% the ordering `mqtt_sn_conn_SUITE' relies on.
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_retain),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_coap),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config0) ->
    stop_all(),
    Saved = #{acl => env(broker_security, acl),
              require_auth => env(broker_security, require_auth),
              users => env(broker_security, users),
              policy => env(broker_event, policy),
              policies => env(broker_event, policies),
              quarantine => env(broker_event, quarantine),
              trace_context => env(broker_event, trace_context)},
    DataDir = filename:join([?config(priv_dir, Config0), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_event, policy, pass_through),
    ok = application:set_env(broker_event, policies, []),
    ok = application:set_env(broker_event, quarantine, []),
    ok = application:set_env(broker_event, trace_context, off),
    %% Start storage first, create the queue partitions, then start the rest of
    %% the kernel via the app under test -- `broker_queue' boots its partition
    %% workers against handles that `create_partitions/0' just created (the same
    %% order `mqtt_sn_conn_SUITE' uses).
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    ok = application:set_env(broker_proto_coap, enabled, false),
    ok = application:set_env(broker_proto_coap, listeners, []),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    [{saved_env, Saved} | Config0].

end_per_testcase(_Case, Config) ->
    stop_all(),
    restore_env(?config(saved_env, Config)),
    ok.

%%%_* Cases -----------------------------------------------------------------

get_on_empty_resource_is_404(_) ->
    Req = request(con, get, 1, [{uri_path, <<"a">>}]),
    {reply, {response, 4, 4}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX).

put_then_get_returns_the_retained_payload(_) ->
    put(<<"a/b">>, <<"hello">>),
    Req = request(con, get, 2, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    {reply, {response, 2, 5}, _, <<"hello">>} =
        coap_resource:handle(Req, ?CFG, ?CTX).

put_replaces_the_representation(_) ->
    put(<<"a/b">>, <<"first">>),
    put(<<"a/b">>, <<"second">>),
    Req = request(con, get, 3, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    {reply, {response, 2, 5}, _, <<"second">>} =
        coap_resource:handle(Req, ?CFG, ?CTX).

put_answers_204_changed(_) ->
    Req = request(con, put, 1, [{uri_path, <<"a">>}], <<"body">>),
    {reply, {response, 2, 4}, _, <<>>} = coap_resource:handle(Req, ?CFG, ?CTX).

post_answers_201_created_and_does_not_retain(_) ->
    Req = request(con, post, 1, [{uri_path, <<"a">>}], <<"evt">>),
    {reply, {response, 2, 1}, _, <<>>} = coap_resource:handle(Req, ?CFG, ?CTX),
    %% POST is an event on the resource: it does not become the representation.
    GetReq = request(con, get, 2, [{uri_path, <<"a">>}]),
    {reply, {response, 4, 4}, _, _} = coap_resource:handle(GetReq, ?CFG, ?CTX).

delete_removes_retained_state_and_answers_202(_) ->
    put(<<"a/b">>, <<"data">>),
    DelReq = request(con, delete, 1, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    {reply, {response, 2, 2}, _, _} =
        coap_resource:handle(DelReq, ?CFG, ?CTX),
    GetReq = request(con, get, 2, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    {reply, {response, 4, 4}, _, _} = coap_resource:handle(GetReq, ?CFG, ?CTX).

delete_is_idempotent_on_absent_resource(_) ->
    %% broker_retain:delete/2 is idempotent: deleting a destination that holds no
    %% retained state still answers ok, so the resource layer returns 2.02 rather
    %% than 4.04. (4.04 is reserved for a genuine store error.)
    Req = request(con, delete, 1, [{uri_path, <<"nope">>}]),
    {reply, {response, 2, 2}, _, _} =
        coap_resource:handle(Req, ?CFG, ?CTX).

put_to_root_destination_is_400(_) ->
    %% An empty destination (a CoAP request to the root with no path) is not a
    %% legal publish target: an empty string is far more likely a client bug.
    Req = request(con, put, 1, [], <<"x">>),
    {reply, {response, 4, 0}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX).

well_known_lists_retained_destinations(_) ->
    put(<<"temp/living">>, <<"21">>),
    put(<<"temp/kitchen">>, <<"19">>),
    Req = request(con, get, 1, [{uri_path, <<".well-known">>}, {uri_path, <<"core">>}]),
    {reply, {response, 2, 5}, [{content_format, _}], Body} =
        coap_resource:handle(Req, ?CFG, ?CTX),
    %% The link format is a CoRE document; both retained destinations appear in it.
    ?assert(is_binary(Body)),
    ?assertNotEqual(nomatch, binary:match(Body, <<"temp/living">>)),
    ?assertNotEqual(nomatch, binary:match(Body, <<"temp/kitchen">>)).

well_known_includes_static_resources(_) ->
    Cfg = #{resources => [#{path => <<"sensors/door">>,
                            attrs => [{rt, <<"door.sensor">>}, {'if', <<"sensor">>}]}]},
    Req = request(con, get, 1, [{uri_path, <<".well-known">>}, {uri_path, <<"core">>}]),
    {reply, {response, 2, 5}, _, Body} =
        coap_resource:handle(Req, Cfg, ?CTX),
    ?assertNotEqual(nomatch, binary:match(Body, <<"sensors/door">>)),
    ?assertNotEqual(nomatch, binary:match(Body, <<"door.sensor">>)).

%% Discovery must not become a way around the per-destination ACL. Two retained
%% destinations exist, one under each "tenant" name; with a rule list that lets
%% alice (t1) subscribe only to `t1/#', the link-format document lists the t1
%% resource and NOT the t2 one -- the same `subscribe' decision a plain GET on
%% each destination would make.
well_known_filters_retained_by_acl(_) ->
    put(<<"t1/x">>, <<"t1-data">>),
    put(<<"t2/y">>, <<"t2-data">>),
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{password => <<"pw">>, tenant_id => <<"t1">>}}]),
    ok = application:set_env(broker_security, acl,
                             [acl_rule(allow, <<"alice">>, subscribe, <<"t1/#">>)]),
    Req = request(con, get, 1, [{uri_path, <<".well-known">>}, {uri_path, <<"core">>}]),
    {reply, {response, 2, 5}, [{content_format, _}], Body} =
        coap_resource:handle(Req, ?CFG, alice_ctx()),
    %% Positive control: the allowed destination is listed.
    ?assertNotEqual(nomatch, binary:match(Body, <<"t1/x">>)),
    %% The disallowed one is not.
    ?assertEqual(nomatch, binary:match(Body, <<"t2/y">>)).

%% Under deny-by-default the anonymous door is documented as closed: a plain GET
%% is refused (4.03). The discovery document must not leak the retained
%% destination set anyway -- it lists none of them.
well_known_deny_posture_lists_no_retained(_) ->
    put(<<"leak/canary">>, <<"secret">>),
    ok = application:set_env(broker_security, acl, deny),
    Req = request(con, get, 2, [{uri_path, <<".well-known">>}, {uri_path, <<"core">>}]),
    {reply, {response, 2, 5}, _, Body} = coap_resource:handle(Req, ?CFG, ?CTX),
    ?assertEqual(nomatch, binary:match(Body, <<"leak/canary">>)).

alice_ctx() ->
    #{endpoint => {?HOST, 0}, tenant => <<"t1">>, principal => <<"alice">>}.

acl_rule(Effect, Principal, Action, Pattern) ->
    #{effect => Effect, principal => Principal,
      action => Action, pattern => Pattern}.

non_write_dispatches_without_appending(_) ->
    %% A NON PUT updates retained state (so a later GET sees it) but is
    %% at-most-once: it is dispatched live and NOT appended to the log (§D3).
    Req = request(non, put, 1, [{uri_path, <<"a">>}], <<"live">>),
    {reply, {response, 2, 4}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX),
    GetReq = request(con, get, 2, [{uri_path, <<"a">>}]),
    {reply, {response, 2, 5}, _, <<"live">>} =
        coap_resource:handle(GetReq, ?CFG, ?CTX),
    ?assertEqual([], queue_rows()).

non_policy_rejection_has_no_side_effects(_) ->
    ok = application:set_env(broker_event, policy, required),
    _ = subscribe_live(<<"reject/a">>),
    Req = request(non, put, 1, [{uri_path, <<"reject">>}, {uri_path, <<"a">>}],
                  <<"ordinary">>),
    {reply, {response, 5, 0}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX),
    ?assertEqual(error, broker_retain:lookup(<<"reject/a">>)),
    ?assertEqual([], queue_rows()),
    ?assertEqual(timeout, live_delivery(200)).

non_policy_rejection_preserves_old_representation(_) ->
    put(<<"reject/old">>, <<"accepted">>),
    RowsBefore = queue_rows(),
    ok = application:set_env(broker_event, policy, required),
    Req = request(non, put, 2, [{uri_path, <<"reject">>}, {uri_path, <<"old">>}],
                  <<"replacement">>),
    {reply, {response, 5, 0}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX),
    {ok, #broker_message{payload = <<"accepted">>}} =
        broker_retain:lookup(<<"reject/old">>),
    ?assertEqual(RowsBefore, queue_rows()).

non_policy_rejection_can_quarantine(_) ->
    ok = application:set_env(broker_event, policy, required),
    ok = application:set_env(
           broker_event, quarantine,
           [{<<"coap-test">>, <<"reject/q">>, <<"quarantine/non">>}]),
    _ = subscribe_live(<<"reject/q">>),
    Req = request(non, put, 1, [{uri_path, <<"reject">>}, {uri_path, <<"q">>}],
                  <<"evidence">>),
    {reply, {response, 5, 0}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX),
    ?assertEqual(error, broker_retain:lookup(<<"reject/q">>)),
    ?assertEqual(timeout, live_delivery(200)),
    [#broker_message{destination = <<"quarantine/non">>,
                     payload = <<"evidence">>,
                     persistence = durable,
                     reliability = fsync,
                     headers = Headers}] = queue_rows(),
    ?assertMatch(#{event_rejection := _,
                   original_destination := <<"reject/q">>}, Headers).

con_policy_rejection_is_500(_) ->
    ok = application:set_env(broker_event, policy, required),
    Req = request(con, put, 1, [{uri_path, <<"reject">>}, {uri_path, <<"con">>}],
                  <<"ordinary">>),
    {reply, {response, 5, 0}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX),
    ?assertEqual(error, broker_retain:lookup(<<"reject/con">>)),
    ?assertEqual([], queue_rows()).

non_normalize_dispatches_accepted_event(_) ->
    ok = application:set_env(broker_event, policy, normalize),
    _ = subscribe_live(<<"events/non">>),
    Payload = cloud_event_payload(),
    Req = request(non, put, 1, [{uri_path, <<"events">>}, {uri_path, <<"non">>}],
                  Payload),
    {reply, {response, 2, 4}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX),
    {ok, #broker_message{payload = Payload,
                         event = #{id := <<"non-event">>}}} =
        broker_retain:lookup(<<"events/non">>),
    {broker_deliver, _, 0, _, {undefined, undefined,
                              #broker_message{payload = Payload,
                                              event = #{id := <<"non-event">>}}}} =
        live_delivery(1000),
    ?assertEqual([], queue_rows()).

non_post_dispatches_without_retaining(_) ->
    _ = subscribe_live(<<"events/post">>),
    Req = request(non, post, 1, [{uri_path, <<"events">>}, {uri_path, <<"post">>}],
                  <<"notification">>),
    {reply, {response, 2, 1}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX),
    ?assertEqual(error, broker_retain:lookup(<<"events/post">>)),
    {broker_deliver, _, 0, _, {undefined, undefined,
                              #broker_message{payload = <<"notification">>}}} =
        live_delivery(1000),
    ?assertEqual([], queue_rows()).

get_reads_only_retained_not_the_log(_) ->
    %% POST appends to the log but does not retain, so a GET (which reads only
    %% the retained store) still 404s. This pins that GET is a representation
    %% fetch, not a log tail.
    PostReq = request(con, post, 1, [{uri_path, <<"a">>}], <<"evt">>),
    {reply, {response, 2, 1}, _, _} = coap_resource:handle(PostReq, ?CFG, ?CTX),
    GetReq = request(con, get, 2, [{uri_path, <<"a">>}]),
    {reply, {response, 4, 4}, _, _} = coap_resource:handle(GetReq, ?CFG, ?CTX).

observe_option_is_ignored_not_rejected(_) ->
    %% The resource layer itself answers the plain representation and adds no
    %% Observe option; adding `Observe: 0' and minting the subscription is the
    %% listener's job (RFC 7641 §3.4, covered by `coap_listener_SUITE').
    put(<<"clock">>, <<"t0">>),
    Req = request(con, get, 1, [{uri_path, <<"clock">>}, {observe, 0}]),
    {reply, {response, 2, 5}, _, <<"t0">>} =
        coap_resource:handle(Req, ?CFG, ?CTX).

%% Observe=1 deregisters (the listener drops the relationship), but the resource
%% layer still answers the GET as usual: the current representation with NO
%% Observe option of its own (RFC 7641 §4.1) -- not a bare 2.01 "Created". This
%% module stays a pure function; the listener owns the subscription.
observe_1_returns_representation_without_option(_Config) ->
    put(<<"z">>, <<"v">>),
    Req = request(con, get, 9, [{uri_path, <<"z">>}, {observe, 1}]),
    {reply, {response, 2, 5}, Options, <<"v">>} =
        coap_resource:handle(Req, ?CFG, ?CTX),
    ?assertNot(lists:keymember(observe, 1, Options)).

%%%_* Helpers ---------------------------------------------------------------

%% PUT a payload to a destination (slash-joined path) via the resource layer.
put(Destination, Payload) ->
    Segs = path_segments(Destination),
    Req = request(con, put, 1, [{uri_path, S} || S <- Segs], Payload),
    {reply, {response, 2, 4}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX),
    ok.

path_segments(<<>>) -> [];
path_segments(Path) -> binary:split(Path, <<"/">>, [global]).

%% Build a request message. `coap_codec:request/4' takes the method, id and
%% options; the payload is set on the `payload' map field directly, mirroring how
%% the listener hands a decoded datagram to the resource layer.
request(Type, Method, Id, Options) ->
    coap_codec:request(Type, Method, Id, Options).

request(Type, Method, Id, Options, Payload) ->
    (coap_codec:request(Type, Method, Id, Options))#{payload => Payload}.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{acl := Acl, require_auth := Auth, users := Users,
              policy := Policy, policies := Policies, quarantine := Quarantine,
              trace_context := Trace}) ->
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, users, Users),
    set_or_unset(broker_event, policy, Policy),
    set_or_unset(broker_event, policies, Policies),
    set_or_unset(broker_event, quarantine, Quarantine),
    set_or_unset(broker_event, trace_context, Trace),
    ok.

set_or_unset(_App, _Key, undefined) -> ok;
set_or_unset(App, Key, Value) -> application:set_env(App, Key, Value).

subscribe_live(Destination) ->
    {ok, SubRef} = broker_subscription:join(
                     shared, Destination, <<"coap-live">>, Destination, 0,
                     <<"coap-test">>, #{anchor => tail}),
    {ok, undefined} = broker_subscription:attach(<<"coap-live">>, self(), false),
    SubRef.

live_delivery(Timeout) ->
    receive
        {broker_deliver, _, _, _, _} = Delivery -> Delivery
    after Timeout ->
        timeout
    end.

cloud_event_payload() ->
    iolist_to_binary(
      json:encode(#{<<"specversion">> => <<"1.0">>,
                    <<"id">> => <<"non-event">>,
                    <<"source">> => <<"/coap-test">>,
                    <<"type">> => <<"com.example.non">>,
                    <<"data">> => <<"payload">>})).

queue_rows() ->
    [Row || P <- broker_queue:partition_ids(),
            {ok, Rows, _} <- [broker_queue:read_from(P, 0, 100)],
            {_Offset, Row} <- Rows].

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

stop_all() ->
    _ = application:stop(broker_proto_coap),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_security),
    _ = application:stop(broker_event),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.
