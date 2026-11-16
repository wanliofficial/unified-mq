%% @doc The CoAP app's boot postures and its credential guard (ADR-031 §D5/§D6).
%%
%% Everything here is about `application:start/1' rather than about datagrams:
%%
%%   * `enabled => false' (the ship default) must produce **zero children** --
%%     not "a listener that happens not to bind", but no socket at all.
%%   * `enabled => true' must produce exactly one listener per configured spec.
%%   * `require_auth' naming `coap' (plain UDP) must be **refused**: a datagram
%%     has no credential field, so the promise cannot be kept and silently
%%     starting an anonymous front-end would be the bug.
%%   * `require_auth' naming `coaps' (DTLS) is **honoured**, but only when every
%%     enabled listener really is DTLS -- one plain listener alongside would be the
%%     same silent downgrade.
%%   * the refusal must be surgical: `require_auth' listing some *other*
%%     protocol leaves CoAP alone (a guard that refuses everything is not a
%%     guard, it is an outage).
-module(coap_app_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).

all() ->
    [disabled_starts_with_no_children,
     enabled_starts_one_listener,
     require_auth_for_coap_is_refused,
     require_auth_for_coaps_is_allowed_when_dtls,
     coaps_requires_dtls_listeners,
     dtls_without_users_is_refused,
     unrelated_require_auth_still_starts].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    %% Load up front so the per-case `set_env' calls win over the `.app' file
    %% defaults when `ensure_all_started' later loads each app (same ordering as
    %% the other CoAP suites).
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
    Saved = #{require_auth => env(broker_security, require_auth),
              acl => env(broker_security, acl),
              users => env(broker_security, users)},
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
    %% Default posture for every case: off, no specs. Cases override.
    ok = application:set_env(broker_proto_coap, enabled, false),
    ok = application:set_env(broker_proto_coap, listeners, []),
    %% Storage and partitions must be up before the app under test can boot its
    %% dependencies (`broker_queue' recovers partitions in `init/1').
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    [{saved_env, Saved} | Config0].

end_per_testcase(_Case, Config) ->
    stop_all(),
    restore_env(?config(saved_env, Config)),
    ok.

%%%_* Cases -----------------------------------------------------------------

%% The ship default. "Zero socket" is asserted as "zero children": the
%% supervisor has nothing to start, so no `gen_udp:open/2' can have run.
disabled_starts_with_no_children(_) ->
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    ?assertEqual([], supervisor:which_children(broker_proto_coap_sup)).

%% Turned on with one spec: exactly one listener, and it reports the real
%% ephemeral port (so `port => 0' is observable to the operator and to tests).
enabled_starts_one_listener(_) ->
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners, [#{port => 0}]),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    Children = supervisor:which_children(broker_proto_coap_sup),
    ?assertEqual(1, length(Children)),
    [{Id, Pid, worker, _}] = Children,
    ?assertEqual({coap_listener, 0}, Id),
    {ok, Port} = coap_listener:port(Pid),
    ?assert(Port > 0).

%% A deployment that demands credentials from CoAP gets a boot failure, not an
%% anonymous listener. The reason names the protocol so the operator can see
%% why, and DTLS (S11-C) is the documented way to lift it.
require_auth_for_coap_is_refused(_) ->
    ok = application:set_env(broker_security, require_auth, [<<"coap">>]),
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners, [#{port => 0}]),
    Result = application:ensure_all_started(broker_proto_coap),
    ?assertEqual({require_auth_unsupported, coap}, refusal(Result)),
    ?assertEqual(undefined, whereis(broker_proto_coap_sup)).

%% `coaps' is honoured -- but only because the listener really is DTLS. This is
%% the positive control for the case below: if it never started, a guard that
%% simply refused `coaps' would pass.
require_auth_for_coaps_is_allowed_when_dtls(_) ->
    ok = application:set_env(broker_security, require_auth, [<<"coaps">>]),
    ok = application:set_env(broker_security, users,
                             [{<<"device-01">>, #{password => <<"secret-01">>}}]),
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners,
                             [#{port => 0, transport => dtls}]),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    ?assertEqual(1, length(supervisor:which_children(broker_proto_coap_sup))).

%% `require_auth' for `coaps' is only satisfiable when EVERY listener is DTLS. A
%% plain listener alongside a DTLS one would take requests with no principal at
%% all -- the silent downgrade §D5 exists to prevent -- so the mixed/plain case
%% is refused at boot.
coaps_requires_dtls_listeners(_) ->
    ok = application:set_env(broker_security, require_auth, [<<"coaps">>]),
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners, [#{port => 0}]),
    ?assertEqual({require_auth_needs_dtls, coaps},
                 refusal(application:ensure_all_started(broker_proto_coap))),
    ?assertEqual(undefined, whereis(broker_proto_coap_sup)).

%% A DTLS listener with an empty `users' table cannot authenticate anyone -- every
%% handshake would fail -- so it is a configuration error, refused at boot rather
%% than shipped as a listener nothing can talk to. (No `require_auth' here: the
%% refusal is about the listener's own precondition, not about policy.)
dtls_without_users_is_refused(_) ->
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners,
                             [#{port => 0, transport => dtls}]),
    Reason = refusal(application:ensure_all_started(broker_proto_coap)),
    ?assert(reason_contains(dtls_no_credentials, Reason)),
    ?assertEqual(undefined, whereis(broker_proto_coap_sup)).

%% Negative control: the guard keys off the protocol name, not off "the list is
%% non-empty". Asking MQTT for credentials must not stop CoAP from starting.
unrelated_require_auth_still_starts(_) ->
    ok = application:set_env(broker_security, require_auth, [<<"mqtt">>]),
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners, [#{port => 0}]),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    ?assertEqual(1, length(supervisor:which_children(broker_proto_coap_sup))).

%%%_* Helpers --------------------------------------------------------------

%% The application controller reports a start failure as
%% `{error, {App, Reason}}' the first time and `{error, {App, {Reason, MFA}}}'
%% on a later attempt in the same VM. Both say the same thing; unwrap them so
%% the assertion is about the reason, not about the controller's bookkeeping.
refusal({error, {broker_proto_coap, Reason}}) ->
    case Reason of
        {Why, {_M, _F, _A}} -> Why;
        Why -> Why
    end.

%% A child that refuses to init surfaces as a nested supervisor shutdown reason,
%% not as the bare tag, so search the term instead of matching one exact shape.
%% (The point of the assertion is "the boot failed FOR THIS REASON", and the
%% nesting is the supervisor's packaging, not ours.)
reason_contains(Tag, Term) when is_tuple(Term) ->
    lists:any(fun(E) -> reason_contains(Tag, E) end, tuple_to_list(Term));
reason_contains(Tag, Term) when is_list(Term) ->
    lists:any(fun(E) -> reason_contains(Tag, E) end, Term);
reason_contains(Tag, Term) ->
    Term =:= Tag.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{require_auth := Auth, acl := Acl, users := Users}) ->
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, users, Users),
    ok.

set_or_unset(_App, _Key, undefined) -> ok;
set_or_unset(App, Key, Value) -> application:set_env(App, Key, Value).

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
    cleanup_writers(),
    ok.

cleanup_writers() ->
    case whereis(broker_storage_wal_sup) of
        undefined ->
            ok;
        _Sup ->
            lists:foreach(
              fun({Id, _Child, _Type, _Modules}) ->
                      _ = supervisor:terminate_child(broker_storage_wal_sup, Id),
                      ok = supervisor:delete_child(broker_storage_wal_sup, Id)
              end, supervisor:which_children(broker_storage_wal_sup))
    end.
