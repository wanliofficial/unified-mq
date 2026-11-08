%% @doc Mnesia control plane (ADR-002, ADR-021 §D1).
%%
%% Only metadata lives here: tenants, users, ACL rules, topic/queue/binding
%% metadata, cluster nodes, partition ownership snapshots, the config version
%% and the outbox. Message bodies, per-ack state and per-partition cursors stay
%% in the data plane -- putting them here would make Mnesia the throughput
%% bottleneck ADR-002 explicitly rejected.
%%
%% Reads are dirty reads on purpose: a control-plane read must be cheap and must
%% never queue behind a transaction that is waiting on a remote node. Writes are
%% transactions so a version bump and its outbox row are atomic.
-module(broker_meta).

%% `put/2' is our row writer, not the process-dictionary BIF.
-compile({no_auto_import, [put/2]}).

-export([tables/0, ensure_schema/0, reset/0]).
-export([put/2, get/2, delete/2, all/1, clear/1]).
-export([put_tenant/1, get_tenant/1,
         put_user/3, get_user/1, authenticate/2,
         put_acl/1, acls/1,
         put_partition/1, get_partition/1,
         put_node/2, nodes/0,
         version/0, bump_version/1,
         enqueue/2, pending_outbox/0, mark_published/1]).

-define(VERSION_KEY, global).

%%%_* Schema --------------------------------------------------------------

%% One definition shared by `create_table/2' and `reset/0', so a test can never
%% leave a table out and then pass against a degraded schema.
tables() ->
    [{tenant,          [tenant, tenant_id, name, status, created_at]},
     {broker_user,     [broker_user, username, tenant_id, password_hash, salt, roles]},
     {acl_rule,        [acl_rule, id, tenant_id, principal, action, resource, effect]},
     {topic_meta,      [topic_meta, key, partitions, config]},
     {queue_meta,      [queue_meta, key, config]},
     {binding_meta,    [binding_meta, key, source, target, config]},
     {subscription_meta, [subscription_meta, sub_ref, tenant_id, filter, kind, config]},
     {cluster_node,    [cluster_node, node, role, status, joined_at]},
     {partition_meta,  [partition_meta, partition_id, owner, epoch, replicas, state]},
     {config_version,  [config_version, key, version, updated_at]},
     {config_outbox,   [config_outbox, seq, version, event, created_at, published_at]}].

%% @doc Create the schema if this node has none, start Mnesia, create any
%% missing table and wait for all of them. Returns an error instead of booting a
%% half-initialized control plane.
-spec ensure_schema() -> ok | {error, term()}.
ensure_schema() ->
    Storage = env(table_storage, ram_copies),
    Nodes = nodes_env(),
    _ = maybe_set_dir(Storage),
    case mnesia:system_info(is_running) of
        yes ->
            ok;
        no ->
            case mnesia:create_schema(Nodes) of
                ok -> ok;
                {error, {_, {already_exists, _}}} -> ok;
                {error, Reason} -> throw({schema, Reason})
            end,
            {ok, _} = application:ensure_all_started(mnesia),
            ok
    end,
    case create_tables(Storage, Nodes) of
        ok -> wait_tables();
        {error, _} = Error -> Error
    end.

wait_tables() ->
    Tabs = [T || {T, _} <- tables()],
    case mnesia:wait_for_tables(Tabs, 15000) of
        ok -> ok;
        {timeout, Missing} -> {error, {tables_not_ready, Missing}};
        {error, Reason} -> {error, Reason}
    end.

maybe_set_dir(disc_copies) ->
    Dir = application:get_env(broker_meta, data_dir, "data"),
    application:set_env(mnesia, dir, filename:absname(Dir));
maybe_set_dir(_) ->
    ok.

create_tables(Storage, Nodes) ->
    lists:foldl(
      fun({Tab, Attrs}, ok) ->
              %% Mnesia attributes exclude the record name; our table list
              %% keeps it for readability, so drop it here (not at each call).
              Opts = [{attributes, tl(Attrs)}, {type, set}, {Storage, Nodes}],
              case mnesia:create_table(Tab, Opts) of
                  {atomic, ok} -> ok;
                  {aborted, {already_exists, Tab}} -> ok;
                  {aborted, Reason} -> {error, {Tab, Reason}}
              end;
         (_Tab, {error, _} = Error) ->
              Error
      end, ok, tables()).

%% @doc Test/ops reset: stop the app, stop Mnesia and drop this node's schema.
%% Destructive by design; not called by normal boot.
-spec reset() -> ok | {error, term()}.
reset() ->
    _ = application:stop(broker_meta),
    _ = application:stop(mnesia),
    _ = mnesia:delete_schema([node()]),
    ok.

%%%_* Generic rows ---------------------------------------------------------

-spec put(atom(), tuple()) -> ok | {error, term()}.
put(Tab, Row) when is_atom(Tab), is_tuple(Row) ->
    case mnesia:transaction(fun() -> mnesia:write(Tab, Row, write) end) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

-spec get(atom(), term()) -> {ok, tuple()} | not_found | {error, term()}.
get(Tab, Key) ->
    case mnesia:dirty_read(Tab, Key) of
        [Row | _] -> {ok, Row};
        [] -> not_found
    end.

-spec delete(atom(), term()) -> ok | {error, term()}.
delete(Tab, Key) ->
    case mnesia:transaction(fun() -> mnesia:delete({Tab, Key}) end) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

-spec all(atom()) -> [tuple()].
all(Tab) ->
    mnesia:dirty_match_object(mnesia:table_info(Tab, wild_pattern)).

-spec clear(atom()) -> ok | {error, term()}.
clear(Tab) ->
    case mnesia:clear_table(Tab) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%%_* Domain helpers -------------------------------------------------------

put_tenant(TenantId) when is_binary(TenantId) ->
    put(tenant, {tenant, TenantId, TenantId, active, now_ms()}).

get_tenant(TenantId) ->
    get(tenant, TenantId).

%% Passwords are never stored in plaintext: `salt' is per user and the hash is
%% sha256(salt || password). ADR-009's principal remains the authenticated
%% username; nothing here reads event attributes.
put_user(Username, TenantId, Password)
  when is_binary(Username), is_binary(TenantId), is_binary(Password) ->
    Salt = crypto:strong_rand_bytes(16),
    Hash = hash_password(Password, Salt),
    put(broker_user, {broker_user, Username, TenantId, Hash, Salt, []}).

get_user(Username) ->
    get(broker_user, Username).

-spec authenticate(binary(), binary()) -> {ok, binary()} | {error, bad_username_or_password}.
authenticate(Username, Password) when is_binary(Username), is_binary(Password) ->
    case get(broker_user, Username) of
        {ok, {broker_user, _, TenantId, Hash, Salt, _}} ->
            case hash_password(Password, Salt) =:= Hash of
                true -> {ok, TenantId};
                false -> {error, bad_username_or_password}
            end;
        _ ->
            %% Same wording for "no such user" and "wrong password": a control
            %% plane must not turn authentication into a user oracle.
            {error, bad_username_or_password}
    end;
authenticate(_, _) ->
    {error, bad_username_or_password}.

put_acl(#{id := Id, tenant_id := Tenant, principal := Principal,
          action := Action, resource := Resource, effect := Effect}) ->
    put(acl_rule, {acl_rule, Id, Tenant, Principal, Action, Resource, Effect}).

-spec acls(binary()) -> [tuple()].
acls(Tenant) ->
    [R || R <- all(acl_rule), element(3, R) =:= Tenant].

put_partition(#{partition_id := P, owner := Owner, epoch := Epoch,
                replicas := Replicas, state := State}) ->
    put(partition_meta, {partition_meta, P, Owner, Epoch, Replicas, State}).

get_partition(PartitionId) ->
    get(partition_meta, PartitionId).

put_node(Node, Role) ->
    put(cluster_node, {cluster_node, Node, Role, up, now_ms()}).

nodes() ->
    all(cluster_node).

%%%_* Config version and outbox -------------------------------------------

-spec version() -> non_neg_integer().
version() ->
    case get(config_version, ?VERSION_KEY) of
        {ok, {config_version, _, V, _}} -> V;
        _ -> 0
    end.

%% @doc Atomic version bump + notification, owned by `broker_meta_config'.
-spec bump_version(term()) -> {ok, non_neg_integer()} | {error, term()}.
bump_version(Changed) ->
    try gen_server:call(broker_meta_config, {bump, Changed}, 5000)
    catch
        exit:Reason -> {error, {config_unavailable, Reason}}
    end.

enqueue(Event, Version) ->
    Seq = erlang:unique_integer([positive, monotonic]),
    put(config_outbox,
        {config_outbox, Seq, Version, Event, now_ms(), undefined}).

pending_outbox() ->
    Pending = [R || R <- all(config_outbox), element(6, R) =:= undefined],
    lists:sort(fun(A, B) -> element(2, A) =< element(2, B) end, Pending).

mark_published(Seq) ->
    case get(config_outbox, Seq) of
        {ok, Row} -> put(config_outbox, setelement(6, Row, now_ms()));
        not_found -> ok
    end.

%%%_* Internal -------------------------------------------------------------

env(Key, Default) ->
    application:get_env(broker_meta, Key, Default).

nodes_env() ->
    case env(nodes, []) of
        [] -> [node()];
        Ns when is_list(Ns) -> Ns
    end.

hash_password(Password, Salt) ->
    crypto:hash(sha256, <<Salt/binary, Password/binary>>).

now_ms() ->
    erlang:system_time(millisecond).
