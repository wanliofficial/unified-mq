%% @doc The three-tier session files of design §5.3, and nothing else.
%%
%% OQ-2's answer (design §13 K3): these files are **internal** to
%% `broker_subscription', not a behavior. S2 replaces them wholesale with the
%% `subscription_meta'/`cursor_meta'/`session_meta' Mnesia tables; what stays
%% stable is `broker_subscription''s public API, so abstracting the disk format
%% now would invent an interface with exactly one implementation and no
%% consumer. This module is pure IO: no process, no ETS, no knowledge of who
%% calls it.
%%
%% **v2 layout (ADR-019 §19.3)**: `.subs'/`.cursors' are keyed by SubRef, not by
%% client. A Shared/Failover group is one subscription with N consumers, so a
%% per-client file would either duplicate the group's cursor N times (two truths,
%% forbidden by ADR-013 §13.1) or pick a "primary" client, which is a second
%% membership table by another name. The SubRef is the identity; the file follows.
%%
%%  * `{kind}.{b64(Key)}.{b64(Filter)}.subs'      -- the subscription set of one
%%    SubRef with its ordered consumer list, fsynced on every change (R1b zero
%%    tolerance). Also carries the init cursors (K5): the subscribe-time tails
%%    travel with the record that is already paying for a synchronous write.
%%  * `{kind}.{b64(Key)}.{b64(Filter)}.cursors'   -- every partition's cursor row
%%    in one file (K4): one rename is an atomic update for all of them.
%%  * `{b64(ClientId)}.inflight'                 -- the opaque session blob
%%    `inflight/2' was handed (§5.3 tier 3, R7). **Still per client**: in-flight
%%    packet ids are a property of the connection, not of a subscription, so
%%    keying it by SubRef would split one allocator across N files (ADR-019
%%    §19.3, D-3).
%%
%% All writes use the same atomic sequence -- temp file, `file:sync', rename over
%% the target. Ids, group names and filters are arbitrary MQTT binaries (slashes
%% are legal), so filenames are url-safe base64 and the real value lives inside
%% the file: the name is an address, the content is the truth. A torn file is
%% structurally impossible: a rename lands old or new.
%%
%% `migrate_v1/0' rewrites the S1 layout (`{vsn,1,ClientId,Body}', one file per
%% client) in place at boot, **write-then-delete**: every v2 file lands before
%% any v1 file is unlinked, so an interrupted migration re-runs from a state that
%% is still complete (ADR-019 §19.4, D-3).
-module(broker_subscription_files).

-include_lib("broker_core/include/broker_message.hrl").

-export([data_dir/0, sessions_dir/0,
         subref_name/1, subref_from_name/2,
         subs_path/1, cursors_path/1, inflight_path/1,
         write_subs/2, write_cursors/2, write_inflight/2,
         read_subscription/1, read_cursors/1, read_inflight/1,
         list_subscriptions/0, prune_orphan_cursors/0,
         delete_subscription/1, delete_inflight/1,
         migrate_v1/0]).

-type sub_body()     :: #{kind := exclusive | shared | failover,
                          key := binary(),
                          filter := binary(),
                          qos := 0..2,
                          consumers := [binary()],
                          init_cursors := [{partition_id(), offset()}],
                          tenant_id => binary(),
                          created_at := integer()}.
-type cursors_body() :: #{kind := exclusive | shared | failover,
                          rows := #{partition_id() => map()}}.

-export_type([sub_body/0, cursors_body/0]).

%%%_* Directories and paths -----------------------------------------------

%% Its own env key, not `broker_storage''s: layers read their own config even
%% when the values coincide (both default to "data"; J8 applies here too --
%% relative to the node's CWD).
-spec data_dir() -> file:filename_all().
data_dir() ->
    application:get_env(broker_subscription, data_dir, "data").

-spec sessions_dir() -> file:filename_all().
sessions_dir() ->
    filename:join([data_dir(), "sessions"]).

%% @doc The filename stem of a SubRef: `"<kind>.<b64(Key)>.<b64(Filter)>"'.
%% Reversible (`subref_from_name/2'), which is what lets the boot scan and the
%% orphan sweep reconstruct identities from names alone.
-spec subref_name({atom(), binary(), binary()}) -> string().
subref_name({Kind, Key, Filter}) ->
    lists:flatten([atom_to_list(Kind), ".", b64(Key), ".", b64(Filter)]).

%% @doc Inverse of `subref_name/1'. The atom is parsed back with
%% `binary_to_existing_atom/2' so a corrupted name cannot mint atoms; an unknown
%% kind is an error, not a guess.
-spec subref_from_name(string(), string()) ->
          {ok, {exclusive | shared | failover, binary(), binary()}} | {error, term()}.
subref_from_name(Name, Ext) ->
    Base = filename:basename(Name, Ext),
    case string:split(Base, ".", all) of
        [KindBin, KeyBin, FilterBin] ->
            try binary_to_existing_atom(list_to_binary(KindBin), utf8) of
                Kind when Kind =:= exclusive; Kind =:= shared; Kind =:= failover ->
                    {ok, {Kind, b64_decode(KeyBin), b64_decode(FilterBin)}};
                Other ->
                    {error, {unknown_kind, Other, Name}}
            catch
                error:badarg -> {error, {unknown_kind, KindBin, Name}}
            end;
        _ ->
            {error, {unparseable_name, Name}}
    end.

-spec subs_path({atom(), binary(), binary()}) -> string().
subs_path(SubRef) ->
    subref_path(SubRef, ".subs").

-spec cursors_path({atom(), binary(), binary()}) -> string().
cursors_path(SubRef) ->
    subref_path(SubRef, ".cursors").

%% Still keyed by client (ADR-019 §19.3): see the module doc.
-spec inflight_path(binary()) -> string().
inflight_path(ClientId) ->
    filename:join(sessions_dir(), b64(ClientId) ++ ".inflight").

subref_path(SubRef, Ext) ->
    filename:join(sessions_dir(), subref_name(SubRef) ++ Ext).

%% base64url (`urlsafe' alphabet) without padding: filename-safe and no '='
%% trailing bytes, which some filesystems and tools treat as odd in a name.
b64(Bin) ->
    binary_to_list(base64:encode(Bin, #{mode => urlsafe, padding => false})).

b64_decode(Str) ->
    base64:decode(list_to_binary(Str), #{mode => urlsafe, padding => false}).

%%%_* Writes ---------------------------------------------------------------

%% @doc One subscription's set -- the consumers, the QoS and the init anchors.
%% The caller treats an error here as fatal: answering ok while `.subs' did not
%% reach the disk is exactly the R1b failure mode.
-spec write_subs({atom(), binary(), binary()}, sub_body()) -> ok | {error, term()}.
write_subs(SubRef, Body) ->
    write_fsynced(subs_path(SubRef), encode(SubRef, Body)).

%% @doc Every cursor row of one subscription. Batched (§5.3 tier 2), so the
%% regression window after a kill is "advances since the last write" -- and one
%% file means one window shared by every partition, never a mix of snapshots
%% taken at different instants.
-spec write_cursors({atom(), binary(), binary()}, cursors_body()) -> ok | {error, term()}.
write_cursors(SubRef, Body) ->
    write_fsynced(cursors_path(SubRef), encode(SubRef, Body)).

%% @doc Opaque session blob (R7: the broker never inspects it, only replays the
%% last one that landed). Client-keyed, vsn 1 envelope, unchanged by ADR-019.
-spec write_inflight(binary(), term()) -> ok | {error, term()}.
write_inflight(ClientId, Term) ->
    write_fsynced(inflight_path(ClientId), encode(ClientId, Term)).

%%%_* Reads ----------------------------------------------------------------

-spec read_subscription({atom(), binary(), binary()}) ->
          {ok, sub_body() | none} | {error, term()}.
read_subscription(SubRef) ->
    read(subs_path(SubRef), [safe]).

-spec read_cursors({atom(), binary(), binary()}) ->
          {ok, cursors_body() | none} | {error, term()}.
read_cursors(SubRef) ->
    read(cursors_path(SubRef), [safe]).

-spec read_inflight(binary()) -> {ok, term() | none} | {error, term()}.
read_inflight(ClientId) ->
    %% NOT `[safe]' (design §13 M11): the blob is opaque and written by the
    %% conn FSM, so it carries that app's atoms (`next_pid'/`pending'). Boot
    %% step 2 reads it before broker_proto_mqtt starts, and a `[safe]' decode
    %% then failed with `{corrupt_session, Path}' -- a persisted session became
    %% a boot blocker. `.subs'/`.cursors' bodies are binaries and integers
    %% only, so they keep `[safe]'. Integrity comes from the atomic rename, and
    %% the file lives in the local trusted data dir (same reasoning as M10).
    read(inflight_path(ClientId), []).

%% @doc Boot step 2's input: every persisted subscription, already migrated to
%% v2 by `migrate_v1/0'. A corrupt file is an error, not a skip: the alternative
%% is to boot without a subscription that exists on disk, which is the
%% silent-empty lie ADR-011 §11.2 exists to refuse.
-spec list_subscriptions() ->
          {ok, [{{atom(), binary(), binary()}, sub_body()}]} | {error, term()}.
list_subscriptions() ->
    Paths = filelib:wildcard(filename:join(sessions_dir(), "*.subs")),
    list_subscriptions(Paths, []).

list_subscriptions([], Acc) ->
    {ok, lists:reverse(Acc)};
list_subscriptions([Path | Rest], Acc) ->
    case read_any(Path) of
        {ok, {v2, SubRef, #{} = Body}} ->
            list_subscriptions(Rest, [{SubRef, Body} | Acc]);
        {ok, {v1, ClientId, _}} ->
            %% migrate_v1/0 runs first; reaching here means it did not finish,
            %% and booting on a half-migrated directory is the case the
            %% write-then-delete order exists to prevent.
            {error, {unmigrated_v1, Path, ClientId}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Delete every `.cursors' whose `.subs' is gone. An orphan cursor is the
%% residue of a teardown that died between the two unlinks; keeping it would let
%% a later subscription that happens to mint the same SubRef resume from a dead
%% one's position.
-spec prune_orphan_cursors() -> ok.
prune_orphan_cursors() ->
    lists:foreach(
      fun(Path) ->
              case filelib:is_file(filename:rootname(Path, ".cursors") ++ ".subs") of
                  true -> ok;
                  false -> _ = file:delete(Path), ok
              end
      end,
      filelib:wildcard(filename:join(sessions_dir(), "*.cursors"))).

%%%_* Cleanup --------------------------------------------------------------

%% @doc Remove both files of one subscription (UNSUBSCRIBE, last group member
%% leaving, clean_session teardown). Missing files are success: tearing down
%% something that was never persisted is a no-op, not an error to act on.
-spec delete_subscription({atom(), binary(), binary()}) -> ok.
delete_subscription(SubRef) ->
    delete_if_present([subs_path(SubRef), cursors_path(SubRef)]).

-spec delete_inflight(binary()) -> ok.
delete_inflight(ClientId) ->
    delete_if_present([inflight_path(ClientId)]).

delete_if_present(Paths) ->
    lists:foreach(
      fun(Path) ->
              case file:delete(Path) of
                  ok -> ok;
                  {error, enoent} -> ok
              end
      end, Paths).

%%%_* v1 -> v2 migration (ADR-019 §19.4) -----------------------------------

%% @doc Rewrite every S1 session file in place. Called from boot step 2, **before**
%% `list_subscriptions/0'; an error refuses the boot, because starting on a
%% half-migrated directory would serve a subscription set that disagrees with the
%% files (two truths, ADR-013 §13.1).
%%
%% Order per client: write every v2 `.subs' and `.cursors', *then* unlink the v1
%% pair. A crash in the middle leaves the v1 files intact and the v2 files
%% written, so the next boot redoes the same writes and deletes -- idempotent,
%% because a v2 rewrite of the same content is the same file.
-spec migrate_v1() -> ok | {error, term()}.
migrate_v1() ->
    Paths = filelib:wildcard(filename:join(sessions_dir(), "*.subs")),
    migrate_each(Paths).

migrate_each([Path | Rest]) ->
    case read_any(Path) of
        {ok, {v2, _SubRef, _Body}} ->
            migrate_each(Rest);
        {ok, {v1, ClientId, Entries}} ->
            case migrate_client(Path, ClientId, Entries) of
                ok -> migrate_each(Rest);
                {error, Reason} -> {error, {migration, Path, Reason}}
            end;
        {error, Reason} ->
            %% A corrupt file is the same refusal boot step 2 would make; doing
            %% it here names the file that is actually broken.
            {error, Reason}
    end;
migrate_each([]) ->
    ok.

migrate_client(Path, ClientId, Entries) when is_list(Entries) ->
    case read_any(v1_cursors_path(ClientId)) of
        {ok, {v1, _ClientId, CursorEntries}} when is_list(CursorEntries) ->
            case write_v2(ClientId, Entries, CursorEntries) of
                ok ->
                    %% Both v2 families are on disk; only now is the v1 pair
                    %% expendable.
                    _ = file:delete(Path),
                    _ = file:delete(v1_cursors_path(ClientId)),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {ok, none} ->
            case write_v2(ClientId, Entries, []) of
                ok ->
                    _ = file:delete(Path),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {ok, {v2, _, _}} ->
            {error, {v1_cursors_already_v2, ClientId}};
        {error, Reason} ->
            {error, Reason}
    end;
migrate_client(Path, ClientId, Other) ->
    {error, {corrupt_v1_subs, Path, ClientId, Other}}.

write_v2(ClientId, Entries, CursorEntries) ->
    CreatedAt = erlang:system_time(millisecond),
    write_v2_entries(ClientId, Entries, CursorEntries, CreatedAt).

write_v2_entries(ClientId, [{Filter, QoS, Init} | Rest], CursorEntries, CreatedAt)
  when is_binary(Filter), is_integer(QoS), is_list(Init) ->
    SubRef = {exclusive, ClientId, Filter},
    Body = #{kind => exclusive, key => ClientId, filter => Filter, qos => QoS,
             consumers => [ClientId], init_cursors => Init,
             created_at => CreatedAt},
    case write_subs(SubRef, Body) of
        ok ->
            case write_v2_cursors(SubRef, CursorEntries) of
                ok -> write_v2_entries(ClientId, Rest, CursorEntries, CreatedAt);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
write_v2_entries(_ClientId, [], _CursorEntries, _CreatedAt) ->
    ok;
write_v2_entries(_ClientId, [Other | _], _CursorEntries, _CreatedAt) ->
    {error, {corrupt_v1_entry, Other}}.

write_v2_cursors(SubRef = {exclusive, _ClientId, Filter}, CursorEntries) ->
    case lists:keyfind(Filter, 1, CursorEntries) of
        {Filter, Rows} when is_list(Rows), Rows =/= [] ->
            write_cursors(SubRef, #{kind => exclusive,
                                    rows => maps:from_list(
                                              [{P, #{kind => exclusive,
                                                     offset => Offset,
                                                     pending => []}}
                                               || {P, Offset} <- Rows])});
        false ->
            %% No cursor file for this filter (or no `.cursors' at all): boot
            %% falls back to the init anchors inside `.subs' (K5), which is
            %% exactly what the in-memory state was before the first flush.
            ok;
        {_, _Corrupt} ->
            {error, {corrupt_v1_cursors, Filter}}
    end.

v1_cursors_path(ClientId) ->
    filename:join(sessions_dir(), b64(ClientId) ++ ".cursors").

%%%_* Internal -------------------------------------------------------------

%% v1 and v2 envelopes side by side. `{vsn, 2, SubRef, Body}' is what every
%% writer here emits; `{vsn, 1, ClientId, Body}' is only ever *read*, by
%% `migrate_v1/0'.
read_any(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            decode_any(Bin, Path);
        {error, enoent} ->
            {ok, none};
        {error, Reason} ->
            {error, {read_failed, Path, Reason}}
    end.

read(Path, TermOpts) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            case decode(Bin, Path, TermOpts) of
                {ok, {v2, _SubRef, Body}} -> {ok, Body};
                {ok, {v1, ClientId, _Body}} ->
                    %% migrate_v1/0 has already run: a v1 file here is a
                    %% half-migrated directory, which is a boot refusal rather
                    %% than a silently older answer.
                    {error, {unmigrated_v1, Path, ClientId}};
                {error, Reason} -> {error, Reason}
            end;
        %% Absent is not corrupt: a subscription that never flushed a cursor
        %% tick has no `.cursors', and boot falls back to the origins inside
        %% `.subs' (K5).
        {error, enoent} ->
            {ok, none};
        {error, Reason} ->
            {error, {read_failed, Path, Reason}}
    end.

encode(Key, Body) ->
    term_to_binary({vsn, 2, Key, Body}).

decode(Bin, Path, TermOpts) ->
    decode_any(Bin, Path, TermOpts).

decode_any(Bin, Path) ->
    decode_any(Bin, Path, [safe]).

decode_any(Bin, Path, TermOpts) ->
    try binary_to_term(Bin, TermOpts) of
        {vsn, 2, SubRef, Body} -> {ok, {v2, SubRef, Body}};
        {vsn, 1, ClientId, Body} -> {ok, {v1, ClientId, Body}};
        {vsn, V, _, _} -> {error, {unsupported_vsn, V, Path}};
        Other -> {error, {corrupt_session, Path, Other}}
    catch
        error:_ -> {error, {corrupt_session, Path}}
    end.

%% Temp + fsync + rename. `raw' so the bytes bypass the BEAM io server and the
%% later `file:sync' is the promise; rename is atomic on ext4/xfs, so a reader
%% either sees the whole old file or the whole new one. No directory fsync --
%% same power-loss stance as the WAL (§8.3, S2 work).
write_fsynced(Path, Bin) ->
    Tmp = Path ++ ".tmp",
    ok = filelib:ensure_dir(Tmp),
    case file:open(Tmp, [write, raw]) of
        {ok, Fd} ->
            case write_sync_close(Tmp, Fd, Bin) of
                ok ->
                    case file:rename(Tmp, Path) of
                        ok -> ok;
                        {error, Reason} -> {error, {rename, Path, Reason}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, {open, Tmp, Reason}}
    end.

write_sync_close(Tmp, Fd, Bin) ->
    case file:write(Fd, Bin) of
        ok ->
            case file:sync(Fd) of
                ok ->
                    ok = file:close(Fd),
                    ok;
                {error, Reason} ->
                    _ = file:close(Fd),
                    {error, {sync, Tmp, Reason}}
            end;
        {error, Reason} ->
            _ = file:close(Fd),
            {error, {write, Tmp, Reason}}
    end.
