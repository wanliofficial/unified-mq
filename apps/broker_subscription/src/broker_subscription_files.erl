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
%% Three files per client, all written by the same atomic sequence -- temp file,
%% `file:sync', rename over the target:
%%
%%  * `{clientid}.subs'      -- the subscription set, **fsynced on every
%%    change** (R1b zero tolerance). Also carries each subscription's init
%%    cursors (K5): the subscribe-time tails travel with the record that is
%%    already paying for a synchronous write, so a subscription's origin is
%%    durable without a second fsync anywhere.
%%  * `{clientid}.cursors'   -- the whole per-(SubRef, PartitionId) cursor map
%%    of the client in one file (K4). One rename is an atomic update for *all*
%%    of a subscription's partitions, which separate per-partition files cannot
%%    offer, and a flush tick costs one write instead of one per partition.
%%  * `{clientid}.inflight'  -- the opaque session blob handed over by
%%    `inflight/2' (§5.3 tier 3, R7: may regress, never inspected here).
%%
%% Client ids are arbitrary MQTT binaries (slashes are legal), so filenames use
%% the url-safe base64 of the id and the real id lives inside the file, which is
%% what `list_sessions/0' returns -- the name is an address, the content is the
%% truth. A torn file is structurally impossible: a rename lands old or new.
-module(broker_subscription_files).

-include_lib("broker_core/include/broker_message.hrl").

-export([data_dir/0, sessions_dir/0,
         subs_path/1, cursors_path/1, inflight_path/1,
         write_subs/2, write_cursors/2, write_inflight/2,
         read_subs/1, read_cursors/1, read_inflight/1,
         delete_client/1, list_sessions/0]).

-type subs_body()     :: [{Filter :: binary(), QoS :: 0..1,
                           [{partition_id(), offset()}]}].
-type cursors_body()  :: [{Filter :: binary(), [{partition_id(), offset()}]}].

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

-spec subs_path(binary()) -> string().
subs_path(ClientId) ->
    client_path(ClientId, ".subs").

-spec cursors_path(binary()) -> string().
cursors_path(ClientId) ->
    client_path(ClientId, ".cursors").

-spec inflight_path(binary()) -> string().
inflight_path(ClientId) ->
    client_path(ClientId, ".inflight").

client_path(ClientId, Ext) ->
    %% base64url (`urlsafe' alphabet) without padding: filename-safe and no
    %% '=' trailing bytes, which some filesystems and tools treat as odd in a
    %% name.
    Name = binary_to_list(base64:encode(ClientId, #{mode => urlsafe,
                                                    padding => false})),
    filename:join(sessions_dir(), Name ++ Ext).

%%%_* Writes ---------------------------------------------------------------

%% @doc The subscription set. The caller is `subscribe/3' or `unsubscribe/2'
%% and treats an error here as fatal: answering ok while `.subs' did not reach
%% the disk is exactly the R1b failure mode.
-spec write_subs(binary(), subs_body()) -> ok | {error, term()}.
write_subs(ClientId, Entries) ->
    write_fsynced(subs_path(ClientId), encode(ClientId, Entries)).

%% @doc The whole cursor map of one client. Batched (§5.3 tier 2), so the
%% regression window after a kill is "advances since the last write" -- and one
%% file means one window shared by every (SubRef, PartitionId), never a mix of
%% per-partition snapshots taken at different instants.
-spec write_cursors(binary(), cursors_body()) -> ok | {error, term()}.
write_cursors(ClientId, Entries) ->
    write_fsynced(cursors_path(ClientId), encode(ClientId, Entries)).

%% @doc Opaque session blob (R7: broker never inspects it, only replays the
%% last one that landed).
-spec write_inflight(binary(), term()) -> ok | {error, term()}.
write_inflight(ClientId, Term) ->
    write_fsynced(inflight_path(ClientId), encode(ClientId, Term)).

%%%_* Reads ----------------------------------------------------------------

-spec read_subs(binary()) -> {ok, subs_body() | none} | {error, term()}.
read_subs(ClientId) ->
    read(subs_path(ClientId)).

-spec read_cursors(binary()) -> {ok, cursors_body() | none} | {error, term()}.
read_cursors(ClientId) ->
    read(cursors_path(ClientId)).

-spec read_inflight(binary()) -> {ok, term() | none} | {error, term()}.
read_inflight(ClientId) ->
    read(inflight_path(ClientId)).

%% @doc Boot step 2's input (design §6): every persisted subscription set.
%% Only `.subs' files are listed -- the set is authoritative (§10.6), cursor
%% entries for filters it does not know are dropped by the caller. A corrupt
%% file is an error, not a skip: the alternative is to boot a client whose
%% subscriptions silently vanished, which is the silent-empty-partition lie
%% ADR-011 §11.2 exists to refuse.
-spec list_sessions() -> {ok, [{binary(), subs_body()}]} | {error, term()}.
list_sessions() ->
    Paths = filelib:wildcard(filename:join(sessions_dir(), "*.subs")),
    list_sessions(Paths, []).

list_sessions([], Acc) ->
    {ok, lists:reverse(Acc)};
list_sessions([Path | Rest], Acc) ->
    case read_file_subs(Path) of
        {ok, ClientId, Entries} ->
            list_sessions(Rest, [{ClientId, Entries} | Acc]);
        {error, Reason} ->
            {error, Reason}
    end.

%%%_* Cleanup --------------------------------------------------------------

%% @doc Remove all three files of a client (clean_session teardown, §4.2).
%% Missing files are success: the teardown of a never-persisted session is a
%% no-op, not an error the caller can act on.
-spec delete_client(binary()) -> ok.
delete_client(ClientId) ->
    lists:foreach(
      fun(Path) ->
              case file:delete(Path) of
                  ok -> ok;
                  {error, enoent} -> ok
              end
      end,
      [subs_path(ClientId), cursors_path(ClientId), inflight_path(ClientId)]).

%%%_* Internal -------------------------------------------------------------

read_file_subs(Path) ->
    %% The client id inside the file is returned with the body: it is the
    %% truth, the filename only located the candidate.
    case file:read_file(Path) of
        {ok, Bin} ->
            case decode(Bin, Path) of
                {ok, ClientId, Body} -> {ok, ClientId, Body};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, {read_failed, Path, Reason}}
    end.

read(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            case decode(Bin, Path) of
                {ok, _ClientId, Body} -> {ok, Body};
                {error, Reason} -> {error, Reason}
            end;
        %% Absent is not corrupt: a client that never flushed a cursor tick has
        %% no `.cursors', and boot falls back to the origins inside `.subs' (K5).
        {error, enoent} ->
            {ok, none};
        {error, Reason} ->
            {error, {read_failed, Path, Reason}}
    end.

encode(ClientId, Body) ->
    term_to_binary({vsn, 1, ClientId, Body}).

decode(Bin, Path) ->
    try binary_to_term(Bin, [safe]) of
        {vsn, 1, ClientId, Body} -> {ok, ClientId, Body};
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
