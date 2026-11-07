%% @doc The retained-message snapshot file, and nothing else (ADR-018 §18.2).
%%
%% Format: `term_to_binary({vsn, 1, [{Topic, #broker_message{}}]})' at
%% `{data_dir}/retain/retain.v1'. The write is the same temp-file + `file:sync'
%% + rename sequence the session files use, so a reader sees the whole old file
%% or the whole new one -- never a half-written tree.
%%
%% `parse/1' is exposed for the suite: a corrupt or unknown-vsn snapshot must be
%% a boot blocker with the path in the error, because booting with an empty tree
%% would silently tell every new subscriber "nothing was ever retained".
-module(broker_retain_files).

-include_lib("broker_core/include/broker_message.hrl").

-export([data_dir/0, retain_dir/0, path/0, write/1, read/0, parse/1]).

-type entries() :: [{binary(), #broker_message{}}].
-export_type([entries/0]).

-spec data_dir() -> file:filename_all().
data_dir() ->
    application:get_env(broker_retain, data_dir, "data").

-spec retain_dir() -> file:filename_all().
retain_dir() ->
    filename:join(data_dir(), "retain").

-spec path() -> file:filename_all().
path() ->
    filename:join(retain_dir(), "retain.v1").

%% @doc Whole-tree replace. The caller (`broker_retain') has already done every
%% check it wants to do; this is pure IO.
-spec write(entries()) -> ok | {error, term()}.
write(Entries) ->
    Path = path(),
    Bin = term_to_binary({vsn, 1, Entries}),
    Tmp = Path ++ ".tmp",
    ok = filelib:ensure_dir(Tmp),
    case file:open(Tmp, [write, raw]) of
        {ok, Fd} -> write_sync_close(Path, Tmp, Fd, Bin);
        {error, Reason} -> {error, {open, Tmp, Reason}}
    end.

write_sync_close(Path, Tmp, Fd, Bin) ->
    case file:write(Fd, Bin) of
        ok ->
            case file:sync(Fd) of
                ok ->
                    ok = file:close(Fd),
                    case file:rename(Tmp, Path) of
                        ok -> ok;
                        {error, Reason} -> {error, {rename, Path, Reason}}
                    end;
                {error, Reason} ->
                    _ = file:close(Fd),
                    {error, {sync, Tmp, Reason}}
            end;
        {error, Reason} ->
            _ = file:close(Fd),
            {error, {write, Tmp, Reason}}
    end.

%% @doc Absent is an empty tree (a broker that never retained anything); a
%% present-but-unreadable file is an error.
-spec read() -> {ok, entries()} | {error, term()}.
read() ->
    case file:read_file(path()) of
        {ok, Bin} -> parse(Bin);
        {error, enoent} -> {ok, []};
        {error, Reason} -> {error, {read_failed, path(), Reason}}
    end.

%% @doc `[]' (not `[safe]'): the body carries the broker's own `#broker_message{}'
%% with its atom fields, exactly like the `.inflight' blob. Integrity comes from
%% the atomic rename and the trusted local data dir (design §13 M10/M11).
-spec parse(binary()) -> {ok, entries()} | {error, term()}.
parse(Bin) ->
    try binary_to_term(Bin) of
        {vsn, 1, Entries} when is_list(Entries) -> {ok, Entries};
        {vsn, V, _} when is_integer(V) -> {error, {unsupported_vsn, V, path()}};
        Other -> {error, {corrupt_retain_file, path(), Other}}
    catch
        error:_ -> {error, {corrupt_retain_file, path()}}
    end.
