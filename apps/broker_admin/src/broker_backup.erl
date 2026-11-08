%% @doc Backup and restore of the broker's on-disk state (S6, roadmap §8.2).
%%
%% One backup is a gzipped tar of the two directories whose loss would lose
%% data the broker promised to keep (ADR-011, ADR-019):
%%
%%   * `<data_dir>/wal/<partition>' -- the append-only logs, their index and
%%     per-partition meta;
%%   * `<data_dir>/sessions' -- `.subs' / `.cursors' / `.inflight' files.
%%
%% The backup is **hot**: it reads files while the broker runs. A WAL segment
%% being appended to at the same moment is copied as-is, which is safe because a
%% restore replays through `wal_recovery' (partial tail = torn tail, the same
%% path an unclean shutdown takes). The runbook still recommends a quiesce for
%% backups that must be byte-exact.
%%
%% Restore is deliberately explicit and refuses to write anywhere but a target
%% directory the caller names -- there is no `restore/0' that guesses, because
%% restoring over a live data dir silently is how an operator loses the data
%% they meant to keep.
-module(broker_backup).

-export([create/0, create/1, list/0, restore/2, backup_dir/0]).

-include_lib("kernel/include/file.hrl").

%% @doc Create a backup under the configured `broker_admin.backup_dir'.
-spec create() -> {ok, file:filename_all()} | {error, term()}.
create() ->
    create(default_label()).

%% @doc Create a backup with an explicit label (appended to the timestamp).
-spec create(string() | binary()) -> {ok, file:filename_all()} | {error, term()}.
create(Label) ->
    DataDir = env(broker_storage, data_dir, "data"),
    Dir = backup_dir(),
    Name = filename:join(Dir, "unified-mq-" ++ label(Label) ++ ".tar.gz"),
    case filelib:ensure_dir(Name) of
        ok ->
            Files = data_paths(DataDir),
            case write_archive(Name, DataDir, Files) of
                ok -> {ok, Name};
                {error, Reason} -> {error, {backup_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {backup_dir_unavailable, Dir, Reason}}
    end.

%% @doc Every backup file in the backup dir, newest first.
-spec list() -> [#{name := file:filename_all(),
                   bytes := non_neg_integer(),
                   mtime := non_neg_integer()}].
list() ->
    Dir = backup_dir(),
    Names = filelib:wildcard(filename:join(Dir, "unified-mq-*.tar.gz")),
    Entries =
        lists:foldl(
          fun(Path, Acc) ->
                  case file:read_file_info(Path) of
                      {ok, #file_info{size = Size, mtime = MTime}} ->
                          [#{name => Path, bytes => Size,
                             mtime => calendar:datetime_to_gregorian_seconds(MTime)}
                           | Acc];
                      {error, _} ->
                          Acc
                  end
          end, [], Names),
    lists:reverse(
      lists:sort(fun(A, B) -> maps:get(mtime, A) =< maps:get(mtime, B) end,
                 Entries)).

%% @doc Extract a backup into `TargetDir'. The directory must be empty or
%% missing; `restore' never merges into existing data. Returns `{ok, TargetDir}'
%% once the archive is fully extracted.
-spec restore(file:filename_all(), file:filename_all()) ->
          {ok, file:filename_all()} | {error, term()}.
restore(Archive, TargetDir) ->
    case ensure_empty(TargetDir) of
        ok ->
            case filelib:is_regular(Archive) of
                false ->
                    {error, {no_such_backup, Archive}};
                true ->
                    case erl_tar:extract(Archive, [compressed, {cwd, TargetDir}]) of
                        ok -> {ok, TargetDir};
                        {error, Reason} -> {error, {restore_failed, Reason}}
                    end
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc The configured backup directory (absolute or relative to the node CWD).
-spec backup_dir() -> file:filename_all().
backup_dir() ->
    env(broker_admin, backup_dir, "backups").

%%%_* Internal --------------------------------------------------------------

write_archive(Name, DataDir, Files) ->
    case erl_tar:open(Name, [write, compressed]) of
        {ok, Tar} ->
            try
                ok = add_each(Tar, DataDir, Files),
                erl_tar:close(Tar)
            catch
                Class:Reason ->
                    _ = erl_tar:close(Tar),
                    {error, {Class, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

add_each(_Tar, _DataDir, []) ->
    ok;
add_each(Tar, DataDir, [Rel | Rest]) ->
    Full = filename:join(DataDir, Rel),
    case erl_tar:add(Tar, {Rel, Full}, []) of
        ok -> add_each(Tar, DataDir, Rest);
        {error, Reason} -> erlang:error(Reason)
    end.

data_paths(DataDir) ->
    case filelib:is_dir(DataDir) of
        false -> [];
        true -> relative_files(DataDir, DataDir)
    end.

relative_files(Root, Dir) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            lists:flatmap(
              fun(Name) ->
                      Path = filename:join(Dir, Name),
                      case file:read_file_info(Path) of
                          {ok, #file_info{type = directory}} ->
                              relative_files(Root, Path);
                          {ok, #file_info{type = regular}} ->
                              [filename:join(relative_to(Root, Dir), Name)];
                          _ ->
                              []
                      end
              end, Names);
        {error, _} ->
            []
    end.

relative_to(Root, Dir) ->
    case Dir =:= Root of
        true -> ".";
        false -> string:prefix(Dir, Root ++ "/")
    end.

ensure_empty(TargetDir) ->
    case filelib:is_dir(TargetDir) of
        false ->
            ok;
        true ->
            case file:list_dir(TargetDir) of
                {ok, []} -> ok;
                {ok, _} -> {error, {target_not_empty, TargetDir}};
                {error, Reason} -> {error, {target_unreadable, TargetDir, Reason}}
            end
    end.

default_label() ->
    {{Y, Mo, D}, {H, Mi, S}} = calendar:universal_time(),
    io_lib:format("~4..0w~2..0w~2..0w-~2..0w~2..0w~2..0w",
                  [Y, Mo, D, H, Mi, S]).

label(B) when is_binary(B) -> binary_to_list(B);
label(L) when is_list(L) -> L.

env(App, Key, Default) ->
    application:get_env(App, Key, Default).
