%% @doc The single writer process for one WAL partition: group commit, rotation,
%% watermarks and the durability guard (design §7.2, ADR-011 §11.3/§11.4/§11.5).
%%
%% One process per partition is a deliberate bottleneck, and a bounded one. It is
%% the only way `file:sync' can be amortised across callers -- a queue of 32
%% fsync requests answered by one sync instead of 32 is the 17x S0.3 §2.2
%% measured -- and it is what makes "exactly one writer per file" true, which
%% `wal_segment' relies on to be able to cut a torn tail. What the process must
%% NOT do is the work it can hand back: records are encoded by `wal_codec' in the
%% **caller's** process (the on-disk body carries no offset, so nothing about the
%% encoding depends on what the writer knows), and reads never come here at all --
%% they go straight to the public `wal_index' and the segment files.
%%
%% Offsets are allocated in `handle_call', before the caller is made to wait for
%% the flush. That is what keeps them dense and unique under concurrency even
%% though the durability of a batch is settled later, and it is why a rejected
%% request consumes nothing: the check happens before the counter moves.
%%
%% Two ordering facts are load-bearing and both are safe because of ADR-011
%% §11.3's rule that the watermark may come back low:
%%
%%  * `advance_committed/2' can be served while appends are still queued, so
%%    meta.log may claim a watermark whose records are not on disk yet. A crash
%%    there is clamped back by `wal_recovery', never the other way round.
%%  * a write or sync failure is fatal rather than retryable. The offsets of that
%%    batch were already handed out, and a partial batch would leave the segment
%%    holding fewer records than the callers were told -- renumbering everything
%%    after it at the next recovery. So the batch is answered with the error and
%%    the process stops, letting the supervisor rebuild the state from the disk.
%%
%% The durability guard replaces S0.3 §4's timing heuristic ("refuse to start if
%% an fsync is faster than 100 µs"). A fast NVMe with power-loss protection would
%% fail that test and refuse to boot a healthy machine, so this reads the mount
%% table instead and refuses only the filesystems that are volatile by
%% definition. Everything it cannot classify is allowed: the guard is a safety
%% net for a tmpfs `data_dir', not a portability gate.
-module(wal_writer).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/3, stop/1, capability/0]).
-export([append/3, advance_committed/2, truncate/2, close/1, open/1,
         state/1, stats/1]).
-export([check_filesystem/2]).
%% Exported for wal_writer_SUITE: the guard is only testable if the mount table
%% is an argument rather than a file the test has to fake on the host.
-export([classify_fs/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% ADR-011 §11.4: the WAL backend's honest capability. `replicated_commit' needs
%% a consensus group, which is S3.
-define(CAPABILITY, [os_buffer, fsync]).
-define(DEFAULT_SEGMENT_MAX_RECORDS, 100000).
-define(DEFAULT_ANCHOR_INTERVAL, 256).
-define(DEFAULT_MOUNTS, "/proc/mounts").
%% Only the in-memory filesystems are refused. ext4/xfs/btrfs/zfs/overlay are
%% taken at their word: a false refusal is a node that will not boot.
-define(VOLATILE_FS_TYPES, ["tmpfs", "ramfs"]).
%% Long enough for a batch window plus an fsync on a disk that is having a bad
%% day, short enough that a hung disk does not pin a publisher forever.
-define(APPEND_TIMEOUT, 30000).

-record(seg, {path         :: file:filename_all(),
              fd           :: file:io_device(),
              start_offset :: non_neg_integer(),
              records      :: non_neg_integer(),
              end_pos      :: non_neg_integer()}).

%% A queued append. `bins' rather than messages: encoding already happened in
%% the caller's process.
-record(req, {from     :: gen_server:from(),
              offsets  :: [offset()],
              bins     :: [binary()],
              mode     :: sync_mode(),
              deadline :: wal_group_committer:deadline()}).

-record(st, {id              :: partition_id(),
             dir             :: file:filename_all(),
             index           :: ets:tid(),
             meta_fd         :: file:io_device(),
             policy          :: wal_group_committer:policy(),
             interval        :: pos_integer(),
             seg_max         :: pos_integer(),
             seg             :: #seg{},
             high            :: offset(),
             committed       :: offset(),
             truncate_before :: offset(),
             pending         :: [#req{}],
             min_deadline    :: wal_group_committer:deadline(),
             timer           :: reference() | undefined,
             warnings        :: [term()],
             stats           :: map(),
             closed = false  :: boolean()}).

-type stats() :: #{appends := non_neg_integer(),
                   records := non_neg_integer(),
                   batches := non_neg_integer(),
                   syncs := non_neg_integer(),
                   rotations := non_neg_integer()}.

-export_type([stats/0]).

%%%_* API ----------------------------------------------------------------

%% The filesystem guard runs here rather than in `init/1'. It is a pure
%% precondition with no state, and a `data_dir' that points at a tmpfs is a
%% configuration error, not a crash: refusing it before spawning keeps it out of
%% the log as an `{error, Reason}' instead of a crash report that would bury the
%% failures which do matter.
-spec start_link(partition_id(), file:filename_all(), map()) ->
    {ok, pid()} | {error, term()}.
start_link(PartitionId, Dir, Config) when is_map(Config) ->
    case check_filesystem(Dir, Config) of
        {ok, FsWarnings} ->
            gen_server:start_link(?MODULE, {PartitionId, Dir, Config, FsWarnings}, []);
        {error, Reason} ->
            {error, Reason}
    end.

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid, normal, 5000).

-spec capability() -> [sync_mode()].
capability() ->
    ?CAPABILITY.

%% Encodes in the caller's process: the on-disk body carries no offset and no
%% partition id, so nothing here depends on state only the writer has.
-spec append(pid(), [#broker_message{}], sync_mode()) -> {ok, [offset()]} | {error, term()}.
append(Pid, Msgs, Mode) ->
    Bins = [wal_codec:encode(Msg) || Msg <- Msgs],
    gen_server:call(Pid, {append, Bins, Mode}, ?APPEND_TIMEOUT).

-spec advance_committed(pid(), offset()) -> ok | {error, term()}.
advance_committed(Pid, Offset) ->
    gen_server:call(Pid, {advance_committed, Offset}).

-spec truncate(pid(), offset()) -> ok | {error, term()}.
truncate(Pid, BeforeOffset) ->
    gen_server:call(Pid, {truncate, BeforeOffset}).

%% Flushes and marks the partition closed, but stays alive: a stale handle then
%% gets `{error, {closed, Id}}' instead of a `noproc' exit. The index table dies
%% with this process, so closing it is also what makes `read_from' refuse.
-spec close(pid()) -> ok | {error, term()}.
close(Pid) ->
    gen_server:call(Pid, close).

%% The inverse of `close/1', for ADR-011 §11.2's rule that `recover/2' may be
%% called again on a partition that was closed: the descriptors and the offset
%% sequence are still here, so reopening is a flag flip rather than a rescan.
%% Idempotent, because a supervisor restart has already run the rescan and left
%% the partition open -- `recover/2' should not have to tell those apart.
-spec open(pid()) -> {ok, map()} | {error, term()}.
open(Pid) ->
    gen_server:call(Pid, open).

-spec state(pid()) -> map().
state(Pid) ->
    gen_server:call(Pid, state).

%% `syncs' and `batches' are not vanity counters: they are the only way to
%% observe that a batch of fsync callers paid for one sync, which the return
%% value of append/3 cannot show.
-spec stats(pid()) -> stats().
stats(Pid) ->
    gen_server:call(Pid, stats).

%%%_* gen_server ---------------------------------------------------------

init({PartitionId, Dir, Config, FsWarnings}) ->
    case start_up(PartitionId, Dir, Config, FsWarnings) of
        {ok, State} -> {ok, State};
        {stop, Reason} -> {stop, Reason}
    end.

handle_call({append, _Bins, _Mode}, _From, #st{closed = true, id = Id} = State) ->
    {reply, {error, {closed, Id}}, State};
handle_call({append, Bins, Mode}, From, #st{high = High, policy = Policy} = State) ->
    case lists:member(Mode, ?CAPABILITY) of
        %% §11.4: a mode outside capability is an error, never a quieter write.
        %% Checked before the counter moves, so a rejected append consumes no
        %% offset and the next caller still gets the number it was promised.
        false ->
            {reply, {error, {unsupported_sync_mode, Mode}}, State};
        true ->
            N = length(Bins),
            Now = now_ms(),
            Req = #req{from = From,
                       offsets = lists:seq(High, High + N - 1),
                       bins = Bins,
                       mode = Mode,
                       deadline = wal_group_committer:deadline(Policy, Mode, Now)},
            State1 = State#st{high = High + N,
                              pending = State#st.pending ++ [Req],
                              min_deadline = erlang:min(State#st.min_deadline,
                                                        Req#req.deadline),
                              stats = bump(appends, State#st.stats)},
            case service(State1) of
                {ok, State2} -> {noreply, State2};
                {stop, Reason, State2} -> {stop, Reason, State2}
            end
    end;

handle_call({advance_committed, _Offset}, _From, #st{closed = true, id = Id} = State) ->
    {reply, {error, {closed, Id}}, State};
handle_call({advance_committed, Offset}, _From,
            #st{high = High, committed = Committed, id = Id} = State) ->
    case Offset =< High - 1 andalso Offset >= Committed of
        true ->
            State1 = State#st{committed = Offset},
            case persist_watermarks(State1) of
                ok -> {reply, ok, State1};
                {error, Reason} -> {stop, Reason, {error, Reason}, State}
            end;
        false ->
            {reply, {error, {invalid_committed_offset, Id, {Committed, Offset}}}, State}
    end;

handle_call({truncate, _Before}, _From, #st{closed = true, id = Id} = State) ->
    {reply, {error, {closed, Id}}, State};
handle_call({truncate, Before}, _From,
            #st{committed = Committed, truncate_before = Trunc, id = Id} = State) ->
    case Before >= Trunc andalso Before =< Committed + 1 of
        true ->
            true = wal_index:set_trunc(State#st.index, Before),
            State1 = State#st{truncate_before = Before},
            case persist_watermarks(State1) of
                ok -> {reply, ok, State1};
                {error, Reason} -> {stop, Reason, {error, Reason}, State}
            end;
        false ->
            {reply, {error, {invalid_truncate, Id, {Trunc, Committed, Before}}}, State}
    end;

handle_call(close, _From, #st{closed = true} = State) ->
    {reply, ok, State};
handle_call(close, _From, State) ->
    case drain(State) of
        {ok, State1} ->
            true = wal_index:set_open(State1#st.index, false),
            State2 = State1#st{closed = true},
            #st{seg = #seg{fd = Fd}} = State1,
            _ = sync_fd_measured(Fd),
            {reply, ok, State2};
        {stop, Reason, State1} ->
            {stop, Reason, {error, Reason}, State1}
    end;

handle_call(open, _From, #st{index = Index} = State) ->
    true = wal_index:set_open(Index, true),
    State1 = State#st{closed = false},
    {reply, {ok, state_map(State1)}, State1};

handle_call(state, _From, #st{} = State) ->
    {reply, state_map(State), State};

handle_call(stats, _From, #st{stats = Stats} = State) ->
    {reply, Stats, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(flush, State) ->
    case service(State) of
        {ok, State1} -> {noreply, State1};
        {stop, Reason, State1} -> {stop, Reason, State1}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%% Nobody is left waiting on a promise this process can no longer keep, so a
%% pending batch is answered with the error rather than dropped on the floor --
%% a caller blocked in append/3 would otherwise see a bare exit and have no way
%% to tell "not written" from "written but unanswered".
terminate(Reason, #st{pending = Pending} = State) ->
    lists:foreach(fun(#req{from = From}) ->
                          gen_server:reply(From, {error, {writer_stopped, Reason}})
                  end, Pending),
    _ = sync_fd(State),
    _ = wal_segment:close(State#st.seg#seg.fd),
    _ = wal_meta:close(State#st.meta_fd),
    ok.

%%%_* Startup ------------------------------------------------------------

start_up(PartitionId, Dir, Config, FsWarnings) ->
    case wal_recovery:recover(PartitionId, Dir, Config) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Recovered} ->
            open_partition(PartitionId, Dir, Config, Recovered, FsWarnings)
    end.

open_partition(PartitionId, Dir, Config, Recovered, FsWarnings) ->
    #{index := Index, high := High, committed := Committed,
      truncate_before := Trunc, current := Current, warnings := Warnings} = Recovered,
    #{path := Path, start_offset := StartOffset, records := Records,
      end_pos := EndPos} = Current,
    case wal_segment:open_append(Path) of
        {error, Reason} ->
            {stop, Reason};
        {ok, SegFd} ->
            case wal_meta:open_append(Dir) of
                {error, Reason} ->
                    _ = wal_segment:close(SegFd),
                    {stop, {meta_open_failed, Dir, Reason}};
                {ok, MetaFd} ->
                    {ok, #st{id = PartitionId,
                             dir = Dir,
                             index = Index,
                             meta_fd = MetaFd,
                             policy = wal_group_committer:new(Config),
                             interval = maps:get(index_anchor_interval, Config,
                                                 ?DEFAULT_ANCHOR_INTERVAL),
                             seg_max = maps:get(segment_max_records, Config,
                                                ?DEFAULT_SEGMENT_MAX_RECORDS),
                             seg = #seg{path = Path,
                                        fd = SegFd,
                                        start_offset = StartOffset,
                                        records = Records,
                                        end_pos = EndPos},
                             high = High,
                             committed = Committed,
                             truncate_before = Trunc,
                             pending = [],
                             min_deadline = infinity,
                             timer = undefined,
                             warnings = Warnings ++ FsWarnings,
                             stats = #{appends => 0, records => 0, batches => 0,
                                       syncs => 0, rotations => 0}}}
            end
    end.

%%%_* The flush loop -----------------------------------------------------

%% Serve the queue: flush while anything is due, otherwise arm the one timer
%% that will wake us for the earliest deadline.
service(#st{pending = []} = State) ->
    {ok, State};
service(#st{pending = Pending, min_deadline = MinDeadline, policy = Policy} = State) ->
    Now = now_ms(),
    case wal_group_committer:due(Policy, length(Pending), MinDeadline, Now) of
        true ->
            case flush(State) of
                {ok, State1} -> service(State1);
                {stop, _, _} = Stop -> Stop
            end;
        false ->
            {ok, arm_timer(State, wal_group_committer:timeout(MinDeadline, Now))}
    end.

%% Unconditional: `close' must not leave a caller waiting on a deadline that is
%% still 30 s away.
drain(#st{pending = []} = State) ->
    {ok, State};
drain(State) ->
    case flush(State) of
        {ok, State1} -> drain(State1);
        {stop, _, _} = Stop -> Stop
    end.

flush(#st{pending = Pending, policy = Policy, timer = Timer} = State) ->
    {Batch, Rest} = wal_group_committer:take(Policy, Pending),
    _ = cancel_timer(Timer),
    OweSync = wal_group_committer:needs_sync([Mode || #req{mode = Mode} <- Batch]),
    Records = [{Offset, Bin} || #req{offsets = Offsets, bins = Bins} <- Batch,
                                {Offset, Bin} <- lists:zip(Offsets, Bins)],
    State0 = State#st{pending = Rest,
                      min_deadline = wal_group_committer:earliest(
                                       [D || #req{deadline = D} <- Rest]),
                      timer = undefined,
                      stats = bump(batches, State#st.stats)},
    case write_records(Records, State0, OweSync) of
        {ok, State1} ->
            %% The reply is the durability promise, so it goes out only after the
            %% writes -- and, for a batch that owes one, after the sync.
            lists:foreach(fun(#req{from = From, offsets = Offsets}) ->
                                  gen_server:reply(From, {ok, Offsets})
                          end, Batch),
            {ok, State1};
        {error, Reason} ->
            lists:foreach(fun(#req{from = From}) ->
                                  gen_server:reply(From, {error, Reason})
                          end, Batch),
            {stop, Reason, State0}
    end.

write_records([], State, true) ->
    sync_current(State);
write_records([], State, false) ->
    {ok, State};
write_records([{Offset, Bin} | Rest], State, OweSync) ->
    case write_one(Offset, Bin, State, OweSync) of
        {ok, State1} -> write_records(Rest, State1, OweSync);
        {error, Reason} -> {error, Reason}
    end.

write_one(Offset, Bin, State, OweSync) ->
    case ensure_room(Offset, State, OweSync) of
        {ok, #st{seg = #seg{path = Path, fd = Fd, start_offset = Start,
                            records = Records, end_pos = Pos} = Seg} = State1} ->
            case wal_segment:append(Fd, Bin) of
                ok ->
                    %% Anchored at the position the record starts at, before the
                    %% append moves the file position.
                    maybe_anchor(Offset, Pos, Path, Start, State1),
                    EndPos = Pos + byte_size(Bin),
                    broker_metrics:inc(wal_append_total),
                    broker_metrics:gauge(segment_bytes, EndPos),
                    broker_metrics:gauge(queue_depth,
                                         max(0, State1#st.high
                                                - State1#st.committed)),
                    {ok, State1#st{seg = Seg#seg{records = Records + 1,
                                                 end_pos = EndPos},
                                   stats = bump(records, State1#st.stats)}};
                {error, Reason} ->
                    {error, {append_failed, Path, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% `wal_index:locate/2' can only answer for an offset if some anchor at or below
%% it names the right file, so the first record of every segment must be one --
%% which `(Offset - Start) rem Interval =:= 0' gives for free at Offset = Start.
maybe_anchor(Offset, Pos, Path, Start, #st{index = Index, interval = Interval}) ->
    case (Offset - Start) rem Interval of
        0 -> true = wal_index:add_anchor(Index, Offset, Path, Pos);
        _ -> ok
    end.

ensure_room(_Offset, #st{seg = #seg{records = Records}, seg_max = Max} = State, _OweSync)
        when Records < Max ->
    {ok, State};
%% Rotation in the middle of a batch. The outgoing segment owes this batch's
%% sync: closing it without one would leave an fsync caller's records in the
%% page cache of a file nobody holds a descriptor for any more.
ensure_room(Offset, #st{seg = #seg{fd = Fd, path = Path}, dir = Dir} = State, OweSync) ->
    case sync_before_close(Fd, OweSync, State) of
        {ok, State1} ->
            case wal_segment:close(Fd) of
                ok ->
                    open_segment(Offset, Dir, State1);
                {error, Reason} ->
                    {error, {close_failed, Path, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

sync_before_close(_Fd, false, State) ->
    {ok, State};
sync_before_close(Fd, true, State) ->
    case sync_fd_measured(Fd) of
        ok -> {ok, State#st{stats = bump(syncs, State#st.stats)}};
        {error, Reason} -> {error, {sync_failed, State#st.seg#seg.path, Reason}}
    end.

open_segment(Offset, Dir, State) ->
    Path = wal_segment:path(Dir, Offset),
    case wal_segment:open_append(Path) of
        {ok, Fd} ->
            {ok, State#st{seg = #seg{path = Path,
                                     fd = Fd,
                                     start_offset = Offset,
                                     records = 0,
                                     end_pos = 0},
                          stats = bump(rotations, State#st.stats)}};
        {error, Reason} ->
            {error, Reason}
    end.

sync_current(#st{seg = #seg{fd = Fd, path = Path}, stats = Stats} = State) ->
    case sync_fd_measured(Fd) of
        ok -> {ok, State#st{stats = bump(syncs, Stats)}};
        {error, Reason} -> {error, {sync_failed, Path, Reason}}
    end.

sync_fd(#st{seg = #seg{fd = Fd}}) ->
    wal_segment:sync(Fd).

%% One place to measure an fsync: the histogram is the gate's latency evidence
%% (ADR-016 §16.2) and the counter lets the report compare batches with syncs.
sync_fd_measured(Fd) ->
    T0 = erlang:monotonic_time(microsecond),
    case wal_segment:sync(Fd) of
        ok ->
            broker_metrics:observe(wal_sync_latency_us,
                                   erlang:monotonic_time(microsecond) - T0),
            broker_metrics:inc(wal_sync_total),
            ok;
        {error, _} = Error ->
            Error
    end.

%%%_* Watermarks ---------------------------------------------------------

%% Appended rather than rewritten, and synced before the caller is answered:
%% §11.3 allows the watermark to come back low after a crash, so a lost tail
%% entry costs duplicate deliveries, while an unsynced one that survived would
%% have cost the opposite. S1 does not compact this file (design §7.3 makes the
%% same trade for segments); that is retention work in S2.
persist_watermarks(#st{meta_fd = MetaFd, committed = Committed,
                       truncate_before = Trunc}) ->
    wal_meta:write(MetaFd, Committed, Trunc).

%%%_* Timers -------------------------------------------------------------

arm_timer(#st{timer = Old} = State, Timeout) ->
    _ = cancel_timer(Old),
    State#st{timer = erlang:send_after(Timeout, self(), flush)}.

cancel_timer(undefined) -> false;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%%%_* The volatile-filesystem guard --------------------------------------

%% Exported for `broker_storage_wal:create/2', which has to run this before it
%% writes the partition layout: refusing afterwards would leave a directory that
%% looks created and is not usable.
-spec check_filesystem(file:filename_all(), map()) -> {ok, [term()]} | {error, term()}.
check_filesystem(Dir, Config) ->
    Require = maps:get(require_durable_fs, Config, true),
    Mounts = maps:get(mounts_file, Config, ?DEFAULT_MOUNTS),
    %% absname because `data_dir' is relative in sys.config, and a relative path
    %% can never match a mount point.
    case file:read_file(Mounts) of
        {ok, Text} ->
            case classify_fs(filename:absname(Dir), Text) of
                volatile when Require -> {error, {volatile_filesystem, Dir}};
                volatile -> {ok, [{volatile_filesystem, Dir}]};
                _ -> {ok, []}
            end;
        {error, _Reason} ->
            %% No mount table means a non-Linux host. Cannot tell, so allow.
            {ok, [{no_mount_table, Mounts}]}
    end.

%% Longest matching mount point wins, matched on a path boundary so that `/ab'
%% is not reported as living inside a mount on `/a'.
%%
%% Mount points containing a space appear in /proc/mounts as `\040' and will not
%% match; the answer degrades to `unknown', which is allowed, so a pathological
%% path costs the guard rather than the boot.
-spec classify_fs(file:filename_all(), binary() | string()) ->
    durable | volatile | unknown.
classify_fs(Path, MountsText) ->
    case longest_mount(to_list(Path), parse_mounts(MountsText)) of
        {ok, FsType} ->
            case lists:member(FsType, ?VOLATILE_FS_TYPES) of
                true -> volatile;
                false -> durable
            end;
        none ->
            unknown
    end.

longest_mount(Path, Entries) ->
    case [{length(MountPoint), FsType}
          || {MountPoint, FsType} <- Entries, covers(MountPoint, Path)] of
        [] -> none;
        Matches -> {ok, element(2, lists:max(Matches))}
    end.

covers("/", _Path) ->
    true;
covers(MountPoint, Path) when Path =:= MountPoint ->
    true;
covers(MountPoint, Path) ->
    Length = length(MountPoint),
    length(Path) > Length
        andalso MountPoint =:= string:slice(Path, 0, Length)
        andalso "/" =:= string:slice(Path, Length, 1).

parse_mounts(Text) ->
    lists:foldl(fun(Line, Acc) ->
                        case string:lexemes(Line, " \t") of
                            [_Device, MountPoint, FsType | _] ->
                                [{MountPoint, FsType} | Acc];
                            _ ->
                                Acc
                        end
                end, [], string:split(to_list(Text), "\n", all)).

%%%_* Internal -----------------------------------------------------------

state_map(#st{} = State) ->
    #{id => State#st.id,
      dir => State#st.dir,
      index => State#st.index,
      high => State#st.high,
      committed => State#st.committed,
      truncate_before => State#st.truncate_before,
      closed => State#st.closed,
      warnings => State#st.warnings}.

now_ms() ->
    erlang:monotonic_time(millisecond).

bump(Key, Stats) ->
    maps:update_with(Key, fun(Count) -> Count + 1 end, 1, Stats).

to_list(Value) when is_binary(Value) -> binary_to_list(Value);
to_list(Value) when is_list(Value) -> Value.
