%% @doc Group-commit policy: when a batch flushes and whether it owes an fsync
%% (design §7.2, ADR-011 §11.4).
%%
%% This is a pure decision module, not a second gen_server -- a deviation from
%% design §7.2's module list, which drew it as a peer of `wal_writer'. The queue,
%% the file descriptors and the callers' `From's all live in the writer, so a
%% separate process would need to be told about every arrival and would still
%% have to hand the batch back. What is actually shared is four arithmetic rules,
%% and those are testable without a disk only if they are functions.
%%
%% The rules, and where each number comes from:
%%
%%  * **batch_size 32.** S0.3 §2.2 measured 1,148 msg/s for a per-message fsync
%%    and 17,155 msg/s at batch 32 -- a 17x jump for one millisecond of added
%%    latency. 64 buys another 16% and is not worth the latency in S1.
%%  * **batch_delay_ms #{os_buffer => 0, fsync => 1}.** A caller that accepted
%%    `os_buffer' asked for the page cache and must not be made to wait for a
%%    batch it does not need; a caller that asked for `fsync' already accepted a
%%    disk round trip, and 1 ms of it is the batching window.
%%  * **max_batch_size 256.** One flush is one turn of the writer's message loop,
%%    so an unbounded batch would keep new callers from even enqueueing. Capping
%%    it costs one extra fsync per 256 records under a backlog and buys fairness.
%%  * **a batch owes one fsync if any request in it asked for one.** Replying to
%%    the `os_buffer' callers after that sync is stronger than they were promised,
%%    so splitting the batch to avoid it would buy nothing.
%%
%% `infinity' is the deadline of an empty queue. Term ordering puts every integer
%% below every atom, so `Now >= infinity' is false and `erlang:min/2' keeps the
%% real deadline -- wal_group_committer_SUITE pins both uses.
-module(wal_group_committer).

-include_lib("broker_core/include/broker_message.hrl").

-export([new/1, deadline/3, due/4, timeout/2, needs_sync/1, take/2, earliest/1]).

-define(DEFAULT_BATCH_SIZE, 32).
-define(DEFAULT_MAX_BATCH_SIZE, 256).
-define(DEFAULT_BATCH_DELAY_MS, #{os_buffer => 0, fsync => 1}).

-type policy() :: #{batch_size := pos_integer(),
                    max_batch_size := pos_integer(),
                    batch_delay_ms := #{sync_mode() => non_neg_integer()}}.
%% Milliseconds on the monotonic clock, or `infinity' for an empty queue.
-type deadline() :: integer() | infinity.
-type wait_ms() :: non_neg_integer() | infinity.

-export_type([policy/0, deadline/0]).

%% A partial `batch_delay_ms' is not merged with the default: a mode that is not
%% listed gets delay 0, i.e. it is written through at once. Waiting for a
%% deadline nobody configured would be the worse failure.
-spec new(map()) -> policy().
new(Config) when is_map(Config) ->
    #{batch_size => maps:get(batch_size, Config, ?DEFAULT_BATCH_SIZE),
      max_batch_size => maps:get(max_batch_size, Config, ?DEFAULT_MAX_BATCH_SIZE),
      batch_delay_ms => maps:get(batch_delay_ms, Config, ?DEFAULT_BATCH_DELAY_MS)}.

-spec deadline(policy(), sync_mode(), ArrivedAt :: integer()) -> deadline().
deadline(#{batch_delay_ms := Delays}, Mode, ArrivedAt) ->
    ArrivedAt + maps:get(Mode, Delays, 0).

-spec due(policy(), PendingCount :: non_neg_integer(), deadline(), Now :: integer()) ->
    boolean().
due(#{batch_size := BatchSize}, PendingCount, MinDeadline, Now) ->
    PendingCount >= BatchSize orelse Now >= MinDeadline.

-spec timeout(deadline(), Now :: integer()) -> wait_ms().
timeout(infinity, _Now) ->
    infinity;
timeout(MinDeadline, Now) ->
    max(0, MinDeadline - Now).

%% Anything that is not `os_buffer' means "the caller wants it on the medium",
%% which for S1 is exactly one `file:sync'. `replicated_commit' is in no S1
%% backend's capability, so it never reaches a batch.
-spec needs_sync([sync_mode()]) -> boolean().
needs_sync(Modes) ->
    lists:any(fun(Mode) -> Mode =/= os_buffer end, Modes).

-spec take(policy(), Pending) -> {Batch :: Pending, Rest :: Pending}.
take(#{max_batch_size := Max}, Pending) ->
    lists:split(min(Max, length(Pending)), Pending).

-spec earliest([deadline()]) -> deadline().
earliest([]) -> infinity;
earliest(Deadlines) -> lists:min(Deadlines).
