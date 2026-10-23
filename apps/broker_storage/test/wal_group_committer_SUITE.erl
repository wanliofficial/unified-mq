%% @doc T5: the group-commit policy of design §7.2, as pure decisions.
%%
%% The writer owns the queue and the file descriptors; this module owns only the
%% three questions that decide when a flush happens -- is the batch full, has the
%% earliest deadline passed, and does the batch owe an fsync. Keeping them apart
%% is what makes the batching rules testable without a disk, and it is why the
%% measured S0.3 numbers (batch 32 => 17x single-message fsync) can be pinned as
%% arithmetic here rather than as a throughput benchmark nobody can run in CI.
%%
%% `infinity' is the deadline of an empty queue. Term ordering puts every integer
%% below every atom, so `Now >= infinity' is false and `erlang:min/2' keeps the
%% real deadline: the two cases below that use `infinity' are the proof that the
%% trick still holds.
-module(wal_group_committer_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [defaults_are_the_measured_values_from_s0_3,
     a_configured_delay_applies_to_its_own_mode_only,
     an_os_buffer_request_is_due_the_moment_it_arrives,
     an_fsync_request_waits_its_delay_and_no_longer,
     a_full_batch_is_due_before_its_deadline,
     an_empty_queue_is_never_due_and_arms_no_timer,
     timeout_counts_down_to_the_earliest_deadline,
     a_deadline_already_passed_leaves_no_wait,
     needs_sync_is_true_when_any_request_in_the_batch_asks_for_it,
     a_batch_is_capped_so_a_backlog_cannot_starve_new_callers,
     the_earliest_of_a_queue_is_its_binding_deadline].

%%%_* Defaults -----------------------------------------------------------

%% Not "new/1 returns this map" -- the defaults are asserted through the
%% decisions they drive, so a representation change cannot make this pass.
defaults_are_the_measured_values_from_s0_3(_Config) ->
    P = wal_group_committer:new(#{}),
    %% batch_delay_ms: fsync waits 1 ms so a batch can form, os_buffer never does.
    1001 = wal_group_committer:deadline(P, fsync, 1000),
    1000 = wal_group_committer:deadline(P, os_buffer, 1000),
    %% batch_size 32 is the S0.3 §2.2 inflection point.
    false = wal_group_committer:due(P, 31, infinity, 0),
    true = wal_group_committer:due(P, 32, infinity, 0),
    %% max_batch_size 256 caps one flush.
    {Batch, Rest} = wal_group_committer:take(P, lists:seq(1, 300)),
    256 = length(Batch),
    44 = length(Rest),
    ok.

a_configured_delay_applies_to_its_own_mode_only(_Config) ->
    P = wal_group_committer:new(#{batch_delay_ms => #{fsync => 50}}),
    50 = wal_group_committer:deadline(P, fsync, 0),
    %% A mode with no configured delay is written through at once. Waiting an
    %% unbounded time for a deadline nobody set would be the worse failure.
    0 = wal_group_committer:deadline(P, os_buffer, 0),
    ok.

%%%_* When is a flush due ------------------------------------------------

an_os_buffer_request_is_due_the_moment_it_arrives(_Config) ->
    P = wal_group_committer:new(#{}),
    Deadline = wal_group_committer:deadline(P, os_buffer, 1000),
    true = wal_group_committer:due(P, 1, Deadline, 1000),
    ok.

an_fsync_request_waits_its_delay_and_no_longer(_Config) ->
    P = wal_group_committer:new(#{}),
    Deadline = wal_group_committer:deadline(P, fsync, 1000),
    false = wal_group_committer:due(P, 1, Deadline, 1000),
    true = wal_group_committer:due(P, 1, Deadline, 1001),
    %% A later batch is still due: the deadline is a floor, not a window.
    true = wal_group_committer:due(P, 7, Deadline, 5000),
    ok.

a_full_batch_is_due_before_its_deadline(_Config) ->
    P = wal_group_committer:new(#{batch_size => 4}),
    Deadline = wal_group_committer:deadline(P, fsync, 1000),
    false = wal_group_committer:due(P, 3, Deadline, 999),
    true = wal_group_committer:due(P, 4, Deadline, 999),
    ok.

%% The empty queue is the case a naive `Now >= Deadline' gets wrong: `infinity'
%% is greater than every timestamp, so the comparison is false by term ordering.
an_empty_queue_is_never_due_and_arms_no_timer(_Config) ->
    P = wal_group_committer:new(#{}),
    false = wal_group_committer:due(P, 0, infinity, 1 bsl 62),
    infinity = wal_group_committer:timeout(infinity, 0),
    ok.

timeout_counts_down_to_the_earliest_deadline(_Config) ->
    10 = wal_group_committer:timeout(1000, 990),
    1 = wal_group_committer:timeout(1000, 999),
    ok.

a_deadline_already_passed_leaves_no_wait(_Config) ->
    %% A negative send_after would be rejected by the BIF, and 0 is what "flush
    %% on the next turn of the message loop" means to a gen_server.
    0 = wal_group_committer:timeout(1000, 1000),
    0 = wal_group_committer:timeout(1000, 1005),
    ok.

%%%_* What a flush owes --------------------------------------------------

needs_sync_is_true_when_any_request_in_the_batch_asks_for_it(_Config) ->
    false = wal_group_committer:needs_sync([]),
    false = wal_group_committer:needs_sync([os_buffer, os_buffer]),
    true = wal_group_committer:needs_sync([fsync]),
    %% One fsync caller in a batch of os_buffer callers pays for the whole batch.
    %% That is stronger than any of them was promised, so the batch is not split.
    true = wal_group_committer:needs_sync([os_buffer, fsync, os_buffer]),
    true = wal_group_committer:needs_sync([replicated_commit]),
    ok.

a_batch_is_capped_so_a_backlog_cannot_starve_new_callers(_Config) ->
    P = wal_group_committer:new(#{max_batch_size => 3}),
    {[1, 2, 3], [4, 5]} = wal_group_committer:take(P, [1, 2, 3, 4, 5]),
    {[], []} = wal_group_committer:take(P, []),
    ok.

%%%_* Queue arithmetic ---------------------------------------------------

the_earliest_of_a_queue_is_its_binding_deadline(_Config) ->
    infinity = wal_group_committer:earliest([]),
    3 = wal_group_committer:earliest([5, 3, 9]),
    %% A real deadline always beats the empty-queue sentinel.
    0 = wal_group_committer:earliest([infinity, 0]),
    ok.
