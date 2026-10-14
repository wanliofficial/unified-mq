%% S0.3 correctness tests for the WAL / segment log.
%%
%% These matter more than the benchmarks: a fast log that silently loses or
%% duplicates records under a torn write is worse than a slow correct one.
-module(s0_3_wal_tests).

-include_lib("eunit/include/eunit.hrl").

basic_append_read_test() ->
    Dir = tmp("basic"),
    {ok, W0} = s0_3_wal:open(Dir, #{}),
    W = lists:foldl(fun(N, Acc) ->
                            {ok, W1} = s0_3_wal:append(Acc, {msg, N}, os_buffer),
                            W1
                    end, W0, lists:seq(1, 100)),
    {ok, _} = s0_3_wal:close(W),
    {ok, Terms} = s0_3_wal:read_all(Dir),
    ?assertEqual(lists:seq(1, 100), [N || {msg, N} <- Terms]),
    ok = rm(Dir).

reopen_appends_test() ->
    Dir = tmp("reopen"),
    {ok, W0} = s0_3_wal:open(Dir, #{}),
    {ok, W1} = s0_3_wal:append(W0, first, fsync),
    {ok, _} = s0_3_wal:close(W1),

    %% Reopening must not truncate.
    {ok, W2} = s0_3_wal:open(Dir, #{}),
    {ok, W3} = s0_3_wal:append(W2, second, fsync),
    {ok, _} = s0_3_wal:close(W3),

    {ok, Terms} = s0_3_wal:read_all(Dir),
    ?assertEqual([first, second], Terms),
    ok = rm(Dir).

%% A crash mid-record leaves a torn tail. Recovery must stop cleanly at the
%% tear instead of failing or inventing records (README 11.3).
torn_tail_recovery_test() ->
    Dir = tmp("torn"),
    {ok, W0} = s0_3_wal:open(Dir, #{}),
    {ok, W1} = s0_3_wal:append(W0, alpha, fsync),
    {ok, W2} = s0_3_wal:append(W1, beta, fsync),
    {ok, _} = s0_3_wal:close(W2),

    %% Append half a record by hand. The body must not compress down to a few
    %% bytes, otherwise a 3-byte prefix looks like a complete frame.
    Seg = s0_3_wal:segment_path(Dir, 1),
    {ok, Fd} = file:open(Seg, [append, binary, raw]),
    Body = term_to_binary(crypto:strong_rand_bytes(512), [{compressed, 1}]),
    ?assert(byte_size(Body) > 100),
    Partial = <<(byte_size(Body)):32/big, (binary:part(Body, 0, 3))/binary>>,
    ok = file:write(Fd, Partial),
    ok = file:close(Fd),

    {ok, Terms} = s0_3_wal:read_all(Dir),
    ?assertEqual([alpha, beta], Terms),
    ok = rm(Dir).

segment_rotation_test() ->
    %% Tiny segments force rotation so the multi-segment path is exercised.
    Dir = tmp("rotate"),
    {ok, W0} = s0_3_wal:open(Dir, #{seg_max_bytes => 4096}),
    W = lists:foldl(fun(N, Acc) ->
                            {ok, W1} = s0_3_wal:append(Acc, {msg, N, payload()}, fsync),
                            W1
                    end, W0, lists:seq(1, 200)),
    {ok, _} = s0_3_wal:close(W),

    {ok, Segs} = s0_3_wal:load_segments(Dir),
    ?assert(length(Segs) > 1),
    ?assertEqual(lists:seq(1, length(Segs)), Segs),

    {ok, Terms} = s0_3_wal:read_all(Dir),
    ?assertEqual(lists:seq(1, 200), [N || {msg, N, _} <- Terms]),
    ok = rm(Dir).

%% Records must survive a restart on an existing log with several segments.
restart_with_segments_test() ->
    Dir = tmp("restart_seg"),
    {ok, W0} = s0_3_wal:open(Dir, #{seg_max_bytes => 4096}),
    W = lists:foldl(fun(N, Acc) ->
                            {ok, W1} = s0_3_wal:append(Acc, {msg, N}, os_buffer),
                            W1
                    end, W0, lists:seq(1, 100)),
    {ok, _} = s0_3_wal:close(W),
    {ok, SegsBefore} = s0_3_wal:load_segments(Dir),

    {ok, W1} = s0_3_wal:open(Dir, #{seg_max_bytes => 4096}),
    {ok, W2} = s0_3_wal:append(W1, {msg, 101}, fsync),
    {ok, _} = s0_3_wal:close(W2),

    {ok, SegsAfter} = s0_3_wal:load_segments(Dir),
    %% Reopening must not create a new segment.
    ?assertEqual(SegsBefore, SegsAfter),
    {ok, Terms} = s0_3_wal:read_all(Dir),
    ?assertEqual(101, length(Terms)),
    ok = rm(Dir).

%% The guard that stops us publishing numbers measured on tmpfs.
real_disk_guard_test() ->
    ?assertEqual(ok, s0_3_bench:assert_real_disk()).

payload() -> binary:copy(<<"p">>, 64).

tmp(Tag) ->
    Dir = "/tmp/unified_mq_s0_3_test/" ++ Tag ++ "_"
          ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

rm(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.