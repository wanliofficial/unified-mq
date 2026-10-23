%% @doc T5: tests for the sparse read index (ADR-011 §11.6, design §7.2).
%%
%% The index exists so that `read_from' can run in the calling process: the
%% table is public and the writer is not involved, which is the whole point of
%% keeping dispatch off the writer's mailbox. Two properties carry that weight
%% and are pinned here -- a reader process sees what the writer published, and
%% `locate' returns the segment path together with the position, because a
%% position alone is meaningless once the log has rotated.
-module(wal_index_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [a_new_index_is_open_and_truncates_nothing,
     state_is_readable_from_another_process,
     set_trunc_moves_the_logical_boundary,
     closing_is_visible_to_a_reader_process,
     locate_finds_an_exact_anchor,
     locate_falls_back_to_the_greatest_anchor_below,
     locate_below_the_first_anchor_is_not_found,
     locate_never_returns_an_anchor_from_an_earlier_segment].

%%%_* Tests --------------------------------------------------------------

a_new_index_is_open_and_truncates_nothing(_Config) ->
    Tid = wal_index:new(),
    {true, 1} = wal_index:state(Tid),
    ok.

state_is_readable_from_another_process(_Config) ->
    Tid = wal_index:new(),
    true = wal_index:set_trunc(Tid, 42),
    {true, 42} = in_other_process(fun() -> wal_index:state(Tid) end),
    ok.

set_trunc_moves_the_logical_boundary(_Config) ->
    Tid = wal_index:new(),
    true = wal_index:set_trunc(Tid, 7),
    {true, 7} = wal_index:state(Tid),
    true = wal_index:set_trunc(Tid, 9),
    {true, 9} = wal_index:state(Tid),
    ok.

closing_is_visible_to_a_reader_process(_Config) ->
    Tid = wal_index:new(),
    true = wal_index:set_open(Tid, false),
    {false, 1} = in_other_process(fun() -> wal_index:state(Tid) end),
    ok.

locate_finds_an_exact_anchor(_Config) ->
    Tid = wal_index:new(),
    true = wal_index:add_anchor(Tid, 256, "seg-a", 12345),
    {ok, 256, "seg-a", 12345} = wal_index:locate(Tid, 256),
    ok.

locate_falls_back_to_the_greatest_anchor_below(_Config) ->
    Tid = wal_index:new(),
    true = wal_index:add_anchor(Tid, 1, "seg-a", 0),
    true = wal_index:add_anchor(Tid, 256, "seg-a", 12345),
    {ok, 256, "seg-a", 12345} = wal_index:locate(Tid, 300),
    {ok, 1, "seg-a", 0} = wal_index:locate(Tid, 255),
    ok.

locate_below_the_first_anchor_is_not_found(_Config) ->
    Tid = wal_index:new(),
    true = wal_index:add_anchor(Tid, 100, "seg-a", 0),
    not_found = wal_index:locate(Tid, 99),
    %% The meta row sorts before every anchor key, so a table with no anchors at
    %% all must report not_found rather than handing back the meta row.
    not_found = wal_index:locate(wal_index:new(), 1),
    ok.

%% The hazard this pins: offsets are dense across the whole partition, so an
%% anchor from the previous segment is "the greatest one below" for the first
%% records of the next one. Serving it would seek into the wrong file.
locate_never_returns_an_anchor_from_an_earlier_segment(_Config) ->
    Tid = wal_index:new(),
    true = wal_index:add_anchor(Tid, 1, "seg-a", 0),
    true = wal_index:add_anchor(Tid, 256, "seg-a", 900),
    %% Rotation anchors the first record of the new segment at position 0.
    true = wal_index:add_anchor(Tid, 500, "seg-b", 0),
    true = wal_index:add_anchor(Tid, 756, "seg-b", 4321),
    {ok, 500, "seg-b", 0} = wal_index:locate(Tid, 500),
    {ok, 500, "seg-b", 0} = wal_index:locate(Tid, 600),
    {ok, 756, "seg-b", 4321} = wal_index:locate(Tid, 999),
    {ok, 256, "seg-a", 900} = wal_index:locate(Tid, 499),
    ok.

%%%_* Helpers ------------------------------------------------------------

in_other_process(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() -> Parent ! {Ref, Fun()} end),
    receive
        {Ref, Value} -> Value
    after
        5000 -> exit({timeout_waiting_for, Pid})
    end.
