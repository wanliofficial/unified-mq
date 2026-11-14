%% S10-D D.2 evidence: the runtime "no persistence" snapshot for the WebRTC
%% signaling path (ADR-028 D2: signaling is instantaneous forwarding, it writes
%% nothing to the kernel).
%%
%% The source-level proof already lives in `webrtc_boundary_SUITE' -- it strips
%% comments and asserts that no `broker_queue:' / `broker_router:' / `#broker_message{'
%% / `.cursors' appears in the signaling app's sources. That suite proves the
%% *absence of a call site*. It cannot prove the absence of a *write*, because a
%% write could arrive through a dependency. This module supplies the
%% complementing runtime proof the execution plan asks for (D.2):
%%
%%   1. boot the real kernel, with `broker_subscription' running and its 100 ms
%%      cursor tick armed -- so "no cursor file appeared" is a statement about a
%%      live writer, not about an app that was never started;
%%   2. snapshot the three durable facts a signaling frame must not move;
%%   3. drive a full 1:1 WebRTC call (offer / answer / ICE / data channel)
%%      through the production WSS signaling relay;
%%   4. snapshot again and assert all three are unchanged.
%%
%% The three facts:
%%
%%   * `offsets'  -- every partition's `{Committed, High}' watermark, read
%%                   through the public `broker_queue:watermarks/1'. One
%%                   appended message moves `high'; one ack moves `committed'.
%%   * `wal_bytes'-- the total size of every file under the WAL directory
%%                   (segments, `meta.log', and any part-written segment). A
%%                   frame that reached storage at all would move this even if
%%                   the watermark were rolled back.
%%   * `cursors'  -- the set of `*.cursors' files under the subscription
%%                   sessions dir, i.e. the third data plane ADR-028 §D1
%%                   forbids. This must stay empty.
%%
%% `session_files' is recorded for the record but deliberately **not** compared:
%% `.subs'/`.inflight' belong to the subscription data plane, and while this
%% evidence also expects them to stay empty, the plan's contract for D.2 names
%% the three above and a guard that drifts from its contract gets disabled.
%%
%% Everything here is read-only observation; no function in this module mutates
%% kernel state.
-module(s10_no_persist).

-include_lib("kernel/include/file.hrl").

-export([snapshot/0, diff/2, format/1, to_json/1]).

%% @doc One observation of the durable facts. A map so the JSON evidence and the
%% diff below read the same term.
-spec snapshot() -> map().
snapshot() ->
    #{taken_at_ms => erlang:system_time(millisecond),
      offsets => offsets(),
      wal_bytes => wal_bytes(),
      cursors => cursor_files(),
      session_files => session_files()}.

%% @doc `unchanged', or `{changed, [{Fact, Before, After}]}'. Only the three
%% contract facts are compared; `taken_at_ms' is wall clock and
%% `session_files' is informational (see the module doc).
-spec diff(map(), map()) -> unchanged | {changed, [{atom(), term(), term()}]}.
diff(Before, After) ->
    Moved = [{Key, maps:get(Key, Before), maps:get(Key, After)}
             || Key <- [offsets, wal_bytes, cursors],
                maps:get(Key, Before) =/= maps:get(Key, After)],
    case Moved of
        [] -> unchanged;
        _ -> {changed, Moved}
    end.

%%%_* Offsets ---------------------------------------------------------------

%% Public API on purpose: `broker_queue:watermarks/1' is what an operator would
%% read, and going through it keeps this evidence from depending on the WAL's
%% internal process names. Maps rather than tagged tuples so the value is
%% JSON-encodable as-is and still compares byte-for-byte.
offsets() ->
    lists:sort(
      [#{partition => P, committed => Committed, high => High}
       || P <- broker_queue:partition_ids(),
          {ok, Committed, High} <- [watermarks(P)]]).

watermarks(P) ->
    try broker_queue:watermarks(P)
    catch _:_ -> {error, unavailable}
    end.

%%%_* WAL bytes -------------------------------------------------------------

%% Sum every regular file under `data_dir/wal'. A missing `wal' directory is 0,
%% not an error: a node that never created a partition has nothing to measure
%% and the comparison is still meaningful (0 == 0).
wal_bytes() ->
    case storage_dir() of
        undefined ->
            0;
        DataDir ->
            WalDir = filename:join(DataDir, "wal"),
            filelib:fold_files(WalDir, ".*", true,
                               fun(Path, Acc) -> Acc + file_size(Path) end,
                               0)
    end.

storage_dir() ->
    case application:get_env(broker_storage, data_dir) of
        {ok, Dir} when is_list(Dir) -> Dir;
        {ok, Dir} when is_binary(Dir) -> binary_to_list(Dir);
        _ -> undefined
    end.

file_size(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = regular, size = Size}} -> Size;
        _ -> 0
    end.

%%%_* Cursor / session files -------------------------------------------------

%% Ask `broker_subscription_files' where the sessions live instead of rebuilding
%% the layout here: the file layout is that module's business (ADR-019 §19.3).
sessions_dir() ->
    try broker_subscription_files:sessions_dir()
    catch _:_ -> undefined
    end.

cursor_files() ->
    names("*.cursors").

session_files() ->
    names("*").

names(Pattern) ->
    case sessions_dir() of
        undefined -> [];
        Dir -> lists:sort([filename:basename(P)
                           || P <- filelib:wildcard(filename:join(Dir, Pattern))])
    end.

%%%_* Formatting ------------------------------------------------------------

-spec format(map()) -> iolist().
format(Snapshot) ->
    io_lib:format("~p", [Snapshot]).

%% @doc The snapshot as a JSON-ready term: atom keys become binaries, tuple/list
%% values become JSON arrays. Everything else already encodes (integers,
%% binaries, booleans, `null').
to_json(Snapshot) -> to_json_value(Snapshot).

to_json_value(V) when is_map(V) ->
    maps:from_list([{json_key(K), to_json_value(Val)}
                    || {K, Val} <- maps:to_list(V)]);
to_json_value(V) when is_tuple(V) ->
    [to_json_value(E) || E <- tuple_to_list(V)];
to_json_value([]) ->
    %% `io_lib:printable_unicode_list([])' is true, so the empty list would
    %% otherwise encode as the empty string -- wrong for an empty cursor set.
    [];
to_json_value(V) when is_list(V) ->
    case io_lib:printable_unicode_list(V) of
        true -> unicode:characters_to_binary(V);
        false -> [to_json_value(E) || E <- V]
    end;
to_json_value(V) when is_atom(V), V =/= true, V =/= false, V =/= null ->
    atom_to_binary(V, utf8);
to_json_value(V) ->
    V.

json_key(K) when is_atom(K) -> atom_to_binary(K, utf8);
json_key(K) when is_binary(K) -> K;
json_key(K) when is_integer(K) -> integer_to_binary(K).
