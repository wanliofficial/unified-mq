%% S0.3 - batch size sweep: where does group commit stop paying off?
%%
%% The headline number is that per-append fsync costs ~1ms while a 64-message
%% batch costs the same ~1ms. This sweep finds the knee, which is what the
%% production group-commit window should be derived from (not guessed).
-module(s0_3_sweep).

-export([main/1, run/2, format/1]).

-define(BATCHES, [1, 8, 16, 32, 64, 128, 256, 512]).
-define(MSGS, 2000).

%% main(Args) -> no_return().  Args: ["msgs","2000","out","report.txt"]
main(Args) ->
    Msgs = int_arg("msgs", Args, ?MSGS),
    Out = str_arg("out", Args, undefined),
    ok = s0_3_bench:assert_real_disk(),
    Payload = binary:copy(<<"x">>, 256),
    Result = run(Msgs, Payload),
    Text = format(Result),
    io:format("~s", [Text]),
    case Out of
        undefined -> ok;
        Path ->
            ok = filelib:ensure_dir(Path),
            ok = file:write_file(Path, io_lib:format("~s~n", [Text])),
            io:format("report written: ~s~n", [Path])
    end,
    halt(0).

run(Msgs, Payload) ->
    #{msgs => Msgs,
      payload_bytes => byte_size(Payload),
      points => [point(Msgs, Payload, B) || B <- ?BATCHES]}.

point(Msgs, Payload, Batch) ->
    R = s0_3_bench:group_commit(Msgs, Payload, Batch),
    PerSec = maps:get(msgs_per_sec, R),
    #{batch_size => Batch,
      msgs_per_sec => PerSec,
      us_per_msg => maps:get(us_per_msg, R),
      fsyncs => maps:get(fsyncs, R),
      msgs_per_fsync => maps:get(msgs, R) div maps:get(fsyncs, R)}.

format(#{msgs := Msgs, payload_bytes := Bytes, points := Points}) ->
    Rows = [begin
                io_lib:format("  batch=~4w ~8w msgs/s ~7.1f us/msg ~6w msgs/fsync~n",
                              [maps:get(batch_size, P),
                               maps:get(msgs_per_sec, P),
                               maps:get(us_per_msg, P),
                               maps:get(msgs_per_fsync, P)])
            end || P <- Points],
    Header = io_lib:format("=== S0.3 group-commit sweep (~w msgs, ~w B payload) ===~n",
                           [Msgs, Bytes]),
    lists:flatten([Header, Rows]).

str_arg(Key, Args, Default) ->
    case flat_lookup(Key, Args) of
        {ok, Value} -> Value;
        error -> Default
    end.

int_arg(Key, Args, Default) ->
    case flat_lookup(Key, Args) of
        {ok, Value} -> list_to_integer(Value);
        error -> Default
    end.

flat_lookup(Key, [Key, Value | _]) -> {ok, Value};
flat_lookup(Key, [_ | Rest]) -> flat_lookup(Key, Rest);
flat_lookup(_Key, []) -> error.