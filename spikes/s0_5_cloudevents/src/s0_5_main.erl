%% S0.5 - CloudEvents overhead benchmark driver.
%%
%% The question is NOT "is validation fast" in the abstract. It is:
%%   what does validation cost per message, and therefore what limits does
%%   README 10.1 need on extensions / nesting depth to keep a hostile producer
%%   from turning validation into a CPU denial-of-service vector?
-module(s0_5_main).

-export([main/1, run/1, format/1]).

-define(DEFAULT_ITERS, 20000).
%% Extension counts to probe, spanning "normal" to "hostile".
-define(EXT_POINTS, [0, 4, 16, 64, 256, 1024]).
-define(DATA_SIZES, [64, 1024]).

%% main(Args) -> no_return().  Args: ["iters","20000","out","report.txt"]
main(Args) ->
    Iters = int_arg("iters", Args, ?DEFAULT_ITERS),
    Out = str_arg("out", Args, undefined),
    Result = run(Iters),
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

run(Iters) ->
    #{iterations => Iters,
      points => [s0_5_codec:bench(Iters, E, D)
                 || D <- ?DATA_SIZES, E <- ?EXT_POINTS]}.

format(#{iterations := Iters, points := Points}) ->
    Header = io_lib:format(
        "=== S0.5 CloudEvents validation overhead (~w iters) ===~n"
        "  ext  data   json_B  decode_us  validate_us  encode_us  total_us~n",
        [Iters]),
    Rows = [io_lib:format("  ~4w  ~5w  ~7w  ~9.1f  ~12.1f  ~10.1f  ~8.1f~n",
                          [maps:get(extensions, P),
                           maps:get(data_bytes, P),
                           maps:get(json_bytes, P),
                           maps:get(decode_us, P),
                           maps:get(validate_us, P),
                           maps:get(encode_us, P),
                           maps:get(total_us, P)])
            || P <- Points],
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