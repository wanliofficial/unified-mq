%% @doc Entry point for the D.6 ICE bench: reads the STUN/TURN port and the
%% bench knobs from the environment, runs `ice_bench:run/5', then stops the VM.
%%
%% Driven by `run_bench.sh' against the same evidence node the signaling bench
%% uses. The node's `broker_ice' `max_allocations' (16) bounds how many
%% allocations can exist at once, so the concurrent-allocation count is kept at
%% or below that by the script.
-module(s10_bench_run).
-export([main/0]).

main() ->
    StunPort = list_to_integer(os:getenv("BENCH_STUN_PORT", "3478")),
    Opts = #{alloc_seq => i("BENCH_ALLOC_SEQ", 50),
             alloc_conc => i("BENCH_ALLOC_CONC", 16),
             relay_msgs => i("BENCH_RELAY_MSGS", 5000),
             payload_bytes => i("BENCH_PAYLOAD", 256)},
    ice_bench:run({127, 0, 0, 1}, StunPort, <<"alice">>, <<"alicepw">>, Opts),
    init:stop().

i(Key, Default) ->
    case os:getenv(Key) of
        false -> Default;
        "" -> Default;
        S -> list_to_integer(S)
    end.
