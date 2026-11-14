%% @doc Launch the S10-D evidence node, print the readiness line, then block.
%%
%% `webrtc_evidence.py' spawns this and reads `S10-NODE-READY wss=<n> stun=<n>' to learn
%% the two ephemeral ports before it drives any aiortc peer against them. Splitting the
%% boot (here) from the drive (Python) keeps the node a plain `erl -noshell' target that
%% works on a clean checkout with only `rebar3 compile' done.
-module(s10_run).
-export([main/0]).

main() ->
    logger:remove_handler(default),
    ok = logger:add_handler(default, logger_std_h,
                            #{config => #{type => standard_error}}),
    s10_node:run().
