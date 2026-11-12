%% S10-0 spike: same signaling code over ranch's ssl transport.
%%
%% This is the question S10-A cannot answer by reading code: ranch 2.x hands
%% the protocol module either a gen_tcp or an ssl socket, and both must go
%% through the identical ws_handshake / ws_frame path. If that holds, S10-A
%% needs no second implementation and no cowboy.
-module(spike_run_tls).
-export([main/1]).

main([PortFile, OutFile, CertDir, SecsStr]) ->
    Secs = list_to_integer(SecsStr),
    {ok, _} = application:ensure_all_started(ranch),
    {ok, _} = application:ensure_all_started(ssl),
    CertFile = filename:join(CertDir, "spike-cert.pem"),
    KeyFile = filename:join(CertDir, "spike-key.pem"),
    {ok, _} = ranch:start_listener(
                spike_wss, ranch_ssl,
                #{socket_opts => [{ip, {127,0,0,1}}, {port, 0},
                                  {certfile, CertFile}, {keyfile, KeyFile}],
                  num_acceptors => 1},
                spike_ws, #{}),
    Port = ranch:get_port(spike_wss),
    ok = file:write_file(PortFile, integer_to_list(Port)),
    register(spike_probe_srv, self()),
    _ = ets:new(spike_room, [named_table, public, set, {read_concurrency, true}]),
    Events = collect(Secs * 1000, []),
    ok = file:write_file(OutFile, [io_lib:format("~p.~n", [E]) || E <- lists:reverse(Events)]),
    io:format("~p events written to ~s~n", [length(Events), OutFile]),
    halt(0).

collect(Timeout, Acc) ->
    receive
        {ev, E} -> collect(Timeout, [E | Acc])
    after Timeout -> Acc
    end.
