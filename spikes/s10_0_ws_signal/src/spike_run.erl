%% S10-0 spike runner: start a ranch listener on port 0 with spike_ws, publish
%% the port to a file, then log every {ev, _} the connections report.
%%
%% Two modes:
%%   erl -run spike_run main <portfile> <outfile> <seconds>
%% The Python client in this directory reads <portfile>, drives the scenario,
%% and the events land in <outfile>. Port 0 keeps it clear of every fixed port
%% the repo already uses (8080/8083/1883/8081/8082).
-module(spike_run).
-export([main/1]).

main([PortFile, OutFile, SecsStr]) ->
    Secs = list_to_integer(SecsStr),
    {ok, _} = application:ensure_all_started(ranch),
    {ok, _} = ranch:start_listener(spike_ws, ranch_tcp,
                                   #{socket_opts => [{ip, {127,0,0,1}}, {port, 0}],
                                     num_acceptors => 1},
                                   spike_ws, #{}),
    Port = ranch:get_port(spike_ws),
    ok = file:write_file(PortFile, integer_to_list(Port)),
    register(spike_probe_srv, self()),
    %% one public room table for every connection (ADR-028 D4's stand-in)
    _ = ets:new(spike_room, [named_table, public, set, {read_concurrency, true}]),
    Events = collect(Secs * 1000, []),
    dump(OutFile, Events),
    io:format("~p events written to ~s~n", [length(Events), OutFile]),
    halt(0).

%% Write incrementally so a SIGTERM from the runner still leaves a full trace.
dump(OutFile, Events) ->
    file:write_file(OutFile, [io_lib:format("~p.~n", [E]) || E <- lists:reverse(Events)]).

collect(Timeout, Acc) ->
    receive
        {ev, E} = M ->
            maybe_flush(Acc, M),
            collect(Timeout, [E | Acc]);
        done -> collect(Timeout, Acc)
    after Timeout -> Acc
    end.

maybe_flush(_Acc, _M) -> ok.
