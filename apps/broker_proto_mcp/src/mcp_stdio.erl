%% @doc stdio front end (ADR-027 §D8; `basic/transports/stdio').
%%
%% Framing rules that define the module:
%%
%%   * one JSON-RPC message per line, no embedded newlines, UTF-8;
%%   * `stdout' carries **only** protocol frames -- every log line goes to
%%     `stderr' (`logger' already does);
%%   * end of input (EOF) is the graceful shutdown signal: the process exits
%%     promptly and takes its subscriptions with it;
%%   * a `subscriptions/listen' request runs in a **child process** that shares
%%     the same stdout writer, so the read loop keeps serving other requests.
%%     Cancellation arrives as `notifications/cancelled' with the listen
%%     request's id, which is why subscriptions are keyed by that id.
%%
%% There is no header layer and no credential channel: the trust boundary is
%% OS-level access to the process, which is exactly what the spec says for stdio
%% ("retrieve credentials from the environment"). The gateway therefore runs
%% stdio as the anonymous local identity.
-module(mcp_stdio).

-export([start_link/0, run/2]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, spawn_link(fun() -> run(fun read_line/0, fun write_line/1) end)}.

%% @doc The loop, with the input/output primitives injected so the suite can
%% drive it in-process.
-spec run(fun(() -> binary() | string() | eof | {error, term()}),
          fun((iodata()) -> ok | {error, term()})) -> ok.
run(Read, Write) ->
    loop(Read, Write, identity(), #{}).

loop(Read, Write, Identity, Subscriptions) ->
    case Read() of
        eof ->
            shutdown(Subscriptions);
        {error, Reason} ->
            logger:warning("mcp stdio read failed: ~0p", [Reason]),
            shutdown(Subscriptions);
        Data ->
            Line = normalize(Data),
            Next = handle_line(Line, Write, Identity, Subscriptions),
            loop(Read, Write, Identity, Next)
    end.

handle_line(<<>>, _Write, _Identity, Subscriptions) ->
    Subscriptions;
handle_line(Line, Write, Identity, Subscriptions) ->
    case agent_rpc_json:decode(Line) of
        {error, parse_error} ->
            _ = write(Write, agent_rpc_json:response_error(null, parse_error, undefined)),
            Subscriptions;
        {ok, Term} ->
            case cancelled(Term) of
                {yes, Id} ->
                    cancel(Id, Subscriptions);
                no ->
                    dispatch(Term, Write, Identity, Subscriptions)
            end
    end.

dispatch(Term, Write, Identity, Subscriptions) ->
    case mcp_dispatch:handle(Term, #{transport => stdio, identity => Identity}) of
        no_reply ->
            Subscriptions;
        {bare, _Status} ->
            Subscriptions;
        {json, _Status, Object} ->
            _ = write(Write, Object),
            Subscriptions;
        {stream, Plan} ->
            Id = maps:get(id, Plan),
            %% Linked on purpose: the stdio process owns the whole channel, so a
            %% crashed stream is a channel failure, not a silent leak of a
            %% process nobody can cancel.
            Pid = spawn_link(fun() ->
                                     mcp_stream:run_stdio({stdio, Write}, Plan)
                             end),
            Subscriptions#{Id => Pid}
    end.

%% `notifications/cancelled' is the only client-to-server notification in the
%% core protocol, and stdio is the only transport that carries it.
cancelled(#{<<"method">> := <<"notifications/cancelled">>,
            <<"params">> := Params}) when is_map(Params) ->
    case maps:get(<<"requestId">>, Params, undefined) of
        Id when is_binary(Id); is_integer(Id) -> {yes, Id};
        _ -> no
    end;
cancelled(_Term) ->
    no.

cancel(Id, Subscriptions) ->
    case maps:take(Id, Subscriptions) of
        {Pid, Rest} ->
            %% The child notices as soon as it reaches its next wait, and its
            %% `after' clause removes the process-local partition hook.
            Pid ! mcp_cancel,
            Rest;
        error ->
            Subscriptions
    end.

shutdown(Subscriptions) ->
    maps:foreach(fun(_Id, Pid) -> Pid ! mcp_cancel end, Subscriptions),
    ok.

write(Write, Object) ->
    try Write([agent_rpc_json:encode(Object), <<"\n">>]) of
        ok -> ok;
        {error, Reason} ->
            logger:warning("mcp stdio write failed: ~0p", [Reason]),
            ok;
        _Other ->
            ok
    catch
        _:_ ->
            ok
    end.

normalize(Data) when is_binary(Data) ->
    trim(Data);
normalize(Data) when is_list(Data) ->
    trim(unicode:characters_to_binary(Data));
normalize(_Other) ->
    <<>>.

trim(Bin) ->
    string:trim(Bin).

%% stdio runs as the local anonymous identity: the process itself is the
%% credential (`basic/authorization': stdio SHOULD NOT use the HTTP framework).
identity() ->
    agent_rpc_auth:anonymous().

read_line() ->
    case io:get_line("") of
        eof -> eof;
        {error, _} = Error -> Error;
        Data -> Data
    end.

write_line(Iodata) ->
    io:put_chars(standard_io, Iodata).
