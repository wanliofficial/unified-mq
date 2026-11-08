%% @doc HTTP/1.1 listener for the S6 admin API.
%%
%% One acceptor process owns the listen socket and spawns one short-lived
%% handler per connection. The HTTP framing is Erlang's built-in
%% `{packet, http_bin}' mode: no third-party server, no dependency beyond
%% kernel/stdlib, matching the project's "no external runtime dependency"
%% posture (ADR-016).
-module(broker_admin_listener).
-behaviour(gen_server).

-export([start_link/0, port/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(PT, {?MODULE, port}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc The bound port (useful when `port => 0' in tests).
-spec port() -> inet:port_number() | undefined.
port() ->
    persistent_term:get(?PT, undefined).

init([]) ->
    Bind = application:get_env(broker_admin, bind, {127, 0, 0, 1}),
    Port = application:get_env(broker_admin, port, 8090),
    Opts = [binary, {packet, http_bin}, {active, false},
            {reuseaddr, true}, {ip, Bind}, {backlog, 128}],
    case gen_tcp:listen(Port, Opts) of
        {ok, LSock} ->
            {ok, Actual} = inet:port(LSock),
            persistent_term:put(?PT, Actual),
            {ok, Acceptor} = start_acceptor(LSock),
            {ok, #{lsock => LSock, acceptor => Acceptor, port => Actual}};
        {error, Reason} ->
            {stop, {listen_failed, Bind, Port, Reason}}
    end.

handle_call(_Req, _From, State) ->
    {reply, {error, unsupported}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, #{acceptor := Pid} = State) ->
    case Reason of
        normal -> {noreply, State};
        _ -> {stop, {acceptor_died, Reason}, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{lsock := LSock}) ->
    _ = gen_tcp:close(LSock),
    persistent_term:erase(?PT),
    ok.

%%%_* Internal --------------------------------------------------------------

start_acceptor(LSock) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn_link(fun() ->
                              receive go -> ok end,
                              Parent ! {Ref, self()},
                              accept_loop(LSock)
                      end),
    Pid ! go,
    receive
        {Ref, Pid} -> {ok, Pid}
    after 5000 ->
            {error, acceptor_start_timeout}
    end.

accept_loop(LSock) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            _ = spawn(fun() -> handle(Sock) end),
            accept_loop(LSock);
        {error, closed} ->
            ok;
        {error, _Reason} ->
            accept_loop(LSock)
    end.

handle(Sock) ->
    _ = inet:setopts(Sock, [{packet, http_bin}, {active, false}]),
    try
        case read_request(Sock) of
            {ok, Method, Path, Headers, Body} ->
                Response = broker_admin_http:handle(Method, Path, Headers, Body),
                send_response(Sock, Response);
            {error, _Reason} ->
                send_response(Sock, {400, "application/json",
                                     <<"{\"error\":\"bad_request\"}">>})
        end
    catch
        Class:Reason:Stack ->
            logger:warning("broker_admin handler crashed ~p:~p ~p",
                           [Class, Reason, Stack]),
            _ = catch send_response(
                        Sock, {500, "application/json",
                               <<"{\"error\":\"internal_error\"}">>}),
            ok
    after
        _ = gen_tcp:close(Sock)
    end.

read_request(Sock) ->
    case gen_tcp:recv(Sock, 0, 30000) of
        {ok, {http_request, Method, Uri, _Version}} ->
            case read_headers(Sock, #{}) of
                {ok, Headers} ->
                    Path = path_of(Uri),
                    case maybe_read_body(Sock, Headers) of
                        {ok, Body} -> {ok, method(Method), Path, Headers, Body};
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        {ok, _Other} ->
            {error, not_http};
        {error, Reason} ->
            {error, Reason}
    end.

read_headers(Sock, Acc) ->
    case gen_tcp:recv(Sock, 0, 30000) of
        {ok, {http_header, _Len, Name, _Reserved, Value}} ->
            read_headers(Sock, Acc#{normalize_header(Name) => Value});
        {ok, http_eoh} ->
            {ok, Acc};
        {ok, _Other} ->
            read_headers(Sock, Acc);
        {error, Reason} ->
            {error, Reason}
    end.

normalize_header(Name) when is_atom(Name) ->
    lower(atom_to_binary(Name, utf8));
normalize_header(Name) when is_binary(Name) ->
    lower(Name).

method('GET') -> get;
method('POST') -> post;
method('PUT') -> put;
method('DELETE') -> delete;
method(Other) when is_atom(Other) -> Other;
method(Other) when is_binary(Other) -> lower(Other).

path_of({abs_path, Path}) -> Path;
path_of({absoluteURI, _Scheme, _Host, _Port, Path}) -> Path;
path_of('*') -> <<"*">>;
path_of(Other) when is_binary(Other) -> Other;
path_of(_) -> <<"/">>.

maybe_read_body(Sock, Headers) ->
    case maps:get(<<"content-length">>, Headers, undefined) of
        undefined ->
            {ok, <<>>};
        <<"0">> ->
            %% `recv(Sock, 0)' in raw mode blocks until more data arrives; a
            %% zero-length body has nothing to read, so answer directly.
            {ok, <<>>};
        Value ->
            Len = binary_to_integer(Value),
            _ = inet:setopts(Sock, [{packet, raw}]),
            case gen_tcp:recv(Sock, Len, 30000) of
                {ok, Body} -> {ok, Body};
                {error, Reason} -> {error, Reason}
            end
    end.

send_response(Sock, {Status, ContentType, Body}) ->
    BodyBin = iolist_to_binary(Body),
    Reason = reason(Status),
    Head =
        [<<"HTTP/1.1 ">>, integer_to_binary(Status), <<" ">>, Reason, <<"\r\n">>,
         <<"Content-Type: ">>, ContentType, <<"\r\n">>,
         <<"Content-Length: ">>, integer_to_binary(byte_size(BodyBin)), <<"\r\n">>,
         <<"Connection: close\r\n\r\n">>],
    gen_tcp:send(Sock, [Head, BodyBin]).

reason(200) -> <<"OK">>;
reason(201) -> <<"Created">>;
reason(400) -> <<"Bad Request">>;
reason(401) -> <<"Unauthorized">>;
reason(404) -> <<"Not Found">>;
reason(405) -> <<"Method Not Allowed">>;
reason(500) -> <<"Internal Server Error">>;
reason(503) -> <<"Service Unavailable">>.

lower(Bin) when is_binary(Bin) -> << <<(lower_char(C))>> || <<C>> <= Bin >>.

lower_char(C) when C >= $A, C =< $Z -> C + 32;
lower_char(C) -> C.
