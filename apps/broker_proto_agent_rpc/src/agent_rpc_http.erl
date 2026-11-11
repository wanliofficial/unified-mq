%% @doc HTTP/1.1 request/response framing, transport-agnostic.
%%
%% The module is handed a `ranch' transport module (`ranch_tcp' / `ranch_ssl')
%% and a socket, so the same code serves both listeners and the CT suites can
%% drive it with a plain `gen_tcp' socket via `ranch_tcp'. It deliberately does
%% **not** own an accept loop: the adapter's listener decides when to accept and
%% how many connections to run (ADR-026 §D1/§D2).
%%
%% Framing rules that matter for the A2A front end:
%%
%%   * `{packet, http_bin}' parses the request line and headers; the body is
%%     read in `{packet, raw}' (same two-mode trick `broker_admin_listener'
%%     uses).
%%   * `Content-Length' wins when present; `Transfer-Encoding: chunked' is
%%     accepted for request bodies and decoded with the same hard size cap.
%%   * The cap is checked **before** the body is read, so a lying
%%     `Content-Length' cannot allocate the memory it claims.
%%   * `Expect: 100-continue' is answered with the interim response before the
%%     body is read.
-module(agent_rpc_http).

-export([read_request/3, header/2, content_type/1, accepts_json/1,
         keep_alive/1, keep_alive/2, response/3, response/4,
         chunk/1, last_chunk/0, normalize_headers/1]).
-export_type([headers/0, request/0]).

-type headers() :: #{binary() => binary()}.
-type request() :: #{method := atom() | binary(),
                     path := binary(),
                     version := {non_neg_integer(), non_neg_integer()},
                     headers := headers(),
                     body := binary(),
                     keep_alive := boolean()}.

%% @doc Read one request. `Limits' keys: `body_bytes' (default 1 MiB) and
%% `request_timeout_ms' (default 30 s).
-spec read_request(module(), term(), map()) ->
          {ok, request()} | {error, term()}.
read_request(Transport, Socket, Limits) ->
    Timeout = maps:get(request_timeout_ms, Limits, 30000),
    case setopts(Transport, Socket, [{packet, http_bin}, {active, false}]) of
        ok ->
            case Transport:recv(Socket, 0, Timeout) of
                {ok, {http_request, Method, Uri, Version}} ->
                    case read_headers(Transport, Socket, Timeout, #{}) of
                        {ok, Headers} ->
                            read_body(Transport, Socket, Method, Uri, Version,
                                      Headers, Limits);
                        {error, _} = Error ->
                            Error
                    end;
                {ok, _Other} ->
                    {error, not_http};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Case-insensitive header lookup; the map is already lower-cased.
-spec header(binary(), headers()) -> binary() | undefined.
header(Name, Headers) when is_binary(Name) ->
    maps:get(lower(Name), Headers, undefined).

%% @doc Base media type, lower-cased, parameters stripped.
-spec content_type(headers()) -> binary() | undefined.
content_type(Headers) ->
    case header(<<"content-type">>, Headers) of
        undefined -> undefined;
        Value ->
            [Base | _] = binary:split(Value, <<";">>),
            lower(trim(Base))
    end.

%% @doc The A2A JSON-RPC binding carries `application/json'; the A2A media type
%% is accepted as an alias because some clients send it for the same binding.
-spec accepts_json(headers()) -> boolean().
accepts_json(Headers) ->
    lists:member(content_type(Headers),
                 [<<"application/json">>, <<"application/a2a+json">>]).

%% @doc HTTP/1.1 default is keep-alive; an explicit `Connection' wins.
-spec keep_alive(headers()) -> boolean().
keep_alive(Headers) ->
    keep_alive({1, 1}, Headers).

-spec keep_alive({non_neg_integer(), non_neg_integer()}, headers()) -> boolean().
keep_alive(Version, Headers) ->
    case header(<<"connection">>, Headers) of
        undefined -> Version =:= {1, 1};
        Value ->
            case lower(trim(Value)) of
                <<"close">> -> false;
                <<"keep-alive">> -> true;
                _ -> Version =:= {1, 1}
            end
    end.

%% @doc Serialize a response. `content-length' is added unless the caller
%% already supplied it, and the connection is closed by default (`SSE' callers
%% pass `#{keep_alive => true, chunked => true}').
-spec response(non_neg_integer(), [{binary(), binary()}], iodata()) -> iolist().
response(Status, Headers, Body) ->
    response(Status, Headers, Body, #{}).

-spec response(non_neg_integer(), [{binary(), binary()}], iodata(), map()) -> iolist().
response(Status, Headers, Body, Opts) ->
    BodyBin = iolist_to_binary(Body),
    KeepAlive = maps:get(keep_alive, Opts, false),
    Chunked = maps:get(chunked, Opts, false),
    Base0 = [{<<"connection">>, case KeepAlive of true -> <<"keep-alive">>;
                                     false -> <<"close">> end}],
    Base = case {Chunked, has_header(<<"transfer-encoding">>, Headers)} of
               {true, false} -> [{<<"transfer-encoding">>, <<"chunked">>} | Base0];
               _ -> Base0
           end,
    WithLength =
        case {Chunked, has_header(<<"content-length">>, Headers),
              has_header(<<"transfer-encoding">>, Headers)} of
            {false, false, false} ->
                [{<<"content-length">>, integer_to_binary(byte_size(BodyBin))} | Headers];
            _ ->
                Headers
        end,
    [<<"HTTP/1.1 ">>, integer_to_binary(Status), <<" ">>, reason(Status), <<"\r\n">>,
     [[Name, <<": ">>, Value, <<"\r\n">>] || {Name, Value} <- Base ++ WithLength],
     <<"\r\n">>, BodyBin].

-spec chunk(iodata()) -> iolist().
chunk(Data) ->
    Bin = iolist_to_binary(Data),
    [integer_to_binary(byte_size(Bin), 16), <<"\r\n">>, Bin, <<"\r\n">>].

-spec last_chunk() -> binary().
last_chunk() ->
    <<"0\r\n\r\n">>.

%% @doc Turn `{Name, Value}' pairs from `{packet, http_bin}' into a lower-cased
%% map, joining duplicates with a comma (A2A §9.2 requires comma-merged service
%% parameters).
-spec normalize_headers([{atom() | binary(), binary()}]) -> headers().
normalize_headers(Pairs) ->
    lists:foldl(
      fun({Name, Value}, Acc) ->
              Key = normalize_name(Name),
              case maps:find(Key, Acc) of
                  {ok, Existing} -> Acc#{Key => <<Existing/binary, ",", Value/binary>>};
                  error -> Acc#{Key => Value}
              end
      end, #{}, Pairs).

%%%_* Body ------------------------------------------------------------------

read_body(Transport, Socket, Method, Uri, Version, Headers, Limits) ->
    Max = maps:get(body_bytes, Limits, 1048576),
    Timeout = maps:get(request_timeout_ms, Limits, 30000),
    case {content_length(Headers), is_chunked(Headers)} of
        {{error, Reason}, _} ->
            {error, Reason};
        {{ok, Length}, _} when Length > Max ->
            {error, payload_too_large};
        {{ok, Length}, _} when Length >= 0 ->
            maybe_continue(Transport, Socket, Length, Headers),
            case read_exact_raw(Transport, Socket, Length, Timeout) of
                {ok, Body} ->
                    {ok, request(Method, Uri, Version, Headers, Body)};
                {error, _} = Error ->
                    Error
            end;
        {undefined, true} ->
            maybe_continue(Transport, Socket, 1, Headers),
            case read_chunked(Transport, Socket, Max, Timeout, [], 0) of
                {ok, Body} -> {ok, request(Method, Uri, Version, Headers, Body)};
                {error, _} = Error -> Error
            end;
        {undefined, false} ->
            {ok, request(Method, Uri, Version, Headers, <<>>)}
    end.

request(Method, Uri, Version, Headers, Body) ->
    #{method => method(Method),
      path => path(Uri),
      version => Version,
      headers => Headers,
      body => Body,
      keep_alive => keep_alive(Version, Headers)}.

read_headers(Transport, Socket, Timeout, Acc) ->
    case Transport:recv(Socket, 0, Timeout) of
        {ok, {http_header, _Len, Name, _Reserved, Value}} ->
            Key = normalize_name(Name),
            Next = case maps:find(Key, Acc) of
                       {ok, Existing} -> Acc#{Key => <<Existing/binary, ",", Value/binary>>};
                       error -> Acc#{Key => Value}
                   end,
            read_headers(Transport, Socket, Timeout, Next);
        {ok, http_eoh} ->
            {ok, Acc};
        {ok, _Other} ->
            read_headers(Transport, Socket, Timeout, Acc);
        {error, Reason} ->
            {error, Reason}
    end.

content_length(Headers) ->
    case header(<<"content-length">>, Headers) of
        undefined -> undefined;
        Value ->
            try binary_to_integer(trim(Value)) of
                N when N >= 0 -> {ok, N};
                _ -> {error, invalid_content_length}
            catch
                error:badarg -> {error, invalid_content_length}
            end
    end.

is_chunked(Headers) ->
    case header(<<"transfer-encoding">>, Headers) of
        undefined -> false;
        Value -> lists:member(<<"chunked">>, [lower(trim(V))
                                              || V <- binary:split(Value, <<",">>, [global])])
    end.

maybe_continue(Transport, Socket, Length, Headers) when Length > 0 ->
    case header(<<"expect">>, Headers) of
        undefined -> ok;
        Value ->
            case lower(trim(Value)) of
                <<"100-continue">> ->
                    _ = Transport:send(Socket, <<"HTTP/1.1 100 Continue\r\n\r\n">>),
                    ok;
                _ -> ok
            end
    end;
maybe_continue(_Transport, _Socket, _Length, _Headers) ->
    ok.

read_exact_raw(Transport, Socket, Length, Timeout) ->
    case setopts(Transport, Socket, [{packet, raw}]) of
        ok -> read_exact(Transport, Socket, Length, Timeout, []);
        {error, Reason} -> {error, Reason}
    end.

read_exact(_Transport, _Socket, 0, _Timeout, Acc) ->
    {ok, iolist_to_binary(lists:reverse(Acc))};
read_exact(Transport, Socket, Remaining, Timeout, Acc) ->
    case Transport:recv(Socket, Remaining, Timeout) of
        {ok, Data} when byte_size(Data) =< Remaining ->
            read_exact(Transport, Socket, Remaining - byte_size(Data),
                       Timeout, [Data | Acc]);
        {ok, _TooMuch} ->
            {error, protocol_error};
        {error, Reason} ->
            {error, Reason}
    end.

read_chunked(Transport, Socket, Max, Timeout, Acc, Total) ->
    case setopts(Transport, Socket, [{packet, line}]) of
        ok ->
            case Transport:recv(Socket, 0, Timeout) of
                {ok, Line} ->
                    chunk_line(Transport, Socket, Max, Timeout, Acc, Total, Line);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

chunk_line(Transport, Socket, Max, Timeout, Acc, Total, Line) ->
    case chunk_size(Line) of
        {ok, 0} ->
            _ = skip_trailers(Transport, Socket, Timeout),
            {ok, iolist_to_binary(lists:reverse(Acc))};
        {ok, Size} when Total + Size > Max ->
            {error, payload_too_large};
        {ok, Size} ->
            case read_exact_raw(Transport, Socket, Size, Timeout) of
                {ok, Data} ->
                    case read_crlf(Transport, Socket, Timeout) of
                        ok -> read_chunked(Transport, Socket, Max, Timeout,
                                           [Data | Acc], Total + Size);
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        error ->
            {error, invalid_chunk}
    end.

chunk_size(Line) ->
    [SizePart | _Ext] = binary:split(Line, <<";">>),
    try binary_to_integer(trim(SizePart), 16) of
        N when N >= 0 -> {ok, N};
        _ -> error
    catch
        error:badarg -> error
    end.

read_crlf(Transport, Socket, Timeout) ->
    case setopts(Transport, Socket, [{packet, raw}]) of
        ok ->
            case Transport:recv(Socket, 2, Timeout) of
                {ok, <<"\r\n">>} -> ok;
                {ok, _Other} -> {error, invalid_chunk};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

skip_trailers(Transport, Socket, Timeout) ->
    case setopts(Transport, Socket, [{packet, line}]) of
        ok ->
            case Transport:recv(Socket, 0, Timeout) of
                {ok, <<"\r\n">>} -> ok;
                {ok, _Line} -> skip_trailers(Transport, Socket, Timeout);
                {error, _} -> ok
            end;
        {error, _} ->
            ok
    end.

%%%_* Internal --------------------------------------------------------------

setopts(Transport, Socket, Opts) ->
    case Transport:setopts(Socket, Opts) of
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end.

method('GET') -> get;
method('POST') -> post;
method('PUT') -> put;
method('DELETE') -> delete;
method('HEAD') -> head;
method('OPTIONS') -> options;
method(Other) when is_atom(Other) -> Other;
method(Other) when is_binary(Other) -> lower(Other);
method(Other) -> Other.

path({abs_path, Path}) -> Path;
path({absoluteURI, _Scheme, _Host, _Port, Path}) -> Path;
path('*') -> <<"*">>;
path(Other) when is_binary(Other) -> Other;
path(_Other) -> <<"/">>.

normalize_name(Name) when is_atom(Name) -> lower(atom_to_binary(Name, utf8));
normalize_name(Name) when is_binary(Name) -> lower(Name).

has_header(Name, Headers) ->
    lists:any(fun({K, _V}) -> lower(K) =:= Name end, Headers).

lower(Bin) when is_binary(Bin) ->
    << <<(lower_byte(B))/integer>> || <<B/integer>> <= Bin >>.

lower_byte(B) when B >= $A, B =< $Z -> B + 32;
lower_byte(B) -> B.

trim(Bin) when is_binary(Bin) ->
    trim_left(Bin).

trim_left(<<C, Rest/binary>>)
  when C =:= $\s; C =:= $\t; C =:= $\r; C =:= $\n ->
    trim_left(Rest);
trim_left(Bin) ->
    trim_right(Bin, byte_size(Bin)).

trim_right(Bin, 0) ->
    Bin;
trim_right(Bin, N) ->
    case binary:at(Bin, N - 1) of
        C when C =:= $\s; C =:= $\t; C =:= $\r; C =:= $\n ->
            trim_right(Bin, N - 1);
        _ ->
            binary:part(Bin, 0, N)
    end.

reason(100) -> <<"Continue">>;
reason(200) -> <<"OK">>;
reason(201) -> <<"Created">>;
reason(202) -> <<"Accepted">>;
reason(204) -> <<"No Content">>;
reason(304) -> <<"Not Modified">>;
reason(400) -> <<"Bad Request">>;
reason(401) -> <<"Unauthorized">>;
reason(403) -> <<"Forbidden">>;
reason(404) -> <<"Not Found">>;
reason(405) -> <<"Method Not Allowed">>;
reason(413) -> <<"Content Too Large">>;
reason(415) -> <<"Unsupported Media Type">>;
reason(429) -> <<"Too Many Requests">>;
reason(500) -> <<"Internal Server Error">>;
reason(503) -> <<"Service Unavailable">>;
reason(_Other) -> <<"Unknown">>.
