%% @doc Streamable HTTP transport for MCP 2026-07-28 (ADR-027 §D7).
%%
%% One endpoint, POST only. This module owns exactly the transport layer:
%%
%%   1. `Origin' -- present and not allowed is 403 (DNS-rebinding defence);
%%   2. `Accept' -- must list both `application/json' and `text/event-stream';
%%   3. method   -- anything but POST is 405 with `Allow: POST';
%%   4. `Content-Type' -- must be `application/json';
%%   5. `MCP-Protocol-Version' -- required, must equal the body's `_meta'
%%      protocol version;
%%   6. `Mcp-Method' -- required, must equal the body `method';
%%   7. `Mcp-Name' -- required for the three methods that carry one, must equal
%%      the body value after Base64-sentinel decoding;
%%   8. authentication -- `broker_security' opaque token;
%%   9. envelope + method semantics -- handed to `mcp_dispatch'.
%%
%% Every gate failure is a `-32020' (HeaderMismatch, HTTP 400) except Origin
%% (403) and Accept (406), exactly as `streamable-http#request-metadata' and
%% `#server-validation' specify. The version-unsupported case is `-32022' with
%% the supported list, and an unknown method is `-32601' on 404.
-module(mcp_rpc).

-export([handle/1, max_body_bytes/0, request_timeout_ms/0, path/0]).

-type req() :: agent_rpc_http:request().

%% @doc Answer one HTTP request. `{stream, Plan}' means the caller must hand the
%% socket to `mcp_stream:run/3' (the SSE response owns the connection).
-spec handle(req()) ->
          {json, pos_integer(), [{binary(), binary()}], iodata(), boolean()}
        | {no_content, boolean()}
        | {stream, map()}.
handle(#{path := RequestPath} = Req) ->
    case path_matches(RequestPath) of
        true -> handle_method(Req);
        false -> {json, 404, json_headers(), <<>>, keep(Req)}
    end.

handle_method(#{method := post} = Req) ->
    post(Req);
handle_method(#{method := _Other} = Req) ->
    %% The 2026-07-28 revision removed the GET stream and DELETE session
    %% termination; a legacy client probing them gets 405 and `Allow: POST'.
    {json, 405, [{<<"allow">>, <<"POST">>} | json_headers()], <<>>, keep(Req)}.

%%%_* Transport gates -------------------------------------------------------

post(Req) ->
    Headers = maps:get(headers, Req),
    Keep = keep(Req),
    case gates(Headers) of
        ok -> body(Req, Keep);
        {error, Response} -> Response
    end.

gates(Headers) ->
    case origin_allowed(Headers) of
        false ->
            %% Bodiless on purpose: the request was refused before it became a
            %% JSON-RPC message.
            {error, {json, 403, [], <<>>, false}};
        true ->
            case accept_ok(Headers) of
                false ->
                    {error, {json, 406, json_headers(), <<>>, false}};
                true ->
                    case content_type_json(Headers) of
                        true -> ok;
                        false -> {error, {json, 415, json_headers(), <<>>, false}}
                    end
            end
    end.

%% Parse first (the header rules compare against body fields), then enforce
%% header/body consistency, then authenticate, then dispatch (ADR-027 §D7).
body(Req, Keep) ->
    Headers = maps:get(headers, Req),
    case agent_rpc_json:decode(maps:get(body, Req)) of
        {error, parse_error} ->
            with_identity(Req, Keep,
                          fun(_Identity) ->
                                  error_body(null, parse_error, 400, Keep)
                          end);
        {ok, Term} ->
            case mcp_dispatch:shape(Term) of
                {ok, request, Request} ->
                    case standard_headers(Headers, Request, Keep) of
                        ok ->
                            with_identity(
                              Req, Keep,
                              fun(Identity) -> dispatch(Request, Identity, Keep) end);
                        {error, Response} ->
                            Response
                    end;
                {ok, notification, _Notification} ->
                    %% Header requirements for notification POSTs are not
                    %% defined by this revision; accept any well-formed one.
                    with_identity(Req, Keep, fun(_Identity) -> {no_content, Keep} end);
                {error, invalid_request} ->
                    with_identity(Req, Keep,
                                  fun(_Identity) ->
                                          error_body(null, invalid_request, 400, Keep)
                                  end)
            end
    end.

%% Gates 5-7: the mirrored headers must agree with the body. A missing required
%% header is the same failure as a mismatching one (-32020).
standard_headers(Headers, Request, Keep) ->
    Method = maps:get(<<"method">>, Request),
    Params = maps:get(<<"params">>, Request, #{}),
    Id = maps:get(<<"id">>, Request, null),
    BodyVersion = body_version(Params),
    case required_header(<<"mcp-protocol-version">>, Headers) of
        missing ->
            {error, header_error(Id, <<"MCP-Protocol-Version header is required">>,
                                 Keep)};
        {ok, HeaderVersion} ->
            case HeaderVersion =:= BodyVersion of
                true -> standard_method_header(Headers, Method, Request, Id, Keep);
                false ->
                    {error,
                     header_error(
                       Id,
                       <<"MCP-Protocol-Version header does not match the request body">>,
                       Keep)}
            end
    end.

standard_method_header(Headers, Method, Request, Id, Keep) ->
    case required_header(<<"mcp-method">>, Headers) of
        missing ->
            {error, header_error(Id, <<"Mcp-Method header is required">>, Keep)};
        {ok, Method} ->
            standard_name_header(Headers, Method, Request, Id, Keep);
        {ok, _Other} ->
            {error, header_error(Id, <<"Mcp-Method header does not match the request body">>,
                                 Keep)}
    end.

%% `Mcp-Name' is required for `tools/call' (`params.name'), `resources/read'
%% (`params.uri') and `prompts/get' (`params.name'), and optional elsewhere.
standard_name_header(Headers, Method, Request, Id, Keep) ->
    Params = maps:get(<<"params">>, Request, #{}),
    case name_value(Method, Params) of
        undefined ->
            ok;
        Expected ->
            case required_header(<<"mcp-name">>, Headers) of
                missing ->
                    {error, header_error(Id, <<"Mcp-Name header is required">>, Keep)};
                {ok, HeaderValue} ->
                    case decode_sentinel(HeaderValue) of
                        Expected -> ok;
                        _Other ->
                            {error,
                             header_error(
                               Id,
                               <<"Mcp-Name header does not match the request body">>,
                               Keep)}
                    end
            end
    end.

name_value(<<"tools/call">>, Params) -> string_param(<<"name">>, Params);
name_value(<<"prompts/get">>, Params) -> string_param(<<"name">>, Params);
name_value(<<"resources/read">>, Params) -> string_param(<<"uri">>, Params);
name_value(_Method, _Params) -> undefined.

string_param(Key, Params) when is_map(Params) ->
    case maps:get(Key, Params, undefined) of
        Value when is_binary(Value) -> Value;
        _ -> undefined
    end;
string_param(_Key, _Params) ->
    undefined.

body_version(Params) when is_map(Params) ->
    case maps:get(<<"_meta">>, Params, #{}) of
        Meta when is_map(Meta) ->
            maps:get(<<"io.modelcontextprotocol/protocolVersion">>, Meta, undefined);
        _ ->
            undefined
    end;
body_version(_Params) ->
    undefined.

required_header(Name, Headers) ->
    case agent_rpc_http:header(Name, Headers) of
        undefined -> missing;
        <<>> -> missing;
        Value -> {ok, Value}
    end.

%% `=?base64?...?=' wraps a value that cannot be sent as a plain ASCII header
%% (`streamable-http#value-encoding'); servers MUST decode before comparing.
decode_sentinel(<<"=?base64?", Rest/binary>>) ->
    %% `?=' closes an encoded value; anything else is a literal header.
    case byte_size(Rest) >= 2 of
        true ->
            EncodedSize = byte_size(Rest) - 2,
            case binary:part(Rest, EncodedSize, 2) of
                <<"?=">> ->
                    Encoded = binary:part(Rest, 0, EncodedSize),
                    try base64:decode(Encoded) of
                        Decoded -> Decoded
                    catch
                        _:_ -> <<"=?base64?", Rest/binary>>
                    end;
                _ ->
                    <<"=?base64?", Rest/binary>>
            end;
        false ->
            <<"=?base64?", Rest/binary>>
    end;
decode_sentinel(Value) ->
    Value.

%%%_* Authentication --------------------------------------------------------

with_identity(Req, Keep, Fun) ->
    Headers = maps:get(headers, Req),
    case require_auth() of
        false ->
            Fun(agent_rpc_auth:anonymous());
        true ->
            case agent_rpc_auth:credential(Headers) of
                none ->
                    unauthorized(Keep);
                {error, ambiguous} ->
                    unauthorized(Keep);
                {ok, Credential} ->
                    case agent_rpc_auth:authenticate(Credential) of
                        {ok, Identity} -> Fun(Identity);
                        {error, unauthorized} -> unauthorized(Keep)
                    end
            end
    end.

unauthorized(Keep) ->
    {json, 401, [{<<"www-authenticate">>, challenge()} | json_headers()], <<>>, Keep}.

%%%_* Dispatch --------------------------------------------------------------

dispatch(Request, Identity, Keep) ->
    Context = #{transport => http, identity => Identity},
    case mcp_dispatch:handle(Request, Context) of
        no_reply ->
            {no_content, Keep};
        {bare, Status} ->
            {json, Status, json_headers(), <<>>, Keep};
        {json, Status, Object} ->
            {json, Status, json_headers(), agent_rpc_json:encode(Object), Keep};
        {stream, Plan} ->
            {stream, Plan#{keep_alive => Keep}}
    end.

%%%_* Responses -------------------------------------------------------------

error_body(Id, Reason, Status, Keep) ->
    Body = agent_rpc_json:response_error(Id, Reason, undefined),
    {json, Status, json_headers(), agent_rpc_json:encode(Body), Keep}.

header_error(Id, Message, Keep) ->
    Body = agent_rpc_json:response_error(Id, header_mismatch, Message, undefined),
    {json, 400, json_headers(), agent_rpc_json:encode(Body), Keep}.

json_headers() ->
    [{<<"content-type">>, <<"application/json">>}].

challenge() ->
    <<"Bearer realm=\"mcp\"">>.

%%%_* Transport predicates --------------------------------------------------

path_matches(RequestPath) when is_binary(RequestPath) ->
    Path = hd(binary:split(RequestPath, <<"?">>)),
    Path =:= path().

origin_allowed(Headers) ->
    case agent_rpc_http:header(<<"origin">>, Headers) of
        undefined -> true;
        Origin -> origin_ok(Origin)
    end.

origin_ok(Origin) ->
    case origin_host(Origin) of
        {ok, Host} ->
            case is_loopback_host(Host) of
                true -> true;
                false -> lists:member(Host, allowed_origins())
            end;
        error ->
            false
    end.

origin_host(Origin) ->
    case binary:split(Origin, <<"://">>) of
        [_Scheme, Rest] ->
            Authority = hd(binary:split(Rest, <<"/">>)),
            HostPort = case binary:split(Authority, <<"@">>) of
                           [_UserInfo, HostPart] -> HostPart;
                           [HostPart] -> HostPart
                       end,
            Host = case HostPort of
                       <<"[", _/binary>> -> HostPort;
                       _ -> hd(binary:split(HostPort, <<":">>))
                   end,
            case Host of
                <<>> -> error;
                _ -> {ok, lower(Host)}
            end;
        _ ->
            error
    end.

is_loopback_host(<<"localhost">>) -> true;
is_loopback_host(<<"[::1]">>) -> true;
is_loopback_host(Host) ->
    %% `binary:split/2' splits once, so the octet check needs the global form.
    case binary:split(Host, <<".">>, [global]) of
        [<<"127">>, _, _, _] -> true;
        _ -> false
    end.

%% `Accept' must list both media types (a wildcard covers both).
accept_ok(Headers) ->
    case agent_rpc_http:header(<<"accept">>, Headers) of
        undefined ->
            true;
        Accept ->
            Types = [media_base(T)
                     || T <- binary:split(Accept, <<",">>, [global])],
            HasJson = lists:member(<<"application/json">>, Types),
            HasSse = lists:member(<<"text/event-stream">>, Types),
            HasWild = lists:member(<<"*/*">>, Types),
            (HasJson orelse HasWild) andalso (HasSse orelse HasWild)
    end.

media_base(Type) ->
    [Base | _] = binary:split(Type, <<";">>),
    lower(trim(Base)).

content_type_json(Headers) ->
    case agent_rpc_http:content_type(Headers) of
        <<"application/json">> -> true;
        _ -> false
    end.

trim(Bin) ->
    string:trim(Bin).

lower(Bin) when is_binary(Bin) ->
    << <<(lower_byte(B))/integer>> || <<B/integer>> <= Bin >>.

lower_byte(B) when B >= $A, B =< $Z -> B + 32;
lower_byte(B) -> B.

%%%_* Config ---------------------------------------------------------------

keep(#{keep_alive := Keep}) -> Keep;
keep(_Req) -> false.

-spec path() -> binary().
path() ->
    application:get_env(broker_proto_mcp, path, <<"/">>).

-spec max_body_bytes() -> non_neg_integer().
max_body_bytes() ->
    Configured = application:get_env(broker_proto_mcp, max_body_bytes, 1048576),
    min(Configured, broker_limits:max_message_bytes()).

-spec request_timeout_ms() -> pos_integer().
request_timeout_ms() ->
    application:get_env(broker_proto_mcp, request_timeout_ms, 30000).

require_auth() ->
    application:get_env(broker_proto_mcp, require_auth, true).

allowed_origins() ->
    application:get_env(broker_proto_mcp, allowed_origins, []).
