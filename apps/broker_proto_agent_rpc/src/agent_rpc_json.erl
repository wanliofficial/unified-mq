%% @doc JSON-RPC 2.0 envelope: decode, validate, build responses.
%%
%% A2A §9.3 requires the standard JSON-RPC 2.0 shape. Three deliberate choices
%% narrow the envelope compared with the full spec, because A2A's binding uses
%% none of the extra surface and each omission would otherwise be a guess:
%%
%%   * **No batch.** A top-level array is `invalid_request'. A2A methods create
%%     tasks, and "some of the batch succeeded" is a semantics nobody asked for
%%     (recorded in the capability matrix).
%%   * **No notifications.** Every A2A method is request/response; an envelope
%%     without `id' is rejected instead of silently producing no response.
%%   * **`params' defaults to `#{}'.** Methods validate their own fields; the
%%     envelope only guarantees a map or a list.
-module(agent_rpc_json).

-export([decode/1, encode/1, request/1, response_ok/2,
         response_error/3, response_error/4]).
-export_type([request/0]).

-type request() :: #{id := id(), method := binary(), params := map() | list()}.
-type id() :: binary() | integer() | float() | null.

%% @doc Parse a request body. Any decode failure is `parse_error'; the caller
%% answers with `id = null' (JSON-RPC 2.0 §5).
-spec decode(binary()) -> {ok, term()} | {error, parse_error}.
decode(Bin) when is_binary(Bin) ->
    try json:decode(Bin) of
        Term -> {ok, Term}
    catch
        _:_ -> {error, parse_error}
    end;
decode(_Other) ->
    {error, parse_error}.

%% @doc Encode a term with OTP 28's built-in `json'. The result is an iolist;
%% callers hand it straight to the socket or wrap it in an SSE frame.
-spec encode(term()) -> iolist().
encode(Term) ->
    json:encode(Term).

%% @doc Validate the envelope. Field rules are JSON-RPC 2.0 §4 plus A2A §9.3:
%% `jsonrpc' exactly "2.0", `method' a non-empty string, `id' a string, number
%% or null, `params' a map or array when present.
-spec request(term()) -> {ok, request()} | {error, invalid_request | parse_error}.
request(Term) when is_map(Term) ->
    case maps:get(<<"jsonrpc">>, Term, undefined) of
        <<"2.0">> ->
            case {maps:find(<<"id">>, Term), maps:find(<<"method">>, Term)} of
                {{ok, Id}, {ok, Method}} when is_binary(Method), Method =/= <<>> ->
                    case valid_id(Id) of
                        true -> with_params(Term, Id, Method);
                        false -> {error, invalid_request}
                    end;
                _ ->
                    {error, invalid_request}
            end;
        _ ->
            {error, invalid_request}
    end;
request(_Term) ->
    %% Includes a top-level array (batch), a number, a string, null.
    {error, invalid_request}.

with_params(Term, Id, Method) ->
    case maps:find(<<"params">>, Term) of
        error ->
            {ok, #{id => Id, method => Method, params => #{}}};
        {ok, Params} when is_map(Params); is_list(Params) ->
            {ok, #{id => Id, method => Method, params => Params}};
        {ok, _Other} ->
            {error, invalid_request}
    end.

valid_id(null) -> true;
valid_id(Id) when is_binary(Id); is_integer(Id); is_float(Id) -> true;
valid_id(_) -> false.

-spec response_ok(id(), term()) -> map().
response_ok(Id, Result) ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => Id, <<"result">> => Result}.

-spec response_error(id(), atom(), term()) -> map().
response_error(Id, Reason, Data) ->
    response_error(Id, Reason, agent_rpc_error:message(Reason), Data).

-spec response_error(id(), atom(), binary(), term()) -> map().
response_error(Id, Reason, Msg, Data) ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => Id,
      <<"error">> => agent_rpc_error:jsonrpc_error(Reason, Msg, Data)}.
