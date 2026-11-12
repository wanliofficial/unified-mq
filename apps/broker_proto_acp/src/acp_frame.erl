%% @doc Newline-delimited JSON-RPC framing for ACP (ADR-026 §D5).
%%
%% ACP over stdio is "one JSON-RPC message per line, UTF-8, no batch" (`v2' adds
%% the no-null-id rule; v1 already forbids batch in practice). This module owns
%% exactly three pure things, so the client's state machine never touches a
%% binary:
%%
%%   * `split/2'   -- feed bytes, get back whole lines plus the leftover tail
%%                    (a frame may arrive across reads, and several may arrive
%%                    in one read);
%%   * `decode/1'  -- a line to a term, `parse_error' on anything malformed;
%%   * `request/3', `response_ok/2', `response_error/3', `notification/2' --
%%                    the envelope builders, all routed through the shared
%%                    `agent_rpc_json' code so ACP adds no second envelope.
%%
%% Nothing here interprets a method: the client decides what a message means.
-module(acp_frame).

-export([split/2, decode/1, encode/1,
         request/3, notification/2, response_ok/2, response_error/3,
         id_of/1, method_of/1, params_of/1, kind/1]).

-type message_id() :: integer() | binary() | null.

%% @doc Split accumulated bytes into complete lines. Returns them **without** the
%% line terminator, plus the trailing partial line to carry into the next read.
%% A `\r' before the newline is stripped: ACP says UTF-8/LF, but a Windows agent
%% emitting CRLF still parses instead of failing on a stray byte.
-spec split(binary(), binary()) -> {[binary()], binary()}.
split(Buffer, Chunk) ->
    Acc = <<Buffer/binary, Chunk/binary>>,
    split_lines(Acc, []).

split_lines(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
split_lines(Acc, Lines) ->
    case binary:split(Acc, <<"\n">>) of
        [Line, Rest] ->
            split_lines(Rest, [strip_cr(Line) | Lines]);
        [Partial] ->
            {lists:reverse(Lines), Partial}
    end.

strip_cr(Line) ->
    case Line of
        <<>> -> <<>>;
        _ ->
            case binary:last(Line) of
                $\r -> binary:part(Line, 0, byte_size(Line) - 1);
                _ -> Line
            end
    end.

-spec decode(binary()) -> {ok, term()} | {error, parse_error}.
decode(Line) when is_binary(Line), Line =/= <<>> ->
    agent_rpc_json:decode(Line);
decode(_Empty) ->
    {error, parse_error}.

-spec encode(term()) -> iodata().
encode(Term) ->
    agent_rpc_json:encode(Term).

%%%_* Envelope builders ------------------------------------------------------

%% @doc An outbound request (Client -> Agent). `Id' is chosen by the client and
%% never reused while pending.
-spec request(message_id(), binary(), map()) -> map().
request(Id, Method, Params) ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => Id,
      <<"method">> => Method, <<"params">> => Params}.

-spec notification(binary(), map()) -> map().
notification(Method, Params) ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => Method,
      <<"params">> => Params}.

-spec response_ok(message_id(), term()) -> map().
response_ok(Id, Result) ->
    agent_rpc_json:response_ok(Id, Result).

-spec response_error(message_id(), atom(), term()) -> map().
response_error(Id, Reason, Data) ->
    agent_rpc_json:response_error(Id, Reason, Data).

%%%_* Classification ---------------------------------------------------------

%% @doc `request' has an `id' and a `method'; `notification' has only a method;
%% `response' has an `id' and no method. A message with neither is `invalid'.
-spec kind(term()) -> request | notification | response | invalid.
kind(#{<<"method">> := M, <<"id">> := _} = Term) when is_binary(M) ->
    case maps:is_key(<<"jsonrpc">>, Term) of
        true -> request;
        false -> invalid
    end;
kind(#{<<"method">> := M} = Term) when is_binary(M) ->
    case maps:is_key(<<"jsonrpc">>, Term) of
        true -> notification;
        false -> invalid
    end;
kind(#{<<"id">> := _} = Term) ->
    case maps:is_key(<<"jsonrpc">>, Term) of
        true -> response;
        false -> invalid
    end;
kind(_) ->
    invalid.

-spec id_of(term()) -> message_id() | undefined.
id_of(#{<<"id">> := Id}) -> Id;
id_of(_) -> undefined.

-spec method_of(term()) -> binary() | undefined.
method_of(#{<<"method">> := M}) when is_binary(M) -> M;
method_of(_) -> undefined.

-spec params_of(term()) -> map().
params_of(#{<<"params">> := P}) when is_map(P) -> P;
params_of(#{<<"params">> := P}) when is_list(P) -> maps:from_list(P);
params_of(_) -> #{}.
