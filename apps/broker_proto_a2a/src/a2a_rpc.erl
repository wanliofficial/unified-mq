%% @doc A2A JSON-RPC dispatch (S9-A scope: one method, `SendMessage', echoed).
%%
%% This module is deliberately small: it proves that the transport, version
%% negotiation, authentication, ACL and error model work end to end. No Task is
%% created, no state is written -- `SendMessage' answers with a `Message' (the
%% `SendMessageResponse.message' branch of the A2A 1.0.0 oneof), which is a
%% legal response without a task lifecycle. S9-B replaces the echo with the task
%% state machine and the `$a2a/task/<id>' log (ADR-025).
%%
%% Ordering is deliberate: media type, credential, version, envelope. A client
%% that presents no credential learns nothing about its JSON, and a version it
%% cannot speak never reaches method dispatch.
-module(a2a_rpc).

-export([handle/1, max_body_bytes/0, request_timeout_ms/0,
         require_auth/0, supported_versions/0]).

-spec handle(map()) -> {non_neg_integer(), [{binary(), binary()}], iodata(), boolean()}.
handle(#{method := post, path := Path} = Req) ->
    case rpc_path(Path) of
        true -> json_rpc(Req);
        false -> {404, json_headers(), error_body(<<"not_found">>), keep(Req)}
    end;
handle(#{method := get, path := <<"/health">>} = Req) ->
    {200, json_headers(), error_body(<<"ok">>), keep(Req)};
handle(#{method := Method} = Req)
  when Method =:= get; Method =:= post; Method =:= put; Method =:= delete ->
    {405, [{<<"allow">>, <<"GET, POST">>} | json_headers()],
     error_body(<<"method_not_allowed">>), keep(Req)};
handle(#{method := _Method} = Req) ->
    {405, [{<<"allow">>, <<"GET, POST">>} | json_headers()],
     error_body(<<"method_not_allowed">>), keep(Req)}.

%%%_* Transport-level gates --------------------------------------------------

json_rpc(Req) ->
    Headers = maps:get(headers, Req),
    Keep = keep(Req),
    case agent_rpc_http:accepts_json(Headers) of
        false ->
            {415, json_headers(), <<>>, Keep};
        true ->
            authenticated(Req, Keep)
    end.

authenticated(Req, Keep) ->
    Headers = maps:get(headers, Req),
    case require_auth() of
        false ->
            after_auth(Req, agent_rpc_auth:anonymous(), Keep);
        true ->
            case agent_rpc_auth:credential(Headers) of
                none -> unauthorized(Keep);
                {error, ambiguous} -> unauthorized(Keep);
                {ok, Credential} ->
                    case agent_rpc_auth:authenticate(Credential) of
                        {ok, Identity} -> after_auth(Req, Identity, Keep);
                        {error, unauthorized} -> unauthorized(Keep)
                    end
            end
    end.

unauthorized(Keep) ->
    {401, [{<<"www-authenticate">>, agent_rpc_auth:challenge()} | json_headers()],
     <<>>, Keep}.

after_auth(Req, Identity, Keep) ->
    Version = agent_rpc_http:header(<<"a2a-version">>, maps:get(headers, Req)),
    case agent_rpc_version:parse(Version) of
        {ok, Parsed} ->
            case agent_rpc_version:negotiate(Parsed, supported_versions()) of
                ok -> decode(Req, Identity, Keep);
                {error, version_not_supported} -> version_error(Keep)
            end;
        {error, version_not_supported} ->
            version_error(Keep)
    end.

version_error(Keep) ->
    %% The envelope has not been parsed yet, so the response id is null: there
    %% is no request id to echo (JSON-RPC 2.0 §5).
    Body = agent_rpc_json:response_error(
             null, version_not_supported,
             #{<<"supported">> => supported_versions()}),
    {400, json_headers(), agent_rpc_json:encode(Body), Keep}.

decode(Req, Identity, Keep) ->
    case agent_rpc_json:decode(maps:get(body, Req)) of
        {error, parse_error} ->
            Body = agent_rpc_json:response_error(null, parse_error, undefined),
            {400, json_headers(), agent_rpc_json:encode(Body), Keep};
        {ok, Term} ->
            case agent_rpc_json:request(Term) of
                {error, invalid_request} ->
                    Body = agent_rpc_json:response_error(null, invalid_request, undefined),
                    {400, json_headers(), agent_rpc_json:encode(Body), Keep};
                {ok, Rpc} ->
                    dispatch(Rpc, Identity, Keep)
            end
    end.

%%%_* Methods ----------------------------------------------------------------

dispatch(#{method := <<"SendMessage">>, params := Params, id := Id}, Identity, Keep) ->
    case message(Params) of
        {ok, Message} ->
            case agent_rpc_auth:authorize(Identity, publish, <<"$a2a/task/echo">>) of
                allow ->
                    Result = #{<<"message">> => echo(Message)},
                    {200, json_headers(),
                     agent_rpc_json:encode(agent_rpc_json:response_ok(Id, Result)), Keep};
                deny ->
                    %% ACL is a transport decision, not a JSON-RPC error: the
                    %% request was understood and refused.
                    {403, json_headers(), <<>>, Keep}
            end;
        {error, invalid_params} ->
            {200, json_headers(),
             agent_rpc_json:encode(
               agent_rpc_json:response_error(Id, invalid_params, undefined)), Keep}
    end;
dispatch(#{method := _Other, id := Id}, _Identity, Keep) ->
    {200, json_headers(),
     agent_rpc_json:encode(
       agent_rpc_json:response_error(Id, method_not_found, undefined)), Keep}.

%% @doc Validate the parts of `SendMessageRequest' this stage promises to
%% understand. Everything else in the proto (configuration, push config,
%% history length) is accepted by the client but not read yet -- S9-B/S9-C add
%% the semantics.
message(#{<<"message">> := Message}) when is_map(Message) ->
    case {maps:get(<<"messageId">>, Message, undefined),
          maps:get(<<"role">>, Message, undefined),
          maps:get(<<"parts">>, Message, undefined)} of
        {Id, Role, Parts} when is_binary(Id), Id =/= <<>>,
                               (Role =:= <<"ROLE_USER">> orelse Role =:= <<"ROLE_AGENT">>),
                               is_list(Parts), Parts =/= [] ->
            case lists:all(fun valid_part/1, Parts) of
                true -> {ok, Message};
                false -> {error, invalid_params}
            end;
        _ ->
            {error, invalid_params}
    end;
message(_Params) ->
    {error, invalid_params}.

valid_part(Part) when is_map(Part) ->
    Content = [Key || Key <- [<<"text">>, <<"raw">>, <<"url">>, <<"data">>],
                      maps:is_key(Key, Part)],
    length(Content) =:= 1;
valid_part(_Other) ->
    false.

%% A fresh `messageId' is mandatory: echoing the client's id would make the
%% response look like a duplicate of the request to any deduplicating client.
echo(Message) ->
    Base = #{<<"messageId">> => new_id(),
             <<"role">> => <<"ROLE_AGENT">>,
             <<"parts">> => maps:get(<<"parts">>, Message),
             <<"contextId">> => maps:get(<<"contextId">>, Message, new_id())},
    case maps:find(<<"metadata">>, Message) of
        {ok, Metadata} -> Base#{<<"metadata">> => Metadata};
        error -> Base
    end.

new_id() ->
    url_base64(crypto:strong_rand_bytes(16)).

url_base64(Bin) ->
    Encoded = base64:encode(Bin),
    iolist_to_binary([url_char(B) || <<B/integer>> <= Encoded, B =/= $=]).

url_char($+) -> $-;
url_char($/) -> $_;
url_char(B) -> B.

%%%_* Config ----------------------------------------------------------------

-spec max_body_bytes() -> non_neg_integer().
max_body_bytes() ->
    Configured = application:get_env(broker_proto_a2a, max_body_bytes, 1048576),
    min(Configured, broker_limits:max_message_bytes()).

-spec request_timeout_ms() -> pos_integer().
request_timeout_ms() ->
    application:get_env(broker_proto_a2a, request_timeout_ms, 30000).

-spec require_auth() -> boolean().
require_auth() ->
    application:get_env(broker_proto_a2a, require_auth, true).

-spec supported_versions() -> [binary()].
supported_versions() ->
    application:get_env(broker_proto_a2a, supported_versions, [<<"1.0">>]).

%%%_* Internal --------------------------------------------------------------

rpc_path(<<"/">>) -> true;
rpc_path(<<"/rpc">>) -> true;
rpc_path(_Other) -> false.

keep(#{keep_alive := Keep}) -> Keep;
keep(_) -> false.

json_headers() ->
    [{<<"content-type">>, <<"application/json">>}].

error_body(<<"ok">>) ->
    <<"{\"status\":\"ok\"}">>;
error_body(Reason) ->
    agent_rpc_json:encode(#{<<"error">> => Reason}).
