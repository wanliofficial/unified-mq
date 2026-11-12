%% @doc A2A JSON-RPC dispatch (S9-B: task lifecycle).
%%
%% Transport gates run first (media type, credential, version, envelope); this
%% module then maps the four S9-B methods onto `a2a_task_registry'. The method
%% layer owns three decisions the registry must not make:
%%
%%   * authorization -- an ACL check happens before any write, with the
%%     identity's own tenant (a principal can never widen its tenant);
%%   * cross-tenant reads -- a task id from another tenant answers
%%     `task_not_found', identical to a nonexistent id, so ids are not an
%%     existence oracle;
%%   * error -> HTTP status mapping -- A2A §5.4 puts `task_not_found' on 404 and
%%     the other task errors on 400, unlike envelope errors which stay 200.
-module(a2a_rpc).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-export([handle/1, max_body_bytes/0, request_timeout_ms/0,
         require_auth/0, supported_versions/0, push_method_enabled/0]).

%% `handle/1' answers a plain HTTP response *or* hands the socket to the SSE
%% streamer. The five-tuple is the S9-A/B shape; `{stream, Plan}' means "200 +
%% chunked, then `a2a_stream:run/3' owns the socket until the task ends".

%% @doc HTTP entry point. Discovery GETs bypass JSON-RPC (and therefore the
%% `A2A-Version' negotiation, which a browser fetching a well-known card will
%% not send); everything else keeps the S9-A gate order.
-spec handle(map()) ->
          {non_neg_integer(), [{binary(), binary()}], iodata(), boolean()}
        | {stream, map()}.
handle(#{method := get, path := Path} = Req) when is_binary(Path) ->
    case a2a_card:route(Path) of
        {ok, Kind, Query} ->
            a2a_card:handle(Kind, Query, Req);
        error ->
            case Path of
                <<"/health">> ->
                    {200, json_headers(), error_body(<<"ok">>), keep(Req)};
                _ ->
                    method_not_allowed(Req)
            end
    end;
handle(#{method := post, path := Path} = Req) ->
    case rpc_path(Path) of
        true -> json_rpc(Req);
        false -> {404, json_headers(), error_body(<<"not_found">>), keep(Req)}
    end;
handle(#{method := Method} = Req)
  when Method =:= put; Method =:= delete ->
    method_not_allowed(Req);
handle(#{method := _Method} = Req) ->
    method_not_allowed(Req).

method_not_allowed(Req) ->
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
            with_authorization(
              Identity, publish, <<"$a2a/task/new">>, Keep,
              fun() ->
                      ContextId = maps:get(<<"contextId">>, Message, undefined),
                      rpc_result(Id,
                                 registry(create, [tenant(Identity), ContextId,
                                                   Message]),
                                 Keep)
              end);
        {error, invalid_params} ->
            rpc_error(Id, invalid_params, Keep)
    end;
dispatch(#{method := <<"GetTask">>, params := Params, id := Id}, Identity, Keep) ->
    case task_id(Params) of
        {ok, TaskId} ->
            with_authorization(
              Identity, subscribe, a2a_event:destination(TaskId), Keep,
              fun() -> rpc_result(Id, get_task(TaskId, tenant(Identity)), Keep) end);
        error ->
            rpc_error(Id, invalid_params, Keep)
    end;
dispatch(#{method := <<"ListTasks">>, params := Params, id := Id}, Identity, Keep) ->
    with_authorization(
      Identity, subscribe, <<"$a2a/task">>, Keep,
      fun() ->
              case list_query(Params) of
                  {ok, Filters, Page} ->
                      rpc_result(Id,
                                 list_tasks(tenant(Identity), Filters, Page),
                                 Keep);
                  error ->
                      rpc_error(Id, invalid_params, Keep)
              end
      end);
dispatch(#{method := <<"CancelTask">>, params := Params, id := Id}, Identity, Keep) ->
    case task_id(Params) of
        {ok, TaskId} ->
            with_authorization(
              Identity, publish, a2a_event:destination(TaskId), Keep,
              fun() ->
                      rpc_result(Id,
                                 registry_cancel(TaskId, tenant(Identity)), Keep)
              end);
        error ->
            rpc_error(Id, invalid_params, Keep)
    end;
dispatch(#{method := <<"GetExtendedAgentCard">>, params := Params, id := Id},
         Identity, Keep) ->
    with_authorization(
      Identity, subscribe, <<"$a2a/task">>, Keep,
      fun() ->
              rpc_result(Id, a2a_card:extended(tenant(Identity), Params), Keep)
      end);
%% S9-C: streaming. `SendStreamingMessage' creates the task then streams it, so
%% the first frame is the creation snapshot and the last one is the terminal
%% state (R-A1). `SubscribeToTask' streams an existing task from offset 0, which
%% is what makes a dropped connection recoverable (R-A2).
dispatch(#{method := <<"SendStreamingMessage">>, params := Params, id := Id},
         Identity, Keep) ->
    case message(Params) of
        {ok, Message} ->
            with_authorization(
              Identity, publish, <<"$a2a/task/new">>, Keep,
              fun() ->
                      ContextId = maps:get(<<"contextId">>, Message, undefined),
                      case registry(create, [tenant(Identity), ContextId,
                                             Message]) of
                          {ok, #{<<"task">> := #{<<"id">> := TaskId}}} ->
                              stream_plan(Id, TaskId, tenant(Identity), Keep);
                          {error, Reason} ->
                              rpc_error(Id, classify(Reason), Keep)
                      end
              end);
        {error, invalid_params} ->
            rpc_error(Id, invalid_params, Keep)
    end;
dispatch(#{method := <<"SubscribeToTask">>, params := Params, id := Id},
         Identity, Keep) ->
    case task_id(Params) of
        {ok, TaskId} ->
            with_authorization(
              Identity, subscribe, a2a_event:destination(TaskId), Keep,
              fun() ->
                      case get_task(TaskId, tenant(Identity)) of
                          {ok, _Task} ->
                              stream_plan(Id, TaskId, tenant(Identity), Keep);
                          {error, Reason} ->
                              rpc_error(Id, classify(Reason), Keep)
                      end
              end);
        error ->
            rpc_error(Id, invalid_params, Keep)
    end;
%% S9-C: push config CRUD (R-A4). Creation is gated on `push_enabled' *and* the
%% two-layer allowlist; when either says no, the answer is the spec's
%% `PushNotificationNotSupportedError' (-32003), never a silently accepted
%% config that will never fire (ADR-026 §D6.1).
dispatch(#{method := <<"CreateTaskPushNotificationConfig">>, params := Params,
           id := Id}, Identity, Keep) ->
    case push_config_request(Params, tenant(Identity)) of
        {ok, Spec} ->
            with_authorization(
              Identity, publish, a2a_event:destination(maps:get(task_id, Spec)),
              Keep,
              fun() -> rpc_result(Id, create_push_config(Spec), Keep) end);
        {error, invalid_params} ->
            rpc_error(Id, invalid_params, Keep)
    end;
dispatch(#{method := <<"GetTaskPushNotificationConfig">>, params := Params,
           id := Id}, Identity, Keep) ->
    case push_config_id(Params, tenant(Identity)) of
        {ok, Tenant, TaskId, ConfigId} ->
            with_authorization(
              Identity, subscribe, a2a_event:destination(TaskId), Keep,
              fun() ->
                      rpc_result(Id,
                                 get_push_config(Tenant, TaskId, ConfigId), Keep)
              end);
        {error, invalid_params} ->
            rpc_error(Id, invalid_params, Keep)
    end;
dispatch(#{method := <<"ListTaskPushNotificationConfigs">>, params := Params,
           id := Id}, Identity, Keep) ->
    case task_only(Params, tenant(Identity)) of
        {ok, Tenant, TaskId} ->
            with_authorization(
              Identity, subscribe, a2a_event:destination(TaskId), Keep,
              fun() -> rpc_result(Id, list_push_configs(Tenant, TaskId), Keep) end);
        {error, invalid_params} ->
            rpc_error(Id, invalid_params, Keep)
    end;
dispatch(#{method := <<"DeleteTaskPushNotificationConfig">>, params := Params,
           id := Id}, Identity, Keep) ->
    case push_config_id(Params, tenant(Identity)) of
        {ok, Tenant, TaskId, ConfigId} ->
            with_authorization(
              Identity, publish, a2a_event:destination(TaskId), Keep,
              fun() ->
                      rpc_result(Id,
                                 delete_push_config(Tenant, TaskId, ConfigId),
                                 Keep)
              end);
        {error, invalid_params} ->
            rpc_error(Id, invalid_params, Keep)
    end;
dispatch(#{method := _Other, id := Id}, _Identity, Keep) ->
    rpc_error(Id, method_not_found, Keep).

%%%_* Method bodies ---------------------------------------------------------

get_task(TaskId, TenantId) ->
    case a2a_task_registry:get(TaskId) of
        {ok, Proj} ->
            case a2a_task_projection:tenant_id(Proj) =:= TenantId of
                true -> {ok, a2a_task_projection:to_wire(Proj)};
                false -> {error, task_not_found}
            end;
        not_found ->
            {error, task_not_found}
    end.

registry_cancel(TaskId, TenantId) ->
    case a2a_task_registry:cancel(TaskId, TenantId) of
        {ok, Proj} -> {ok, a2a_task_projection:to_wire(Proj)};
        {error, Reason} -> {error, Reason}
    end.

list_tasks(TenantId, Filters, Page) ->
    {ok, Result} = a2a_task_registry:list(TenantId, Filters, Page),
    Base = #{<<"tasks">> => maps:get(tasks, Result)},
    case maps:get(next_page_token, Result) of
        undefined -> {ok, Base};
        Token -> {ok, Base#{<<"nextPageToken">> => Token}}
    end.

%% Thin wrapper so a registry crash becomes a JSON-RPC internal error rather
%% than an HTTP 500 with a stack trace: the request was understood.
registry(create, [TenantId, ContextId, Message]) ->
    case a2a_task_registry:create(TenantId, ContextId, Message) of
        {ok, Proj} -> {ok, #{<<"task">> => a2a_task_projection:to_wire(Proj)}};
        {error, Reason} -> {error, Reason}
    end.

%%%_* Streaming (S9-C) ------------------------------------------------------

%% @doc The plan a stream needs: which destination to follow, which partition
%% it lives on, and the request id every frame echoes. The partition comes from
%% the same router call the append path used, so a stream can never watch the
%% wrong log.
stream_plan(Id, TaskId, TenantId, Keep) ->
    case a2a_stream_plan(Id, TaskId, TenantId) of
        {ok, Plan} -> {stream, Plan};
        {error, Reason} -> rpc_error(Id, classify(Reason), Keep)
    end.

a2a_stream_plan(Id, TaskId, TenantId) ->
    Destination = a2a_event:destination(TaskId),
    case broker_router:env() of
        {ok, Env} ->
            Ctx = #route_ctx{tenant_id = TenantId, publisher = <<"a2a">>,
                             reliability = fsync},
            try broker_router_hash:route(Destination, Ctx, Env) of
                {ok, [{Partition, _Refs} | _]} ->
                    {ok, #{id => Id,
                           task_id => TaskId,
                           destination => Destination,
                           partition => Partition}};
                _ ->
                    {error, internal_error}
            catch
                _:_ -> {error, internal_error}
            end;
        {error, _} ->
            {error, internal_error}
    end.

%%%_* Push config CRUD (S9-C) ----------------------------------------------

push_method_enabled() ->
    application:get_env(broker_proto_a2a, push_enabled, false).

%% `CreateTaskPushNotificationConfig' params:
%%   {"taskId": "...", "config": {"url": "...", "authentication": {"scheme": "..."}}}
push_config_request(Params, TenantId) when is_map(Params) ->
    case {maps:get(<<"taskId">>, Params, undefined),
          maps:get(<<"config">>, Params, undefined)} of
        {TaskId, Config} when is_binary(TaskId), TaskId =/= <<>>, is_map(Config) ->
            case maps:get(<<"url">>, Config, undefined) of
                Url when is_binary(Url), Url =/= <<>> ->
                    Scheme = auth_scheme(Config),
                    {ok, #{tenant_id => TenantId,
                           task_id => TaskId,
                           url => Url,
                           auth_scheme => Scheme,
                           allow_private => maps:get(<<"allowPrivate">>, Config,
                                                     false)}};
                _ ->
                    {error, invalid_params}
            end;
        _ ->
            {error, invalid_params}
    end;
push_config_request(_Params, _TenantId) ->
    {error, invalid_params}.

auth_scheme(Config) ->
    case maps:get(<<"authentication">>, Config, undefined) of
        #{<<"scheme">> := Scheme} when is_binary(Scheme), Scheme =/= <<>> -> Scheme;
        _ -> undefined
    end.

%% {"taskId": "...", "configId": "..."}
push_config_id(Params, TenantId) when is_map(Params) ->
    case {maps:get(<<"taskId">>, Params, undefined),
          maps:get(<<"configId">>, Params, undefined)} of
        {TaskId, ConfigId} when is_binary(TaskId), TaskId =/= <<>>,
                                is_binary(ConfigId), ConfigId =/= <<>> ->
            {ok, TenantId, TaskId, ConfigId};
        _ ->
            {error, invalid_params}
    end;
push_config_id(_Params, _TenantId) ->
    {error, invalid_params}.

task_only(Params, TenantId) when is_map(Params) ->
    case maps:get(<<"taskId">>, Params, undefined) of
        TaskId when is_binary(TaskId), TaskId =/= <<>> ->
            {ok, TenantId, TaskId};
        _ ->
            {error, invalid_params}
    end;
task_only(_Params, _TenantId) ->
    {error, invalid_params}.

create_push_config(#{tenant_id := Tenant, task_id := TaskId, url := Url,
                     auth_scheme := Scheme, allow_private := AllowPrivate}) ->
    Enabled = push_method_enabled(),
    case Enabled of
        false ->
            {error, push_notification_not_supported};
        true ->
            case config_id_for(Tenant, TaskId, Url) of
                ConfigId ->
                    Spec = #{tenant_id => Tenant, task_id => TaskId,
                             config_id => ConfigId, url => Url,
                             auth_scheme => Scheme,
                             allow_private => AllowPrivate},
                    case broker_meta:put_push_config(Spec) of
                        {ok, _Version} ->
                            {ok, push_config_wire(Spec)};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    end.

%% The config id is server-generated so a client cannot enumerate another
%% tenant's configs by guessing ids; the same (task, url) is idempotent.
config_id_for(Tenant, TaskId, Url) ->
    Digest = crypto:hash(sha256, <<Tenant/binary, 0, TaskId/binary, 0,
                                    Url/binary>>),
    base64url(binary:part(Digest, 0, 9)).

get_push_config(Tenant, TaskId, ConfigId) ->
    case broker_meta:get_push_config(Tenant, TaskId, ConfigId) of
        {ok, Row} -> {ok, push_config_wire(row_to_spec(Row))};
        not_found -> {error, task_not_found}
    end.

list_push_configs(Tenant, TaskId) ->
    Rows = broker_meta:push_configs(Tenant, TaskId),
    {ok, #{<<"configs">> => [push_config_wire(row_to_spec(R)) || R <- Rows]}}.

delete_push_config(Tenant, TaskId, ConfigId) ->
    case broker_meta:get_push_config(Tenant, TaskId, ConfigId) of
        {ok, _Row} ->
            _ = broker_meta:delete_push_config(Tenant, TaskId, ConfigId),
            {ok, #{}};
        not_found ->
            {error, task_not_found}
    end.

%% Row: {push_config, Ref, Tenant, Task, Config, Url, AuthScheme, AllowPrivate,
%%        UpdatedAt, Version}
row_to_spec(Row) ->
    #{tenant_id => element(3, Row),
      task_id => element(4, Row),
      config_id => element(5, Row),
      url => element(6, Row),
      auth_scheme => element(7, Row),
      allow_private => element(8, Row)}.

%% Wire shape follows `TaskPushNotificationConfig' (convergence report §2.4):
%% tenant, id, taskId, url, authentication.scheme. `token' is ours (`configId').
push_config_wire(#{tenant_id := Tenant, task_id := TaskId, config_id := ConfigId,
                   url := Url, auth_scheme := Scheme}) ->
    Base = #{<<"tenant">> => Tenant,
             <<"id">> => ConfigId,
             <<"taskId">> => TaskId,
             <<"url">> => Url},
    case Scheme of
        undefined -> Base;
        <<>> -> Base;
        S -> Base#{<<"authentication">> => #{<<"scheme">> => S}}
    end.

base64url(Bin) ->
    Encoded = base64:encode(Bin),
    iolist_to_binary([url_char(B) || <<B/integer>> <= Encoded, B =/= $=]).

url_char($+) -> $-;
url_char($/) -> $_;
url_char(B) -> B.

%% @doc Validate the parts of `SendMessageRequest' this stage promises to
%% understand. Everything else in the proto (configuration, push config,
%% history length) is accepted by the client but not read yet -- S9-C adds the
%% streaming/push semantics.
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

%% GetTask/CancelTask take `{"id": "..."}'; a non-binary or empty id is a
%% parameter error, not a 404 (the client never named a task).
task_id(#{<<"id">> := TaskId}) when is_binary(TaskId), TaskId =/= <<>> ->
    {ok, TaskId};
task_id(_Params) ->
    error.

%% ListTasks query: unknown states are a parameter error rather than silently
%% widening the filter to "everything".
list_query(Params) when is_map(Params) ->
    case {context_filter(Params), status_filter(Params),
          page_size(Params), page_token(Params)} of
        {{ok, Context}, {ok, Status}, {ok, Size}, {ok, Token}} ->
            {ok, #{context_id => Context, status => Status},
             #{page_size => Size, page_token => Token}};
        _ ->
            error
    end;
list_query(_Params) ->
    error.

context_filter(Params) ->
    case maps:get(<<"contextId">>, Params, undefined) of
        undefined -> {ok, undefined};
        Context when is_binary(Context), Context =/= <<>> -> {ok, Context};
        _ -> error
    end.

status_filter(Params) ->
    case maps:get(<<"status">>, Params, undefined) of
        undefined ->
            {ok, undefined};
        Status when is_binary(Status) ->
            parse_statuses([Status]);
        Statuses when is_list(Statuses) ->
            parse_statuses(Statuses);
        _ ->
            error
    end.

parse_statuses(Bins) ->
    try
        {ok, [begin
                  case a2a_task:from_wire(Bin) of
                      {ok, State} -> State;
                      {error, unknown_state} -> throw(unknown_state)
                  end
              end || Bin <- Bins]}
    catch
        throw:unknown_state -> error
    end.

page_size(Params) ->
    case maps:get(<<"pageSize">>, Params, undefined) of
        undefined -> {ok, 50};
        Size when is_integer(Size), Size > 0 -> {ok, Size};
        _ -> error
    end.

page_token(Params) ->
    case maps:get(<<"pageToken">>, Params, undefined) of
        undefined -> {ok, undefined};
        Token when is_binary(Token) -> {ok, Token};
        _ -> error
    end.

%%%_* Results and errors ----------------------------------------------------

rpc_result(Id, {ok, Value}, Keep) ->
    {200, json_headers(),
     agent_rpc_json:encode(agent_rpc_json:response_ok(Id, Value)), Keep};
rpc_result(Id, {error, Reason}, Keep) ->
    rpc_error(Id, classify(Reason), Keep).

with_authorization(Identity, Action, Resource, Keep, Fun) ->
    case agent_rpc_auth:authorize(Identity, Action, Resource) of
        allow -> Fun();
        deny ->
            %% ACL is a transport decision, not a JSON-RPC error: the request
            %% was understood and refused.
            {403, json_headers(), <<>>, Keep}
    end.

rpc_error(Id, Reason, Keep) ->
    Status = agent_rpc_error:http_status(Reason),
    Body = agent_rpc_json:response_error(Id, Reason, undefined),
    {Status, json_headers(), agent_rpc_json:encode(Body), Keep}.

%% Registry errors are a small closed set; anything else is logged as an
%% internal error instead of leaking an Erlang term into `error.data'.
classify(task_not_found) -> task_not_found;
classify(task_not_cancelable) -> task_not_cancelable;
classify(unsupported_operation) -> unsupported_operation;
classify(push_notification_not_supported) -> push_notification_not_supported;
classify(extended_agent_card_not_configured) -> extended_agent_card_not_configured;
classify(invalid_params) -> invalid_params;
classify(_Other) -> internal_error.

tenant(#{tenant_id := TenantId}) -> TenantId.

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
