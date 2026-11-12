%% @doc Retry, backoff and DLQ for outbound push deliveries (ADR-026 §D6.7,
%% ADR-015 parameters).
%%
%% One `gen_server` owns every delivery. A task shard calls `enqueue/2' after a
%% terminal (or interrupted) event has been appended and **never waits** for the
%% outcome: the task's terminal state is already durable, and a slow webhook
%% endpoint must not stall the state machine (ADR-025 §D7, ADR-026 §D6.7).
%%
%% Attempts follow the ADR-015 vocabulary -- `max_attempts', exponential
%% backoff, TTL -- but the values come from this app's env because a webhook is
%% not a subscription: there is no consumer to redeliver to. When attempts are
%% exhausted (or the TTL has passed) the last attempt's error is written to
%% `$a2a/push/dlq/<tenant>' as an ordinary message, which is subscribable and
%% auditable exactly like `$dlq/...'.
-module(a2a_push_worker).

-behaviour(gen_server).

-export([start_link/0, enqueue/4, stats/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(DEFAULT_MAX_ATTEMPTS, 5).
-define(DEFAULT_BASE_BACKOFF_MS, 500).
-define(DEFAULT_MAX_BACKOFF_MS, 30000).
-define(DEFAULT_TTL_MS, 300000).

-record(pending, {key :: term(),
                  tenant :: binary(),
                  task_id :: binary(),
                  config_id :: binary(),
                  endpoint :: map(),
                  payload :: binary(),
                  headers :: [{binary(), binary()}],
                  attempt = 0 :: non_neg_integer(),
                  deadline :: integer()}).

-record(state, {queue = queue:new() :: queue:queue(),
                timer :: reference() | undefined,
                delivered = 0 :: non_neg_integer(),
                failed = 0 :: non_neg_integer(),
                dropped = 0 :: non_neg_integer()}).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Fire-and-forget: the caller (a task shard) must not block on egress.
%%
%% The rows are `broker_meta:push_config' tuples for this task; the worker owns
%% the guard (which resolves DNS), the signature and the delivery.
-spec enqueue(binary(), binary(), [tuple()], map()) -> ok.
enqueue(TenantId, TaskId, Rows, Body) ->
    gen_server:cast(?MODULE, {enqueue, TenantId, TaskId, Rows, Body}).

-spec stats() -> map().
stats() ->
    gen_server:call(?MODULE, stats, 5000).

%%%_* gen_server ------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

handle_call(stats, _From, State) ->
    {reply, #{delivered => State#state.delivered,
              failed => State#state.failed,
              dropped => State#state.dropped,
              pending => queue:len(State#state.queue)}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unsupported_operation}, State}.

handle_cast({enqueue, TenantId, TaskId, Rows, Body}, State) ->
    State1 =
        lists:foldl(
          fun(Row, Acc) ->
                  case prepare(Row, TenantId, TaskId, Body) of
                      {ok, Pending} -> schedule(Pending, Acc);
                      {error, Reason} ->
                          logger:warning("a2a push skipped ~s/~s: ~0p",
                                         [TenantId, TaskId, Reason]),
                          Acc
                  end
          end, State, Rows),
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(attempt, State) ->
    case queue:out(State#state.queue) of
        {empty, _} ->
            {noreply, State#state{timer = undefined}};
        {{value, Pending}, Rest} ->
            State1 = State#state{queue = Rest, timer = undefined},
            {noreply, retry_or_drop(Pending, State1)}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%_* Delivery loop ---------------------------------------------------------

retry_or_drop(#pending{deadline = Deadline} = Pending, State) ->
    case erlang:system_time(millisecond) > Deadline of
        true ->
            drop(Pending, ttl_expired, State);
        false ->
            Attempt = Pending#pending.attempt + 1,
            Pending1 = Pending#pending{attempt = Attempt},
            case a2a_push:deliver(Pending1#pending.endpoint,
                                  Pending1#pending.payload,
                                  Pending1#pending.headers) of
                {ok, _Status} ->
                    State#state{delivered = State#state.delivered + 1};
                {error, Reason} ->
                    case Attempt >= max_attempts() of
                        true ->
                            drop(Pending1, Reason, State);
                        false ->
                            schedule(Pending1, State#state{
                                                  failed = State#state.failed + 1})
                    end
            end
    end.

schedule(#pending{attempt = Attempt} = Pending, State) ->
    Delay = backoff(Attempt),
    Ref = erlang:send_after(Delay, self(), attempt),
    State#state{queue = queue:in(Pending, State#state.queue), timer = Ref}.

backoff(0) -> 0;
backoff(Attempt) ->
    Base = application:get_env(broker_proto_a2a, push_base_backoff_ms,
                               ?DEFAULT_BASE_BACKOFF_MS),
    Cap = application:get_env(broker_proto_a2a, push_max_backoff_ms,
                              ?DEFAULT_MAX_BACKOFF_MS),
    min(Cap, Base * (1 bsl min(Attempt - 1, 16))).

max_attempts() ->
    application:get_env(broker_proto_a2a, push_max_attempts,
                        ?DEFAULT_MAX_ATTEMPTS).

ttl_ms() ->
    application:get_env(broker_proto_a2a, push_ttl_ms, ?DEFAULT_TTL_MS).

%% Exhausted (or expired): write the failure to the DLQ destination. A DLQ
%% write failure is logged, never raised: the task already ended.
drop(#pending{} = Pending, Reason, State) ->
    _ = write_dlq(Pending, Reason),
    State#state{dropped = State#state.dropped + 1}.

write_dlq(#pending{tenant = Tenant, task_id = TaskId, config_id = ConfigId,
                   attempt = Attempt, endpoint = Endpoint}, Reason) ->
    Destination = <<"$a2a/push/dlq/", Tenant/binary>>,
    Body = agent_rpc_json:encode(#{<<"taskId">> => TaskId,
                                   <<"configId">> => ConfigId,
                                   <<"url">> => maps:get(host, Endpoint, <<>>),
                                   <<"attempts">> => Attempt,
                                   <<"reason">> =>
                                       iolist_to_binary(
                                         io_lib:format("~0p", [Reason])),
                                   <<"_meta">> =>
                                       #{<<"unified-mq">> =>
                                             #{<<"kind">> => <<"pushDlq">>}}}),
    %% The message id is the task id, so the DLQ destination is unique per task
    %% (a single DLQ destination holds every failed delivery; the payload names
    %% the task that failed).
    Msg = #broker_message{message_id = TaskId,
                          tenant_id = Tenant,
                          destination = Destination,
                          partition_id = 0,
                          payload = iolist_to_binary(Body),
                          payload_encoding = json,
                          headers = #{},
                          proto_meta = #{},
                          persistence = durable,
                          reliability = fsync,
                          created_at = erlang:system_time(millisecond),
                          expires_at = undefined,
                          event = undefined},
    Ctx = #route_ctx{tenant_id = Tenant, publisher = <<"a2a_push">>,
                     reliability = fsync},
    case broker_router:env() of
        {ok, Env} ->
            Backend = application:get_env(broker_router, backend,
                                          broker_router_hash),
            try Backend:route(Destination, Ctx, Env) of
                {ok, [{Partition, _Refs} | _]} ->
                    _ = broker_queue:append(Partition,
                                            Msg#broker_message{partition_id =
                                                                   Partition},
                                            fsync),
                    ok;
                {ok, []} ->
                    %% No subscriber for the DLQ destination: the message is
                    %% still durable in the log, so a later `$dlq/...' subscribe
                    %% sees it. Log rather than raise: the task already ended.
                    logger:info("a2a push dlq for ~s landed with no subscribers",
                                [TaskId]),
                    ok;
                {error, R} ->
                    logger:warning("a2a push dlq route failed: ~0p", [R]),
                    ok
            catch
                Class:Reason:Stack ->
                    logger:warning("a2a push dlq route crashed: ~0p:~0p~n~0p",
                                   [Class, Reason, Stack]),
                    ok
            end;
        {error, R} ->
            logger:warning("a2a push dlq env failed: ~0p", [R]),
            ok
    end.

%%%_* Helpers ---------------------------------------------------------------

%% Turn one `broker_meta' row into a pending delivery: validate the endpoint
%% through the egress guard and build the replay-proof headers. A row whose
%% guard or auth fails is skipped with a log line -- a bad config must not stop
%% the others from being delivered.
prepare(Row, TenantId, TaskId, Body) ->
    %% Row shape: {push_config, {T,Task,Cfg}, T, Task, Cfg, Url, AuthScheme,
    %%             AllowPrivate, UpdatedAt, Version}
    ConfigId = element(5, Row),
    Url = element(6, Row),
    Scheme = case element(7, Row) of
                 undefined -> undefined;
                 S when is_binary(S) -> S;
                 S -> atom_to_binary(S, utf8)
             end,
    AllowPrivate = element(8, Row),
    Payload = iolist_to_binary(agent_rpc_json:encode(Body)),
    case a2a_push:guard(TenantId, Url, tenant_allowlist(TenantId)) of
        {ok, Endpoint0} ->
            Endpoint = Endpoint0#{allow_private => AllowPrivate},
            Config = #{tenant_id => TenantId, task_id => TaskId,
                       config_id => ConfigId, endpoint => Endpoint},
            case a2a_push_auth:headers(Scheme, Payload, Config) of
                {ok, Headers} ->
                    {ok, #pending{key = {TenantId, TaskId, ConfigId},
                                  tenant = TenantId,
                                  task_id = TaskId,
                                  config_id = ConfigId,
                                  endpoint = Endpoint,
                                  payload = Payload,
                                  headers = Headers,
                                  deadline = erlang:system_time(millisecond)
                                             + ttl_ms()}};
                {error, Reason} ->
                    {error, {auth_not_configured, Reason}}
            end;
        {error, Reason} ->
            {error, {guard_refused, Reason}}
    end.

tenant_allowlist(TenantId) ->
    All = application:get_env(broker_proto_a2a, push_tenant_allowlist, #{}),
    maps:get(TenantId, All, []).
