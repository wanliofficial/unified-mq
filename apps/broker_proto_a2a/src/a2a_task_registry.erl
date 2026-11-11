%% @doc Task shard registry: `phash2(TaskId, N)' -> one shard (ADR-025 §D2).
%%
%% A supervisor, not a bottleneck: `create/get/cancel' resolve the owning shard
%% and call it directly, so tasks on different shards proceed in parallel. The
%% registry process only owns the child set. `list/3' fans out to every shard and
%% merges in the registry because a globally stable page cannot be produced from
%% per-shard pages.
%%
%% One-for-all on purpose: a crashed shard loses its ETS cache, and the only way
%% to make "the projection is the log" true again is to restart the whole set and
%% let `a2a_task_recovery' replay (it is the last child, so it always runs after
%% the shards exist).
-module(a2a_task_registry).

-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([create/3, get/1, list/3, cancel/2, apply_event/4, restore/1,
         shard_count/0, shard_names/0, shard_for/1]).

-define(PAGE_DEFAULT, 50).
-define(PAGE_MAX, 100).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Shards =
        [#{id => {a2a_task_shard, I},
           start => {a2a_task_shard, start_link, [I]},
           restart => permanent,
           shutdown => 5000,
           type => worker,
           modules => [a2a_task_shard]}
         || I <- lists:seq(1, shard_count())],
    Recovery =
        #{id => a2a_task_recovery,
          start => {a2a_task_recovery, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [a2a_task_recovery]},
    {ok, {#{strategy => one_for_all, intensity => 3, period => 10},
          Shards ++ [Recovery]}}.

%% @doc Number of shards, defaulting to the scheduler count so a task shard is
%% never the only serialization point on a multi-core node.
-spec shard_count() -> pos_integer().
shard_count() ->
    case application:get_env(broker_proto_a2a, task_shards, undefined) of
        N when is_integer(N), N > 0 -> N;
        _ -> erlang:system_info(schedulers_online)
    end.

-spec shard_names() -> [atom()].
shard_names() ->
    [a2a_task_shard:name(I) || I <- lists:seq(1, shard_count())].

-spec shard_for(binary()) -> atom().
shard_for(TaskId) ->
    a2a_task_shard:name(erlang:phash2(TaskId, shard_count()) + 1).

%%%_* API -------------------------------------------------------------------

%% @doc Mint a task id and create it. The id, not the caller, decides the shard;
%% a client can therefore neither choose nor observe the sharding.
-spec create(binary(), binary(), map()) -> {ok, map()} | {error, term()}.
create(TenantId, ContextId, Message) ->
    TaskId = new_id(),
    a2a_task_shard:create(shard_for(TaskId), TaskId, TenantId,
                          context_id(ContextId), Message).

-spec get(binary()) -> {ok, map()} | not_found.
get(TaskId) ->
    a2a_task_shard:get(shard_for(TaskId), TaskId).

-spec cancel(binary(), binary()) -> {ok, map()} | {error, term()}.
cancel(TaskId, TenantId) ->
    a2a_task_shard:cancel(shard_for(TaskId), TaskId, TenantId).

-spec apply_event(binary(), binary(), a2a_task:kind(), map()) ->
    {ok, map()} | {error, term()}.
apply_event(TaskId, TenantId, Kind, Payload) ->
    a2a_task_shard:apply_event(shard_for(TaskId), TaskId, TenantId,
                               {Kind, Payload}).

%% @doc Restore one projection after replay. No event is appended.
-spec restore(map()) -> ok.
restore(Proj) ->
    a2a_task_shard:restore(shard_for(a2a_task_projection:id(Proj)), Proj).

%% @doc Tenant-scoped, filtered, globally paginated listing. `page_token' is the
%% last task id of the previous page; ids are unique and sorted, so the token is
%% stable without a cursor table.
-spec list(binary(), map(), map()) ->
    {ok, #{tasks := [map()], next_page_token := binary() | undefined}}.
list(TenantId, Filters, Page) ->
    Projs = lists:append(
              [a2a_task_shard:tasks(Shard, TenantId, Filters)
               || Shard <- shard_names()]),
    Sorted = lists:sort(fun(A, B) ->
                                a2a_task_projection:id(A) =< a2a_task_projection:id(B)
                        end, Projs),
    {PageSize, Token} = page(Page),
    After = case Token of
                undefined -> Sorted;
                _ -> [P || P <- Sorted,
                           a2a_task_projection:id(P) > Token]
            end,
    {Taken, Rest} = take(PageSize, After),
    Next = case Rest of
               [] -> undefined;
               _ -> a2a_task_projection:id(lists:last(Taken))
           end,
    {ok, #{tasks => [a2a_task_projection:to_wire(P) || P <- Taken],
           next_page_token => Next}}.

%%%_* Internal --------------------------------------------------------------

page(#{page_size := Size, page_token := Token}) ->
    {clamp(Size), Token};
page(#{page_size := Size}) ->
    {clamp(Size), undefined};
page(_Other) ->
    {?PAGE_DEFAULT, undefined}.

clamp(Size) when is_integer(Size), Size > 0 -> min(Size, ?PAGE_MAX);
clamp(_Other) -> ?PAGE_DEFAULT.

take(N, List) when N >= length(List) -> {List, []};
take(N, List) -> lists:split(N, List).

context_id(ContextId) when is_binary(ContextId), ContextId =/= <<>> -> ContextId;
context_id(_Other) -> new_id().

new_id() ->
    url_base64(crypto:strong_rand_bytes(16)).

url_base64(Bin) ->
    Encoded = base64:encode(Bin),
    iolist_to_binary([url_char(B) || <<B/integer>> <= Encoded, B =/= $=]).

url_char($+) -> $-;
url_char($/) -> $_;
url_char(B) -> B.
