%% @doc Will Delay Interval holder (MQTT 5.0 §3.1.3.2.2).
%%
%% A dying connection cannot keep its own timer, so a Will with a Will Delay
%% Interval is parked here and published when the interval elapses -- unless a
%% new Network Connection for the same ClientID arrives first, which is
%% exactly the event that cancels the Will (MQTT-3.1.3-14... §3.1.3.2.2: "If a
%% new Network Connection for this ClientID is made before the Will Delay
%% Interval has passed, the Server MUST NOT send the Will Message").
%%
%% The "session ends first fires the Will earlier" half of §3.1.3.2.2 is
%% approximated by the caller: `mqtt_conn_fsm' schedules
%% `min(will_delay, session_expiry)' and only for a persistent session, so a
%% session that ends at disconnect never parks a Will here at all. A broker
%% restart loses parked Wills, which matches the memory-only Will posture of
%% ADR-018 §18.1 (the same posture mosquitto takes).
%%
%% Like `mqtt_will', this module never raises into its caller: every publish
%% failure is contained in `mqtt_will:publish/3'.
%%
%% Generation guard (§3.1.3.2.2 over a reconnect race): the Will is parked by a
%% *dying* connection, but a takeover (a new Network Connection for the same
%% ClientID) is a separate process whose CONNECT handling and whose socket-death
%% timer race the old connection's `terminate/3'. If the old connection loses
%% that race it runs `publish_will' with reason `normal' and tries to schedule
%% a Will on a ClientID that has already moved on. To make that harmless the
%% server tracks a per-ClientID generation: `adopt/1' (called synchronously by
%% the incoming connection, right after `broker_subscription:attach/3') bumps
%% it and returns the new value, and a parked Will carries the generation it was
%% scheduled under -- it publishes only if that generation is still current.
%% An older generation's Will is dropped at `will_due' instead of firing into a
%% session it does not own.
-module(mqtt_will_server).

-behaviour(gen_server).

-export([start_link/0, adopt/1, schedule/6, cancel/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-type will() :: #{topic := binary(), payload := binary(),
                  qos := 0 | 1 | 2, retain := 0 | 1,
                  delay => non_neg_integer()}.

-type gen() :: pos_integer().

-record(entry, {timer :: reference(),
                will :: will(),
                tenant :: binary(),
                principal :: term()}).

%% ClientId => {Generation, Entry}. The generation is bumped on every adopt.
-record(st, {entries = #{} :: #{binary() => {gen(), tuple()}}}).

%%%_* API ---------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc A new Network Connection is taking over `ClientId': bump its generation
%% and drop any Will an older generation parked. Synchronous so the caller can
%% know, before it schedules its own Will, that no older generation's Will will
%% fire. Returns the generation the caller must carry on its `schedule/6'.
-spec adopt(binary()) -> gen().
adopt(ClientId) ->
    gen_server:call(?MODULE, {adopt, ClientId}).

%% @doc Park `Will' for `DelayMs' on behalf of generation `Gen', then publish it
%% when the interval elapses -- but only if `Gen' is still the current
%% generation for `ClientId'. A later `adopt/1' (a reconnect that outran us)
%% makes this a no-op at `will_due', so a dying connection's Will never lands on
%% a session that has already been replaced.
-spec schedule(binary(), will(), non_neg_integer(), binary(), term(), gen()) ->
          ok.
schedule(ClientId, Will, DelayMs, Tenant, Principal, Gen)
  when is_integer(DelayMs), DelayMs > 0 ->
    gen_server:call(?MODULE, {schedule, ClientId, Will, DelayMs,
                              Tenant, Principal, Gen}).

%% @doc Drop `ClientId's parked Will, whatever its generation. Used by the
%% incoming connection before it decides whether it has a Will of its own.
cancel(ClientId) ->
    gen_server:cast(?MODULE, {cancel, ClientId}).

%%%_* gen_server --------------------------------------------------------------

init([]) ->
    {ok, #st{}}.

handle_call({adopt, ClientId}, _From, #st{entries = Entries} = St) ->
    %% Bump the generation and clear any parked Will an older generation left.
    %% The new holder carries the returned Gen on its schedule/6, so only a Will
    %% it itself parks can fire.
    Prev = case maps:find(ClientId, Entries) of
               {ok, {G, _Entry}} -> G;
               error -> 0
           end,
    Gen = Prev + 1,
    %% Drop any parked Will from the old generation, but KEEP the new generation
    %% recorded so the next schedule knows what generation is current.
    St1 = drop_entry(ClientId, St),
    {reply, Gen, St1#st{entries = maps:put(ClientId, {Gen, undefined}, St1#st.entries)}};
handle_call({schedule, ClientId, Will, DelayMs, Tenant, Principal, Gen},
            _From, #st{entries = Entries} = St) ->
    %% Only the current generation may park a Will: a schedule from an
    %% already-superseded connection is dropped on sight (defence in depth --
    %% the generation check at `will_due' is the authoritative gate).
    case maps:get(ClientId, Entries, undefined) of
        {Gen, _} ->
            %% Drop the timer this same generation may have parked earlier, then
            %% park the fresh Will under the same generation.
            St1 = drop_entry(ClientId, St),
            Timer = erlang:send_after(DelayMs, self(), {will_due, ClientId, Gen}),
            Entry = #entry{timer = Timer, will = Will,
                           tenant = Tenant, principal = Principal},
            Entries1 = (St1#st.entries)#{ClientId => {Gen, Entry}},
            {reply, ok, St1#st{entries = Entries1}};
        _ ->
            {reply, ok, St}
    end;
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast({cancel, ClientId}, St) ->
    {noreply, drop_entry(ClientId, St)};
handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({will_due, ClientId, Gen}, #st{entries = Entries} = St) ->
    case maps:get(ClientId, Entries, undefined) of
        {Gen, #entry{will = Will, tenant = Tenant, principal = Principal}} ->
            St1 = St#st{entries = maps:remove(ClientId, Entries)},
            _ = (catch mqtt_will:publish(Will, Tenant, Principal)),
            logger:log(warning, "mqtt_will_server: delayed will published "
                                "(client_id=~ts)", [ClientId]),
            {noreply, St1};
        _ ->
            %% Superseded between parking and due: a newer generation owns this
            %% ClientID now, so the Will must not fire.
            {noreply, St}
    end;
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, #st{entries = Entries}) ->
    %% Timers die with the server; parked Wills are dropped on purpose (the
    %% module doc explains the restart posture).
    [erlang:cancel_timer(T) || {_, #entry{timer = T}} <- maps:values(Entries)],
    ok.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%_* Internal ----------------------------------------------------------------

drop_entry(ClientId, #st{entries = Entries} = St) ->
    case maps:find(ClientId, Entries) of
        {ok, {_, undefined}} ->
            St;
        {ok, {_, Entry}} ->
            _ = cancel_entry(Entry),
            St#st{entries = maps:remove(ClientId, Entries)};
        error ->
            St
    end.

%% Cancel the parked timer for `Entry' (or no-op on the undefined placeholder),
%% returning `ok' regardless.
cancel_entry(undefined) ->
    ok;
cancel_entry(#entry{timer = Timer}) ->
    _ = erlang:cancel_timer(Timer),
    ok.
