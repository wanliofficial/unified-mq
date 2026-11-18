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
-module(mqtt_will_server).

-behaviour(gen_server).

-export([start_link/0, schedule/5, cancel/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-type will() :: #{topic := binary(), payload := binary(),
                  qos := 0 | 1 | 2, retain := 0 | 1,
                  delay => non_neg_integer()}.

-record(entry, {timer :: reference(),
                will :: will(),
                tenant :: binary(),
                principal :: term()}).

-record(st, {entries = #{} :: #{binary() => #entry{}}}).

%%%_* API ---------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Park `Will' for `DelayMs', then publish it on behalf of `Principal'.
schedule(ClientId, Will, DelayMs, Tenant, Principal) when is_integer(DelayMs),
                                                          DelayMs > 0 ->
    gen_server:cast(?MODULE, {schedule, ClientId, Will, DelayMs,
                              Tenant, Principal}).

%% @doc A new Network Connection for `ClientId': drop its parked Will, if any.
cancel(ClientId) ->
    gen_server:cast(?MODULE, {cancel, ClientId}).

%%%_* gen_server --------------------------------------------------------------

init([]) ->
    {ok, #st{}}.

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast({schedule, ClientId, Will, DelayMs, Tenant, Principal}, St) ->
    %% A second schedule for the same ClientID cannot happen (one dying conn
    %% per ClientID), but replacing is still the safe semantics.
    St1 = cancel_entry(ClientId, St),
    Timer = erlang:send_after(DelayMs, self(), {will_due, ClientId}),
    Entry = #entry{timer = Timer, will = Will,
                   tenant = Tenant, principal = Principal},
    {noreply, St1#st{entries = (St1#st.entries)#{ClientId => Entry}}};
handle_cast({cancel, ClientId}, St) ->
    {noreply, cancel_entry(ClientId, St)};
handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({will_due, ClientId}, #st{entries = Entries} = St) ->
    case maps:take(ClientId, Entries) of
        {#entry{will = Will, tenant = Tenant, principal = Principal}, Rest} ->
            _ = (catch mqtt_will:publish(Will, Tenant, Principal)),
            logger:log(warning, "mqtt_will_server: delayed will published "
                                "(client_id=~ts)", [ClientId]),
            {noreply, St#st{entries = Rest}};
        error ->
            {noreply, St}
    end;
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, #st{entries = Entries}) ->
    %% Timers die with the server; parked Wills are dropped on purpose (the
    %% module doc explains the restart posture).
    [erlang:cancel_timer(T) || #entry{timer = T} <- maps:values(Entries)],
    ok.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%_* Internal ----------------------------------------------------------------

cancel_entry(ClientId, #st{entries = Entries} = St) ->
    case maps:take(ClientId, Entries) of
        {#entry{timer = Timer}, Rest} ->
            _ = erlang:cancel_timer(Timer),
            St#st{entries = Rest};
        error ->
            St
    end.
