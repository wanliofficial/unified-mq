%% @doc Config outbox publisher (ADR-002, ADR-021 §D1).
%%
%% A config change that must also be announced outside Mnesia is written to
%% `config_outbox' first, then published from here. Commit and publish are two
%% systems; marking the row only after the publisher returned `ok' is what keeps
%% a crash between them from losing the announcement (at-least-once, never
%% "marked but never sent").
%%
%% `{outbox_publisher, none}' (the default) leaves rows pending on purpose: it
%% makes "no publisher configured" observable instead of pretending delivery.
-module(broker_meta_outbox).

-behaviour(gen_server).

-export([start_link/0, drain/0, pending/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Synchronous drain; the test seam and the operator's manual retry.
drain() ->
    gen_server:call(?SERVER, drain, 30000).

pending() ->
    broker_meta:pending_outbox().

%%%_* gen_server ----------------------------------------------------------

init([]) ->
    Interval = application:get_env(broker_meta, outbox_interval_ms, 1000),
    Publisher = application:get_env(broker_meta, outbox_publisher, none),
    Timer = case {Publisher, Interval} of
                {none, _} -> undefined;
                {_, I} when is_integer(I), I > 0 ->
                    erlang:send_after(I, self(), drain);
                _ -> undefined
            end,
    {ok, #{publisher => Publisher, interval => Interval, timer => Timer}}.

handle_call(drain, _From, #{publisher := Publisher} = St) ->
    Result = drain(Publisher),
    {reply, Result, St};
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(drain, #{interval := Interval, publisher := Publisher} = St) ->
    _ = drain(Publisher),
    Timer = case Interval > 0 of
                true -> erlang:send_after(Interval, self(), drain);
                false -> undefined
            end,
    {noreply, St#{timer := Timer}};
handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, #{timer := T}) ->
    case T of
        undefined -> ok;
        _ -> _ = erlang:cancel_timer(T), ok
    end.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Internal ------------------------------------------------------------

drain(none) ->
    {ok, #{published => 0, pending => length(broker_meta:pending_outbox()),
           reason => no_publisher}};
drain({M, F}) ->
    Rows = broker_meta:pending_outbox(),
    Published = lists:foldl(
                  fun(Row, Acc) ->
                          Seq = element(2, Row),
                          Event = element(4, Row),
                          case publish(M, F, Event) of
                              ok ->
                                  _ = broker_meta:mark_published(Seq),
                                  Acc + 1;
                              _ ->
                                  Acc
                          end
                  end, 0, Rows),
    {ok, #{published => Published,
           pending => length(broker_meta:pending_outbox())}};
drain(Other) ->
    {error, {invalid_publisher, Other}}.

publish(M, F, Event) ->
    try M:F(Event) of
        ok -> ok;
        {ok, _} -> ok;
        _ -> error
    catch
        _:_ -> error
    end.
