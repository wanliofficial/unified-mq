%% @doc Server-Sent Events for `SendStreamingMessage' / `SubscribeToTask'
%% (A2A §9.4.2; ADR-025 §D7; ADR-026 §D2).
%%
%% Three rules define the whole module:
%%
%%   * **The stream is a transient reader, not a subscription** (ADR-025 §D7).
%%     It reads the task's log from offset 0, then follows the owning partition
%%     through a *process-local* `broker_queue:subscribe/2' (a mailbox hook, not
%%     a SubRef). It never creates a durable subscription, never advances a
%%     cursor and never pins retention: a slow or dead client only costs its own
%%     socket.
%%   * **Subscribe before replay, replay before follow.** The partition hook is
%%     installed first, so events appended during the replay are already queued
%%     when the follow phase starts -- there is no gap between "read history"
%%     and "watch for new ones". Every connection replays the whole history, so
%%     a reconnect (`SubscribeToTask') loses nothing; the client dedupes on
%%     `_meta.unified-mq.seq'.
%%   * **Close on terminal or interrupted.** A terminal state ends the stream; an
%%     interrupted state (`INPUT_REQUIRED' / `AUTH_REQUIRED') also ends it,
%%     because the next move belongs to the client (A2A §9.4.2).
%%
%% Each `data:' payload is one complete JSON-RPC Response whose `result' is one
%% `StreamResponse' branch (R-T2), which is why framing happens here but JSON
%% encoding does not.
-module(a2a_stream).

-include_lib("broker_core/include/broker_message.hrl").

-export([run/3, max_event_bytes/0, heartbeat_ms/0, max_duration_ms/0]).

-define(PAGE, 200).

%% @doc Stream one task until it reaches a terminal/interrupted state, the
%% client disconnects, or the safety cap is hit.
%%
%% `Plan' carries everything the stream needs; the caller has already checked
%% authorization and cross-tenant visibility.
-spec run(module(), term(), map()) -> ok.
run(Transport, Socket, Plan) ->
    Partition = maps:get(partition, Plan),
    case broker_queue:subscribe(Partition, self()) of
        ok ->
            try
                do_run(Transport, Socket, Plan)
            after
                _ = broker_queue:unsubscribe(Partition, self())
            end;
        {error, _Reason} ->
            %% Cannot follow the log: answer with a spec-shaped SSE error and
            %% close instead of hanging on a stream that will never advance.
            _ = Transport:send(Socket,
                               agent_rpc_http:response(
                                 200, agent_rpc_sse:headers(), <<>>,
                                 #{keep_alive => true, chunked => true})),
            Frame = agent_rpc_sse:event(
                      agent_rpc_json:encode(
                        agent_rpc_json:response_error(
                          maps:get(id, Plan), internal_error, undefined))),
            _ = Transport:send(Socket, agent_rpc_http:chunk(Frame)),
            _ = Transport:send(Socket, agent_rpc_http:last_chunk()),
            ok
    end.

do_run(Transport, Socket, Plan) ->
    _ = Transport:send(Socket,
                       agent_rpc_http:response(
                         200, agent_rpc_sse:headers(), <<>>,
                         #{keep_alive => true, chunked => true})),
    Deadline = erlang:monotonic_time(millisecond) + max_duration_ms(),
    case stream(Transport, Socket, Plan, 0, Deadline) of
        closed ->
            ok;
        _ ->
            %% Normal end (terminal/interrupted) or the cap: terminate the
            %% chunked body. On a closed socket this send fails harmlessly.
            _ = Transport:send(Socket, agent_rpc_http:last_chunk()),
            ok
    end.

stream(Transport, Socket, Plan, From, Deadline) ->
    case erlang:monotonic_time(millisecond) > Deadline of
        true ->
            stopped;
        false ->
            case drain(Transport, Socket, Plan, From) of
                {done, _Next} ->
                    done;
                {continue, Next} ->
                    case await(Transport, Socket, Next, Deadline) of
                        {ok, Next2} -> stream(Transport, Socket, Plan, Next2, Deadline);
                        closed -> closed;
                        stopped -> stopped
                    end;
                closed ->
                    closed;
                error ->
                    closed
            end
    end.

%% Send every stored event at or after `From'. Returns `{done, Next}' once a
%% terminal/interrupted state has been forwarded (the stream must stop), or
%% `{continue, Next}' when the log is exhausted but the task is still running.
drain(Transport, Socket, Plan, From) ->
    Partition = maps:get(partition, Plan),
    case broker_queue:read_from(Partition, From, ?PAGE) of
        {ok, [], Next} ->
            {continue, Next};
        {ok, Page, Next} ->
            case forward_all(Transport, Socket, Plan, Page) of
                {done, _} ->
                    {done, Next};
                {continue, _} ->
                    case Next > From of
                        true -> drain(Transport, Socket, Plan, Next);
                        %% No progress: a backend returned the same page twice.
                        false -> {continue, Next}
                    end;
                closed ->
                    closed;
                error ->
                    error
            end;
        {error, _Reason} ->
            error
    end.

forward_all(_Transport, _Socket, _Plan, []) ->
    {continue, ok};
forward_all(Transport, Socket, Plan, [{Offset, Msg} | Rest]) ->
    case matches(Plan, Msg) of
        false ->
            forward_all(Transport, Socket, Plan, Rest);
        true ->
            case forward(Transport, Socket, Plan, Offset, Msg) of
                {sent, true} -> {done, ok};
                {sent, false} -> forward_all(Transport, Socket, Plan, Rest);
                closed -> closed;
                error -> error
            end
    end.

matches(Plan, #broker_message{destination = Destination}) ->
    Destination =:= maps:get(destination, Plan).

forward(Transport, Socket, Plan, _Offset, #broker_message{payload = Payload0}) ->
    Payload = iolist_to_binary(Payload0),
    case byte_size(Payload) > max_event_bytes() of
        true ->
            %% Bounded at append time (ADR-025 §D8); a frame larger than the
            %% advertised cap is a contract break we refuse to emit rather than
            %% silently truncate.
            error;
        false ->
            case a2a_event:decode(Payload) of
                {ok, Event} ->
                    Frame = agent_rpc_sse:event(
                              agent_rpc_json:encode(
                                agent_rpc_json:response_ok(
                                  maps:get(id, Plan), body_of(Event)))),
                    case Transport:send(Socket, agent_rpc_http:chunk(Frame)) of
                        ok -> {sent, is_final(Event)};
                        {error, _} -> closed
                    end;
                {error, _Reason} ->
                    %% Undecodable event: skip it rather than kill a stream the
                    %% client can otherwise use (same posture as replay).
                    {sent, false}
            end
    end.

body_of(#{body := Body}) -> Body.

is_final(#{kind := status_update, body := Body}) ->
    Update = maps:get(<<"statusUpdate">>, Body, #{}),
    Status = maps:get(<<"status">>, Update, #{}),
    Final = maps:get(<<"final">>, Update, false),
    case a2a_task:from_wire(maps:get(<<"state">>, Status, undefined)) of
        {ok, State} ->
            Final =:= true orelse a2a_task:is_terminal(State)
                orelse a2a_task:is_interrupted(State);
        {error, _} ->
            false
    end;
is_final(_Event) ->
    false.

%% Wait for the next append on the owning partition, emitting a heartbeat
%% comment on every idle tick so intermediaries keep the connection open.
await(Transport, Socket, Next, Deadline) ->
    Timeout = heartbeat_ms(),
    receive
        {new_messages, _Partition, _From, _To} ->
            {ok, Next}
    after Timeout ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true ->
                    stopped;
                false ->
                    case Transport:send(Socket, agent_rpc_http:chunk(
                                                   agent_rpc_sse:heartbeat())) of
                        ok -> await(Transport, Socket, Next, Deadline);
                        {error, _} -> closed
                    end
            end
    end.

%%%_* Config ----------------------------------------------------------------

-spec max_event_bytes() -> pos_integer().
max_event_bytes() ->
    application:get_env(broker_proto_a2a, sse_max_event_bytes, 1048576).

-spec heartbeat_ms() -> pos_integer().
heartbeat_ms() ->
    application:get_env(broker_proto_a2a, sse_heartbeat_ms, 15000).

%% A stream that never terminates (a task stuck in WORKING with no producer)
%% must not hold a connection forever; this is a hard cap, not a retention
%% mechanism.
-spec max_duration_ms() -> pos_integer().
max_duration_ms() ->
    application:get_env(broker_proto_a2a, sse_max_duration_ms, 600000).
