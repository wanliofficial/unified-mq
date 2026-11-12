%% @doc One ACP agent connection: the broker as **Client** (ADR-026 §D5,
%% convergence report §7.1).
%%
%% Responsibilities, in the order they happen:
%%
%%   1. **Spawn** the operator-declared agent and own its stdin/stdout
%%      (`acp_transport', default `acp_port').
%%   2. **Handshake**: send `initialize' (`protocolVersion' 1,
%%      `clientCapabilities' advertising no `fs'/`terminal'/`elicitation'),
%%      check the reply's version, then `session/new' to obtain a `sessionId'.
%%      A version mismatch ends the connection; it is never "downgraded".
%%   3. **Serve the one mandatory Client callback**:
%%      `session/request_permission' is answered by `acp_policy' (default deny +
%%      allowlist + timeout) and every decision is audited on `$acp/permission'.
%%      `fs/*', `terminal/*' and `elicitation/*' are **not advertised** and are
%%      refused with a JSON-RPC error rather than dropped -- a dropped request
%%      would leave the agent's id pending forever.
%%   4. **Run prompt turns**: a prompt arrives as a kernel message on
%%      `$acp/in/<agent>' (`#{sessionId, prompt}'); the client sends
%%      `session/prompt' and forwards each `session/update' notification to
%%      `$acp/session/<id>', ending with the `stopReason' the agent returned.
%%   5. **Relay cancellation**: `acp_agent:cancel/1' sends the `session/cancel'
%%      notification.
%%
%% The process is `temporary' in the supervisor: a crashed or exited agent is not
%% restarted, because its session state is gone and the operator must relaunch it.
-module(acp_agent).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-export([start_link/1, prompt/3, cancel/1, deliver/2, state/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_continue/2,
         terminate/2, code_change/3]).

-record(st, {
          agent          :: map(),
          name           :: binary(),
          session        :: acp_session:session(),
          policy         :: map(),
          handle         :: acp_transport:handle(),
          buffer = <<>>  :: binary(),
          pending = #{}  :: map(),
          next_id = 1    :: pos_integer(),
          turn           :: acp_turn:turn() | undefined,
          prompt_sub     :: {non_neg_integer(), non_neg_integer()} | undefined,
          stop           :: undefined | binary()
         }).

-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Spec) ->
    gen_server:start_link(?MODULE, [Spec], []).

%% @doc Run one prompt turn without going through the kernel destination. The
%% in-kernel path is `$acp/in/<agent>'; this is the same action for a console or
%% a test that already knows which agent and session it wants. Returns as soon as
%% the request is written -- the outcome arrives as the `stopReason' in the
%% `$acp/session/<id>' turn projection.
-spec prompt(pid(), binary(), [map()]) -> ok | {error, term()}.
prompt(Pid, SessionId, Prompt) ->
    gen_server:call(Pid, {prompt, SessionId, Prompt}, infinity).

-spec cancel(pid()) -> ok.
cancel(Pid) ->
    gen_server:call(Pid, cancel).

%% @doc Inject bytes as if the agent had written them. Only the test transport
%% uses this; production data arrives from the port.
-spec deliver(pid(), iodata()) -> ok.
deliver(Pid, Bytes) ->
    gen_server:cast(Pid, {transport_data, iolist_to_binary(Bytes)}).

-spec state(pid()) -> map().
state(Pid) ->
    gen_server:call(Pid, state).

%%%_* gen_server ------------------------------------------------------------

init([Spec]) ->
    process_flag(trap_exit, true),
    Name = maps:get(name, Spec),
    Session = acp_session:new(#{agent_name => Name}),
    Policy = policy(),
    case acp_transport:open(Spec) of
        {ok, Handle} ->
            {ok, #st{agent = Spec, name = Name, session = Session,
                     policy = Policy, handle = Handle},
             {continue, initialize}};
        {error, Reason} ->
            {stop, {transport_open_failed, Reason}}
    end.

handle_continue(initialize, State) ->
    %% `clientCapabilities' is the honest gate: an empty capability set means the
    %% agent must not call `fs/*', `terminal/*' or `elicitation/*'. ACP v1 allows
    %% an empty object, and the client additionally refuses those methods.
    Params = #{<<"protocolVersion">> => supported_version(),
               <<"clientCapabilities">> => client_capabilities()},
    {noreply, send_request(initialize, State#st.session, Params, State)}.

handle_call({prompt, SessionId, Prompt}, _From, State) ->
    case acp_session:can(State#st.session, <<"session/prompt">>) of
        false ->
            {reply, {error, {session_not_ready, acp_session:state(State#st.session)}},
             State};
        true ->
            %% Asynchronous by design: the turn's outcome is the `stopReason' the
            %% agent returns, published to `$acp/session/<id>' (plus the update
            %% stream). Blocking the caller would make a slow agent look like a
            %% hung broker.
            {ok, Session1} = acp_session:prompt(State#st.session, SessionId),
            Turn = acp_turn:new(SessionId, Prompt),
            Params = #{<<"sessionId">> => SessionId, <<"prompt">> => Prompt},
            {reply, ok,
             send_request({prompt, SessionId}, Session1, Params,
                          State#st{session = Session1, turn = Turn})}
    end;
handle_call(cancel, _From, #st{session = Session} = State) ->
    case acp_session:state(Session) of
        prompting ->
            SessionId = acp_session:id(Session),
            _ = write(State, acp_frame:notification(
                               <<"session/cancel">>,
                               #{<<"sessionId">> => SessionId})),
            {reply, ok, State#st{session = acp_session:cancel(Session),
                                 turn = finish_turn(State#st.turn, <<"cancelled">>),
                                 stop = <<"cancelled">>}};
        _ ->
            {reply, ok, State}
    end;
handle_call(state, _From, State) ->
    {reply, #{name => State#st.name,
              state => acp_session:state(State#st.session),
              session_id => acp_session:id(State#st.session),
              pending => maps:keys(State#st.pending),
              stop => stop_reason(State)}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({transport_data, Bytes}, State) ->
    {noreply, feed_bytes(Bytes, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({new_messages, _Partition, From, _To},
            #st{prompt_sub = {_, _}} = State) ->
    %% The wake-up hook: a partition this agent subscribed to gained messages.
    {noreply, drain_prompts(From, State)};
handle_info({acp_transport, Handle, {data, Bytes}}, #st{handle = Handle} = State) ->
    {noreply, feed_bytes(Bytes, State)};
handle_info({acp_transport, Handle, closed}, #st{handle = Handle} = State) ->
    {stop, normal, State};
handle_info({Port, {data, {eol, Line}}}, #st{handle = {acp_port, Port}} = State) ->
    %% The port delivers complete lines; the newline re-joins it with any
    %% partial line a previous `{noeol, _}' left in the buffer.
    {noreply, feed_bytes(<<Line/binary, "\n">>, State)};
handle_info({Port, {data, {noeol, Chunk}}}, #st{handle = {acp_port, Port}}
             = State) ->
    {noreply, feed_bytes(Chunk, State)};
handle_info({Port, {exit_status, Status}}, #st{handle = {acp_port, Port}}
             = State) ->
    {stop, {agent_exited, Status}, State};
handle_info({'EXIT', Port, Reason}, #st{handle = {acp_port, Port}} = State) ->
    {stop, {port_closed, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #st{handle = Handle}) ->
    catch acp_transport:close(Handle),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%_* Frame handling ---------------------------------------------------------

%% The single place bytes enter the client: prepend whatever partial line was
%% buffered, split into complete lines, and fold each one. Both transports (a
%% port chunk and an in-process cast) funnel through here.
feed_bytes(Bytes, #st{buffer = Buffer} = State) ->
    {Lines, Rest} = acp_frame:split(Buffer, Bytes),
    lists:foldl(fun handle_line/2, State#st{buffer = Rest}, Lines).

handle_line(<<>>, State) ->
    State;
handle_line(Line, State) ->
    case acp_frame:decode(Line) of
        {ok, Term} ->
            handle_message(Term, State);
        {error, parse_error} ->
            %% No id to correlate with: JSON-RPC's `null' id is exactly for this.
            _ = write(State, acp_frame:response_error(null, parse_error, undefined)),
            State
    end.

handle_message(Term, State) ->
    case acp_frame:kind(Term) of
        response -> handle_response(Term, State);
        notification -> handle_notification(Term, State);
        request -> handle_request(Term, State);
        invalid ->
            _ = write(State, acp_frame:response_error(
                               acp_frame:id_of(Term), invalid_request, undefined)),
            State
    end.

%% An outbound request we issued has come back: `initialize', `session/new', or
%% a prompt turn. The tag tells the three apart.
handle_response(Term, State) ->
    Id = acp_frame:id_of(Term),
    case maps:take(Id, State#st.pending) of
        error ->
            %% A response to an id we never issued (or already retired). Ignore:
            %% answering would be inventing a request.
            State;
        {Tag, Pending} ->
            State1 = State#st{pending = Pending},
            case Tag of
                initialize -> on_initialize_response(Term, State1);
                session_new -> on_session_new_response(Term, State1);
                {prompt, SessionId} -> on_prompt_response(Term, SessionId, State1);
                _Other -> State1
            end
    end.

on_initialize_response(Term, State) ->
    case result_of(Term) of
        {ok, Result} ->
            Expected = supported_version(),
            case maps:get(<<"protocolVersion">>, Result, undefined) of
                Expected ->
                    %% v1 only. The session is now `ready'; ask for a session.
                    Params = #{<<"cwd">> => cwd(State),
                               <<"mcpServers">> => []},
                    send_request(session_new, State#st.session, Params, State);
                Other ->
                    logger:error("ACP agent ~p negotiated protocolVersion ~p, "
                                 "this client speaks only ~p",
                                 [State#st.name, Other, Expected]),
                    State#st{stop = <<"unsupported_protocol_version">>}
            end;
        {error, Reason} ->
            logger:error("ACP initialize failed for ~p: ~p",
                         [State#st.name, Reason]),
            State#st{stop = <<"initialize_failed">>}
    end.

on_session_new_response(Term, State) ->
    case result_of(Term) of
        {ok, Result} ->
            case maps:get(<<"sessionId">>, Result, undefined) of
                SessionId when is_binary(SessionId) ->
                    Session1 = acp_session:set_session_id(
                                 State#st.session, SessionId),
                    subscribe_prompts(State#st{name = State#st.name,
                                               session = Session1});
                _ ->
                    logger:error("ACP session/new for ~p returned no sessionId: ~p",
                                 [State#st.name, Result]),
                    State#st{stop = <<"session_new_failed">>}
            end;
        {error, Reason} ->
            logger:error("ACP session/new failed for ~p: ~p",
                         [State#st.name, Reason]),
            State#st{stop = <<"session_new_failed">>}
    end.

on_prompt_response(Term, SessionId, State) ->
    StopReason =
        case result_of(Term) of
            {ok, Result} -> maps:get(<<"stopReason">>, Result, undefined);
            {error, Reason} ->
                logger:error("ACP session/prompt failed: ~p", [Reason]),
                <<"refusal">>
        end,
    Turn = finish_turn(State#st.turn, StopReason),
    publish_turn(SessionId, Turn),
    State#st{session = acp_session:end_turn(State#st.session, StopReason),
             turn = undefined, stop = StopReason}.

handle_notification(#{<<"method">> := <<"session/update">>} = Term, State) ->
    Params = acp_frame:params_of(Term),
    case State#st.turn of
        undefined ->
            %% An update outside a turn: forward it, but it cannot be attributed
            %% to a prompt, so it never advances a turn fold.
            _ = State,
            State;
        Turn0 ->
            Turn = acp_turn:update(Turn0, Params),
            SessionId = acp_session:id(State#st.session),
            publish_update(SessionId, Params),
            State#st{turn = Turn}
    end;
handle_notification(_Term, State) ->
    %% JSON-RPC: an unknown notification gets no response and no error.
    State.

handle_request(Term, State) ->
    Id = acp_frame:id_of(Term),
    Method = acp_frame:method_of(Term),
    Params = acp_frame:params_of(Term),
    case Method of
        <<"session/request_permission">> ->
            Decision = acp_policy:decide(Params, State#st.policy),
            audit(State, Decision, Params),
            %% ACP v1 result is a nested outcome *object*; acp_policy:wire/1
            %% converts the internal flat decision (the audit shape) to it.
            _ = write(State, acp_frame:response_ok(Id, acp_policy:wire(Decision))),
            case maps:get(outcome, Decision) of
                <<"cancelled">> ->
                    State#st{session = acp_session:failed(
                                        State#st.session, <<"refusal">>,
                                        <<"permission">>)};
                _ ->
                    State
            end;
        _ ->
            %% `fs/*', `terminal/*', `elicitation/*' and anything else the agent
            %% invented: capability not advertised, so a protocol error. Not a
            %% drop -- the agent's id must be resolved.
            _ = write(State, acp_frame:response_error(
                               Id, method_not_found,
                               #{<<"reason">> => <<"capability not advertised">>,
                                 <<"method">> => Method})),
            State
    end.

%%%_* Prompt subscription (transient reader) -------------------------------

%% @doc Subscribe to the prompt destination the way `a2a_stream' and `mcp_stream'
%% do: `broker_queue:subscribe/2' + `read_from/3', no SubRef, no retention pin.
%% A missed prompt costs one prompt, never a stuck partition. If the kernel has no
%% partitions yet (a fresh boot before `broker_queue' is populated) the
%% subscription is skipped and `prompt/3' still works.
subscribe_prompts(State) ->
    Dest = acp_event:in_destination(State#st.name),
    case broker_router:env() of
        {ok, Env} ->
            Ctx = #route_ctx{tenant_id = tenant(), publisher = <<"acp">>,
                             reliability = acp_event:reliability()},
            try broker_router_hash:route(Dest, Ctx, Env) of
                {ok, [{Partition, _Refs} | _]} ->
                    %% Anchor the cursor **before** installing the hook (the
                    %% `mcp_stream' rule): an append that lands in between is
                    %% still read, while nothing published before the
                    %% subscription is replayed.
                    Cursor = tail(Partition),
                    case broker_queue:subscribe(Partition, self()) of
                        ok ->
                            State#st{prompt_sub = {Partition, Cursor}};
                        _Other ->
                            State
                    end
            catch
                _:_ -> State
            end;
        {error, _} ->
            State
    end.

drain_prompts(From, #st{prompt_sub = {Partition, Cursor}} = State) ->
    case broker_queue:read_from(Partition, Cursor, 100) of
        {ok, [], Next} ->
            State#st{prompt_sub = {Partition, Next}};
        {ok, Messages, Next} ->
            State1 = lists:foldl(fun run_prompt/2,
                                 State#st{prompt_sub = {Partition, Next}},
                                 Messages),
            _ = From,
            State1;
        _Other ->
            State
    end.

run_prompt({_Offset, #broker_message{destination = Dest} = Msg}, State) ->
    %% The subscription is per-partition, and `$acp/in/<agent>', `$acp/session/*'
    %% and `$acp/permission' can share a partition. Only the inbound destination
    %% is a prompt; the agent's own published traffic must never be re-read as
    %% one (that would be a self-sustaining prompt loop).
    case Dest =:= acp_event:in_destination(State#st.name) of
        false ->
            State;
        true ->
            case acp_event:decode(Msg) of
                {ok, #{<<"sessionId">> := SessionId} = Payload} ->
                    Prompt = maps:get(<<"prompt">>, Payload, []),
                    case acp_session:can(State#st.session, <<"session/prompt">>) of
                        true ->
                            {ok, Session1} = acp_session:prompt(State#st.session,
                                                                SessionId),
                            Turn = acp_turn:new(SessionId, Prompt),
                            Params = #{<<"sessionId">> => SessionId,
                                       <<"prompt">> => Prompt},
                            send_request({prompt, SessionId}, Session1, Params,
                                         State#st{session = Session1, turn = Turn});
                        false ->
                            logger:warning(
                              "ACP prompt for ~p dropped: session is ~p",
                              [State#st.name,
                               acp_session:state(State#st.session)]),
                            State
                    end;
                {ok, _Other} ->
                    State;
                {error, _} ->
                    State
            end
    end.

tail(Partition) ->
    case broker_queue:watermarks(Partition) of
        {ok, _Committed, High} -> High;
        _ -> 0
    end.

%%%_* Outbound plumbing ------------------------------------------------------

send_request(Tag, _Session, Params, State) ->
    Id = State#st.next_id,
    Frame = acp_frame:request(Id, method_of(Tag), Params),
    _ = write(State, Frame),
    State#st{pending = (State#st.pending)#{Id => Tag}, next_id = Id + 1}.

method_of(initialize) -> <<"initialize">>;
method_of(session_new) -> <<"session/new">>;
method_of({prompt, _SessionId}) -> <<"session/prompt">>.

write(#st{handle = Handle}, Term) ->
    acp_transport:send(Handle, [acp_frame:encode(Term), "\n"]).

result_of(#{<<"result">> := Result}) -> {ok, Result};
result_of(#{<<"error">> := Error}) -> {error, Error};
result_of(_) -> {error, malformed_response}.

%%%_* Publishing -------------------------------------------------------------

%% The forwarded record keeps ACP's own layering: `update' is the
%% `session/update' object (its `sessionUpdate' discriminator plus the payload),
%% not the whole params envelope, so a subscriber does not unwrap twice.
publish_update(SessionId, Params) ->
    Update = maps:get(<<"update">>, Params, #{}),
    _ = acp_event:append(acp_event:session_destination(SessionId), tenant(),
                         #{<<"kind">> => <<"session_update">>,
                           <<"sessionId">> => SessionId,
                           <<"update">> => Update}),
    ok.

publish_turn(undefined, _Turn) ->
    ok;
publish_turn(SessionId, undefined) ->
    _ = SessionId,
    ok;
publish_turn(SessionId, Turn) ->
    _ = acp_event:append(acp_event:session_destination(SessionId), tenant(),
                         #{<<"kind">> => <<"turn">>,
                           <<"turn">> => acp_turn:project(Turn)}),
    ok.

audit(State, Decision, Params) ->
    Body = acp_policy:audit_body(State#st.name,
                                 acp_session:id(State#st.session),
                                 Params, Decision),
    case acp_event:append(acp_event:permission_destination(), tenant(), Body) of
        {ok, _} -> ok;
        {error, Reason} ->
            %% A failed audit write never fails the permission answer: the agent
            %% is blocked on this reply and a lost log line is the lesser harm.
            logger:error("ACP permission audit write failed: ~p", [Reason])
    end.

%% A turn outcome is the prompt response's `stopReason'; before any turn, the
%% session's own record (a permission refusal, a cancel) is the honest answer.
stop_reason(#st{stop = undefined, session = Session}) ->
    acp_session:stop_reason(Session);
stop_reason(#st{stop = Stop}) ->
    Stop.

finish_turn(undefined, _StopReason) ->
    undefined;
finish_turn(Turn, StopReason) ->
    acp_turn:finish(Turn, StopReason).

%%%_* Config ----------------------------------------------------------------

policy() ->
    case application:get_env(broker_proto_acp, permission_policy) of
        {ok, Policy} when is_map(Policy) -> Policy;
        _ -> acp_policy:default()
    end.

supported_version() ->
    case application:get_env(broker_proto_acp, supported_protocol_version) of
        {ok, V} when is_integer(V) -> V;
        _ -> 1
    end.

cwd(State) ->
    case maps:get(cwd, State#st.agent, undefined) of
        undefined -> <<"/">>;
        Cwd -> Cwd
    end.

tenant() ->
    broker_security:default_tenant().

%% `clientCapabilities' advertises nothing: no `fs', no `terminal', no
%% `elicitation'. That absence *is* the capability gate the design freezes
%% (convergence report §7.1); the dispatcher refuses those methods anyway.
client_capabilities() ->
    #{<<"_meta">> => #{<<"unified-mq">> => #{<<"role">> => <<"client">>,
                                            <<"fs">> => false,
                                            <<"terminal">> => false,
                                            <<"elicitation">> => false,
                                            <<"version">> => <<"1">>}}}.
