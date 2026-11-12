%% @doc The ACP session state machine (pure).
%%
%% ACP has no server-side session table in this design: a session is a live
%% JSON-RPC exchange over one stdio pipe, and the client's memory of it is what
%% this record holds. Everything here is a pure fold -- no process, no message, no
%% clock -- so it is unit-testable like `a2a_task' and `a2a_task_projection'.
%%
%%   initializing -> (initialize ok)   -> ready
%%   ready        -> (session/new ok)  -> ready + session_id
%%   ready        -> (session/prompt)  -> prompting
%%   prompting    -> (prompt response) -> ready   (stop_reason recorded)
%%   any          -> (session/cancel)  -> ready   (stop_reason = cancelled)
%%   any          -> (close | failure) -> gone
-module(acp_session).

-export([new/1, initialize/2, set_session_id/2, can/2, close/1, cancel/1,
         prompt/2, end_turn/2, failed/3, state/1, id/1, version/1,
         stop_reason/1]).

-export_type([state/0, session/0]).

-type state() :: initializing | ready | prompting | gone.

-record(session, {
          state = initializing :: state(),
          protocol_version = 1 :: pos_integer(),
          agent_name :: binary() | undefined,
          session_id :: binary() | undefined,
          cwd :: binary() | undefined,
          mcp_servers = [] :: [map()],
          stop_reason :: binary() | undefined,
          turns = 0 :: non_neg_integer()
         }).

-type session() :: #session{}.

-spec new(map()) -> session().
new(#{agent_name := AgentName} = Ctx) ->
    #session{agent_name = AgentName,
             cwd = maps:get(cwd, Ctx, undefined),
             mcp_servers = maps:get(mcp_servers, Ctx, [])}.

-spec initialize(session(), map()) -> {ok, session()} | {error, term()}.
initialize(#session{state = initializing} = S, Params) ->
    case maps:get(<<"protocolVersion">>, Params, undefined) of
        Version when is_integer(Version), Version > 0 ->
            {ok, S#session{state = ready, protocol_version = Version}};
        _Other ->
            {error, unsupported_protocol_version}
    end;
initialize(#session{state = Other}, _Params) ->
    {error, {already_initialized, Other}}.

-spec set_session_id(session(), binary()) -> session().
set_session_id(#session{} = S, SessionId) when is_binary(SessionId) ->
    S#session{state = ready, session_id = SessionId}.

%% @doc Which methods the current state may issue. The capability gate: a prompt
%% before `initialize'/`session/new' is a protocol error, not a routing one, and a
%% second concurrent turn is refused instead of silently interleaved.
-spec can(session(), binary()) -> boolean().
can(#session{state = initializing}, Method) ->
    Method =:= <<"initialize">>;
can(#session{state = gone}, _Method) ->
    false;
can(#session{state = ready, session_id = SessionId}, Method) ->
    lists:member(Method, [<<"session/new">>, <<"session/prompt">>,
                          <<"session/cancel">>, <<"session/close">>])
        andalso (Method =/= <<"session/prompt">> orelse SessionId =/= undefined);
can(#session{state = prompting}, Method) ->
    lists:member(Method, [<<"session/cancel">>, <<"session/close">>]).

-spec prompt(session(), binary()) -> {ok, session()} | {error, term()}.
prompt(#session{state = ready, session_id = undefined}, _SessionId) ->
    %% No negotiated session: `session/prompt` would have nowhere to land.
    {error, {not_promptable, no_session_id}};
prompt(#session{state = ready, session_id = Stored} = S, SessionId)
  when Stored =:= SessionId ->
    {ok, S#session{state = prompting, turns = S#session.turns + 1}};
prompt(#session{state = ready, session_id = Stored}, SessionId) ->
    %% A turn must run against the negotiated session, not an id a caller
    %% invented.
    {error, {session_mismatch, {expected, Stored}, {got, SessionId}}};
prompt(#session{state = prompting, session_id = Active}, SessionId)
  when Active =:= SessionId ->
    {error, turn_already_active};
prompt(#session{state = prompting} = S, _SessionId) ->
    {error, {session_busy, S#session.session_id}};
prompt(#session{state = Other}, _SessionId) ->
    {error, {not_promptable, Other}}.

-spec end_turn(session(), binary() | undefined) -> session().
end_turn(#session{state = prompting} = S, StopReason) ->
    S#session{state = ready, stop_reason = StopReason};
end_turn(#session{} = S, _StopReason) ->
    S.

-spec cancel(session()) -> session().
cancel(#session{state = prompting} = S) ->
    S#session{state = ready, stop_reason = <<"cancelled">>};
cancel(#session{} = S) ->
    S.

-spec close(session()) -> session().
close(#session{} = S) ->
    S#session{state = gone}.

-spec failed(session(), binary(), binary()) -> session().
failed(#session{} = S, Reason, _Detail) ->
    S#session{state = gone, stop_reason = Reason}.

%%%_* Read-only accessors ----------------------------------------------------

-spec state(session()) -> state().
state(#session{state = State}) ->
    State.

-spec id(session()) -> binary() | undefined.
id(#session{session_id = Id}) ->
    Id.

-spec version(session()) -> pos_integer().
version(#session{protocol_version = Version}) ->
    Version.

-spec stop_reason(session()) -> binary() | undefined.
stop_reason(#session{stop_reason = Reason}) ->
    Reason.
