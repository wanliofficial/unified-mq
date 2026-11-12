%% @doc One prompt turn, folded from `session/update' notifications (pure).
%%
%% ACP's `session/prompt' is a **request that ends with a `stopReason' response**,
%% while the work streams as `session/update' notifications in between. The
%% client must therefore keep two things per turn: the ordered updates it has
%% forwarded (so a subscriber sees the same sequence), and the terminal stop
%% reason. This module is that fold -- no process, no clock, no queue access.
%%
%% Recognised `sessionUpdate' discriminators (ACP v1; unknown values are kept
%% verbatim in `other' instead of being dropped, so a newer agent degrades to
%% "forwarded but unclassified" rather than "silently lost"):
%%   `user_message_chunk', `agent_message_chunk', `agent_thought_chunk',
%%   `tool_call', `tool_call_update', `plan',
%%   `available_commands_update', `current_mode_update', `config_option_update'.
-module(acp_turn).

-export([new/2, update/2, finish/2, project/1, kinds/0]).

-export_type([turn/0]).

-record(turn, {
          session_id :: binary(),
          prompt = [] :: [map()],
          updates = [] :: [map()],
          counts = #{} :: #{binary() => non_neg_integer()},
          stop_reason :: binary() | undefined,
          status = open :: open | done
         }).

-type turn() :: #turn{}.

-define(KNOWN, [<<"user_message_chunk">>, <<"agent_message_chunk">>,
                <<"agent_thought_chunk">>, <<"tool_call">>,
                <<"tool_call_update">>, <<"plan">>,
                <<"available_commands_update">>, <<"current_mode_update">>,
                <<"config_option_update">>]).

-spec kinds() -> [binary()].
kinds() ->
    ?KNOWN.

-spec new(binary(), [map()]) -> turn().
new(SessionId, Prompt) when is_binary(SessionId) ->
    #turn{session_id = SessionId, prompt = Prompt}.

%% @doc Fold one `session/update' params object. The update is appended in
%% arrival order, so `project/1' returns a faithful, replayable sequence.
-spec update(turn(), map()) -> turn().
update(#turn{status = done} = T, _Params) ->
    T;
update(#turn{updates = U, counts = C} = T, Params) ->
    %% ACP nests the update object under `update'; accept a bare object too so the
    %% fold works whichever shape the caller hands in.
    Object = maps:get(<<"update">>, Params, Params),
    Kind = update_kind(Object),
    T#turn{updates = [Object | U],
           counts = maps:update_with(Kind, fun(N) -> N + 1 end, 1, C)}.

-spec finish(turn(), binary() | undefined) -> turn().
finish(#turn{} = T, StopReason) ->
    T#turn{stop_reason = StopReason, status = done}.

%% @doc The wire projection of a finished turn: what a subscriber sees. `updates'
%% is in arrival order; `counts' lets an operator assert "the agent streamed N
%% agent chunks" without replaying the bodies.
-spec project(turn()) -> map().
project(#turn{} = T) ->
    #{<<"sessionId">> => T#turn.session_id,
      <<"prompt">> => T#turn.prompt,
      <<"updates">> => lists:reverse(T#turn.updates),
      <<"counts">> => T#turn.counts,
      <<"stopReason">> => T#turn.stop_reason,
      <<"status">> => atom_to_binary(T#turn.status, utf8)}.

update_kind(#{<<"sessionUpdate">> := Kind}) when is_binary(Kind) ->
    Kind;
update_kind(Object) when is_map(Object) ->
    case maps:get(<<"update">>, Object, undefined) of
        Nested when is_map(Nested) -> update_kind(Nested);
        _ -> <<"unknown">>
    end;
update_kind(_) ->
    <<"unknown">>.
