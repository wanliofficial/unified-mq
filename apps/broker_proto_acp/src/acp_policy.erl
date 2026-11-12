%% @doc The `session/request_permission' policy (convergence report §7.1).
%%
%% Pure: given the request params and the configured policy, decide. The design
%% freezes three properties that this module enforces by construction:
%%
%%   * **default deny** -- an allowlist entry must match *both* the tool kind and
%%     the title, and no entry means deny. There is no "unknown tool" fallback
%%     that grants;
%%   * **allowlist, not heuristics** -- matching is exact string equality (or the
%%     `any' wildcard on one axis), never a substring or regex over agent-supplied
%%     text, because the agent is the untrusted side of this exchange;
%%   * **timeout refusal** -- the client never waits indefinitely; the caller
%%     supplies the elapsed time, and a decision past `timeout_ms' is a refusal.
%%     Time is a parameter instead of a clock read, so the rule is testable and
%%     the module stays pure.
%%
%% `outcome' values are ACP v1 `RequestPermissionOutcome': `selected' with an
%% `optionId' when granting, `cancelled' when refusing. The decision map is the
%% *internal* shape (flat `outcome') used by the audit body; `wire/1' converts it
%% to the spec's nested result object at the JSON-RPC boundary. ACP v1
%% `RequestPermissionResponse.result' is
%%   {"outcome": {"outcome": "selected", "optionId": "..."}}
%% -- i.e. an `AllowedOutcome' / `DeniedOutcome' *object*, not a bare string. A
%% flat `{"outcome": "selected"}' is rejected by a conforming agent (the official
%% `agent-client-protocol' SDK fails validation with `model_attributes_type'), so
%% the conversion is mandatory, not cosmetic.
-module(acp_policy).

-export([decide/2, decide/3, audit_body/4, default/0, wire/1]).

-type policy() :: #{default := deny | allow,
                    allow := [map()],
                    timeout_ms := non_neg_integer(),
                    notify_dest := binary()}.

-spec default() -> policy().
default() ->
    #{default => deny,
      allow => [],
      timeout_ms => 30000,
      notify_dest => <<"$acp/permission">>}.

%% @doc Decide a permission request. `ElapsedMs' is how long the request has been
%% outstanding when the client answers; a value past `timeout_ms' refuses.
-spec decide(map(), policy()) -> map().
decide(Params, Policy) ->
    decide(Params, Policy, 0).

-spec decide(map(), policy(), non_neg_integer()) -> map().
decide(Params, Policy, ElapsedMs) ->
    Timeout = maps:get(timeout_ms, Policy, 30000),
    case ElapsedMs > Timeout of
        true -> deny(Params, <<"timeout">>);
        false ->
            case matches(Params, Policy) of
                true -> grant(Params);
                false -> deny(Params, <<"not in allowlist">>)
            end
    end.

%% @doc The auditable record body. It is published on the policy's `notify_dest'
%% as an ordinary kernel message -- **never** as a `session/update' notification,
%% which is Agent->Client only.
-spec audit_body(binary(), binary() | undefined, map(), map()) -> map().
audit_body(Agent, SessionId, Params, Decision) ->
    #{<<"agent">> => Agent,
      <<"sessionId">> => SessionId,
      <<"decision">> => maps:get(outcome, Decision),
      <<"reason">> => maps:get(reason, Decision, undefined),
      <<"toolCall">> => tool(Params)}.

%% @doc Project a decision onto the ACP v1 `RequestPermissionResponse.result'
%% body. The `_meta' extension block and the human `reason' ride along: `_meta'
%% is reserved for exactly this and a conforming peer ignores unknown members.
-spec wire(map()) -> map().
wire(#{outcome := <<"selected">>} = Decision) ->
    Outcome = #{<<"outcome">> => <<"selected">>,
                <<"optionId">> => maps:get(optionId, Decision, null)},
    wire_meta(Decision, #{<<"outcome">> => Outcome});
wire(#{outcome := <<"cancelled">>} = Decision) ->
    Outcome = #{<<"outcome">> => <<"cancelled">>},
    wire_meta(Decision, #{<<"outcome">> => Outcome}).

wire_meta(Decision, Body) ->
    WithReason = case maps:get(reason, Decision, undefined) of
                     undefined -> Body;
                     Reason -> Body#{<<"reason">> => Reason}
                 end,
    case maps:get('_meta', Decision, undefined) of
        undefined -> WithReason;
        Meta -> WithReason#{<<"_meta">> => Meta}
    end.

%%%_* Internal ---------------------------------------------------------------

matches(Params, #{default := deny, allow := Allow}) ->
    Tool = tool(Params),
    Kind = maps:get(<<"kind">>, Tool, undefined),
    Title = maps:get(<<"title">>, Tool, undefined),
    entry_matches(Kind, Title, Allow);
matches(_Params, _Policy) ->
    %% `default => allow' is accepted by config validation but is not shipped:
    %% the frozen posture is deny, so an unexpected value refuses rather than
    %% silently granting.
    false.

entry_matches(_Kind, _Title, []) ->
    false;
entry_matches(Kind, Title, [#{tool := K, title := T} | Rest]) ->
    axis(K, Kind) andalso axis(T, Title) orelse entry_matches(Kind, Title, Rest);
entry_matches(Kind, Title, [_Malformed | Rest]) ->
    entry_matches(Kind, Title, Rest).

axis(any, _Actual) -> true;
axis(Expected, Actual) -> Expected =:= Actual.

tool(Params) ->
    case maps:get(<<"toolCall">>, Params, #{}) of
        Tool when is_map(Tool) -> Tool;
        _ -> #{}
    end.

grant(Params) ->
    #{outcome => <<"selected">>,
      optionId => selected_option(Params),
      reason => <<"matched allowlist">>,
      '_meta' => #{<<"unified-mq">> => #{<<"decision">> => <<"allow">>,
                                       <<"toolCall">> => tool(Params)}}}.

deny(Params, Reason) ->
    #{outcome => <<"cancelled">>,
      reason => Reason,
      '_meta' => #{<<"unified-mq">> => #{<<"decision">> => <<"deny">>,
                                       <<"reason">> => Reason,
                                       <<"toolCall">> => tool(Params)}}}.

%% Prefer an `allow_once' option, then `allow_always'; if the agent offered
%% neither (which the spec says it must), fall back to the first option rather
%% than inventing an id the agent would not recognise.
selected_option(Params) ->
    Options = maps:get(<<"options">>, Params, []),
    Candidates = [O || O <- Options, is_map(O),
                       option_kind(O) =:= <<"allow_once">> orelse
                       option_kind(O) =:= <<"allow_always">>],
    case {Candidates, Options} of
        {[#{<<"optionId">> := Id} | _], _} -> Id;
        {[], [#{<<"optionId">> := Id} | _]} -> Id;
        _ -> null
    end.

option_kind(Option) ->
    maps:get(<<"kind">>, Option, undefined).
