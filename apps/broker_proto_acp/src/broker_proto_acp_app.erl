%% @doc ACP v1 front end: the broker as an ACP **Client** over stdio (S9-E).
%%
%% Boot refusals, both loud:
%%   * `agents' not a list of well-formed maps -- a misconfigured agent spec
%%     would otherwise surface later as an opaque spawn failure;
%%   * a permission allowlist entry naming a non-binary -- the allowlist is the
%%     only thing standing between an agent and a sensitive tool call.
%%
%% There is no `require_auth' knob: ACP stdio's trust boundary is OS-level access
%% to the process (same as `mcp_stdio'), and the identity is the anonymous local
%% one. The agent processes themselves are declared by the operator, never by a
%% remote request.
-module(broker_proto_acp_app).
-behaviour(application).

-export([start/2, stop/1, validate_agents/1, agents/0,
         supported_protocol_version/0]).

start(_StartType, _StartArgs) ->
    case validate_agents(agents()) of
        ok -> broker_proto_acp_sup:start_link();
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok.

-spec agents() -> [map()].
agents() ->
    env(agents, []).

-spec supported_protocol_version() -> pos_integer().
supported_protocol_version() ->
    env(supported_protocol_version, 1).

-spec validate_agents([map()]) -> ok | {error, term()}.
validate_agents([]) ->
    ok;
validate_agents([#{name := Name, command := Command} = Spec | Rest])
  when is_binary(Name), is_binary(Command) ->
    case validate_spec(Spec) of
        ok -> validate_agents(Rest);
        {error, _} = Error -> Error
    end;
validate_agents([_Other | _Rest]) ->
    {error, {invalid_agent_spec, _Other}}.

validate_spec(#{args := Args}) when not is_list(Args) ->
    {error, {invalid_agent_args, Args}};
validate_spec(#{env := Env}) ->
    case lists:all(fun({K, V}) -> is_binary(K) andalso is_binary(V);
                      (_) -> false
                   end, Env) of
        true -> ok;
        false -> {error, {invalid_agent_env, Env}}
    end;
validate_spec(_Spec) ->
    ok.

env(Key, Default) ->
    case application:get_env(broker_proto_acp, Key) of
        {ok, Value} -> Value;
        undefined -> Default
    end.
