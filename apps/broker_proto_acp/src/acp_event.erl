%% @doc ACP turn traffic as ordinary `#broker_message{}' (ADR-025 §D1, reused).
%%
%% ACP adds **no second log and no Mnesia table**. Everything an operator or a
%% subscriber needs to see is a plain kernel message:
%%
%%   * `$acp/in/<agent>'       -- a prompt request aimed at one agent (inbound);
%%   * `$acp/session/<id>'     -- one `session/update' notification, and finally
%%                                the finished turn projection with `stopReason';
%%   * `$acp/permission'       -- every `session/request_permission' decision.
%%
%% Routing goes through `broker_router_hash:route/3' (the only partition oracle,
%% ADR-010) and writing through `broker_queue:append/3' (the only write path, so
%% `replicated_commit' and the size/attribute guards apply unchanged), exactly as
%% `a2a_event' does for Task events.
%%
%% Reliability: turn traffic is **transient / `os_buffer'**. It is a live view of
%% an agent's stream, not a durable task log -- replaying it after a restart would
%% be a lie, because the agent's turn is gone. Permission records are the same
%% class: operational evidence, cheap, never blocking a permission turn.
-module(acp_event).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-export([in_destination/1, session_destination/1, permission_destination/0,
         append/3, decode/1, reliability/0]).

-spec in_destination(binary()) -> binary().
in_destination(Agent) ->
    <<"$acp/in/", Agent/binary>>.

-spec session_destination(binary()) -> binary().
session_destination(SessionId) ->
    <<"$acp/session/", SessionId/binary>>.

-spec permission_destination() -> binary().
permission_destination() ->
    <<"$acp/permission">>.

-spec reliability() -> os_buffer.
reliability() ->
    os_buffer.

%% @doc Append one JSON payload. Returns `{ok, Offset}' or an error the caller
%% decides how to log; a failed audit write never fails the permission answer.
-spec append(binary(), binary(), map()) ->
          {ok, non_neg_integer()} | {error, term()}.
append(Dest, Tenant, Payload) ->
    Msg = message(Dest, Tenant, Payload),
    case broker_router:env() of
        {ok, Env} ->
            Ctx = #route_ctx{tenant_id = Tenant, publisher = <<"acp">>,
                             reliability = Msg#broker_message.reliability},
            try
                {ok, [{Partition, _Refs} | _]} =
                    broker_router_hash:route(Dest, Ctx, Env),
                Partitioned = Msg#broker_message{partition_id = Partition},
                broker_queue:append(Partition, Partitioned,
                                    Msg#broker_message.reliability)
            catch
                Class:Reason ->
                    {error, {route_crashed, Class, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Decode a kernel message back to the payload term. Every ACP destination
%% carries JSON.
-spec decode(#broker_message{}) -> {ok, term()} | {error, parse_error}.
decode(#broker_message{payload = Payload}) ->
    agent_rpc_json:decode(iolist_to_binary(Payload)).

message(Dest, Tenant, Payload) ->
    #broker_message{
       message_id = uuid(),
       tenant_id = Tenant,
       destination = Dest,
       partition_id = 0,            %% filled by append/3 after routing
       payload = iolist_to_binary(agent_rpc_json:encode(Payload)),
       payload_encoding = json,
       headers = #{},
       proto_meta = #{<<"acp">> => true},
       persistence = transient,
       reliability = os_buffer,
       created_at = erlang:system_time(millisecond),
       expires_at = undefined
      }.

uuid() ->
    Bytes = crypto:strong_rand_bytes(16),
    <<A:32, B:16, C:16, D:16, E:48>> = Bytes,
    iolist_to_binary(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                                   [A, B, C, D, E])).
