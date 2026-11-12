%% @doc Transport-neutral MCP protocol dispatch (ADR-027 §D2-D6).
%%
%% Both front ends (Streamable HTTP and stdio) answer a JSON-RPC message the
%% same way; they differ only in framing, in the request-headers gate (HTTP
%% only) and in how a long-lived stream is hosted. This module owns everything
%% that is common: the envelope shape, per-request version negotiation, the
%% required `_meta' fields, feature routing, and the publish/collect plan that
%% turns one primitive call into one kernel message.
%%
%% The outcome vocabulary keeps the transport out of the protocol:
%%   * `no_reply'         -- a notification; HTTP answers 202, stdio writes nothing
%%   * `{bare, Status}'   -- a transport decision with no JSON-RPC body (403)
%%   * `{json, Status, Object}' -- one JSON-RPC response object
%%   * `{stream, Plan}'   -- a long-lived SSE / stdin-shared stream owns the channel
-module(mcp_dispatch).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-export([shape/1, handle/2]).

-export_type([outcome/0]).

-type outcome() :: no_reply
                 | {bare, 200..599}
                 | {json, 200..599, map()}
                 | {stream, map()}.

%%%_* Envelope --------------------------------------------------------------

%% @doc Classify a decoded message. MCP 2026-07-28 (`basic/index'): requests
%% MUST carry a string or integer `id' and MUST NOT use `null'; notifications
%% MUST NOT carry one at all.
-spec shape(term()) ->
          {ok, request | notification, map()} | {error, invalid_request}.
shape(#{<<"jsonrpc">> := <<"2.0">>, <<"method">> := Method} = Term)
  when is_binary(Method), Method =/= <<>> ->
    case maps:find(<<"id">>, Term) of
        error -> {ok, notification, Term};
        {ok, Id} ->
            case valid_id(Id) of
                true -> {ok, request, Term};
                false -> {error, invalid_request}
            end
    end;
shape(_Term) ->
    %% A top-level array (batch), a bare value, a missing `jsonrpc', or a
    %% non-string method are all invalid requests; MCP has no batch.
    {error, invalid_request}.

-spec valid_id(term()) -> boolean().
valid_id(Id) when is_binary(Id) -> Id =/= <<>>;
valid_id(Id) when is_integer(Id) -> true;
valid_id(_) -> false.

%%%_* Dispatch --------------------------------------------------------------

%% @doc Answer one decoded message. `Ctx' carries `transport' (`http' | `stdio')
%% and the authenticated `identity'.
-spec handle(term(), map()) -> outcome().
handle(Term, #{transport := Transport, identity := Identity}) ->
    case shape(Term) of
        {ok, notification, _Term} ->
            no_reply;
        {ok, request, Request} ->
            request(Request, Transport, Identity);
        {error, invalid_request} ->
            rpc_error(invalid_request, null, undefined)
    end.

request(#{<<"id">> := Id, <<"method">> := Method} = Term, Transport, Identity) ->
    case params(Term) of
        {error, invalid_params} ->
            rpc_error(invalid_params, Id, undefined);
        {ok, Params} ->
            %% Per-request protocol fields (`basic/index'): version and client
            %% capabilities are required on every request; a request missing
            %% one is malformed (-32602).
            case check_version(Params, Id) of
                ok ->
                    case check_meta(Params, Id) of
                        ok -> route(Method, Term, Params, Id, Transport, Identity);
                        Outcome -> Outcome
                    end;
                Outcome ->
                    Outcome
            end
    end.

check_version(Params, Id) ->
    case protocol_version(Params) of
        undefined ->
            rpc_error(invalid_params, Id,
                  #{<<"reason">> =>
                        <<"missing _meta.io.modelcontextprotocol/protocolVersion">>});
        Version ->
            Supported = mcp_discover:supported_versions(),
            case lists:member(Version, Supported) of
                true ->
                    ok;
                false ->
                    rpc_error(unsupported_protocol_version, Id,
                          #{<<"supported">> => Supported,
                            <<"requested">> => Version})
            end
    end.

check_meta(Params, Id) ->
    case maps:is_key(<<"io.modelcontextprotocol/clientCapabilities">>, meta(Params)) of
        true ->
            ok;
        false ->
            rpc_error(invalid_params, Id,
                  #{<<"reason">> =>
                        <<"missing _meta.io.modelcontextprotocol/clientCapabilities">>})
    end.

%%%_* Routing ---------------------------------------------------------------

route(Method, Term, Params, Id, Transport, Identity) ->
    Tenant = tenant(Identity),
    case mcp_route:classify(Method, features()) of
        {local, discover} ->
            %% The broker *is* the server here: `server/discover' is answered
            %% from configuration, never forwarded (ADR-027 §D3).
            {json, 200, agent_rpc_json:response_ok(Id, mcp_discover:result())};
        {local, subscriptions} ->
            listen(Params, Id, Transport, Identity);
        {passthrough, _Feature} ->
            call(Method, Term, Params, Id, Transport, Identity, Tenant);
        {feature_not_advertised, Feature} ->
            rpc_error(method_not_found, Id,
                  #{<<"reason">> => <<"feature not advertised">>,
                    <<"feature">> => atom_to_binary(Feature, utf8)});
        {refused, Why} ->
            rpc_error(method_not_found, Id, #{<<"reason">> => refused_reason(Why)});
        {unknown, _Method} ->
            rpc_error(method_not_found, Id, undefined)
    end.

%%%_* subscriptions/listen --------------------------------------------------

listen(Params, Id, Transport, Identity) ->
    case maps:get(<<"notifications">>, Params, #{}) of
        Filter when is_map(Filter) ->
            case authorize(Identity, subscribe, <<"$mcp/notify">>, Transport, Id) of
                ok ->
                    Entries = resolve_entries(mcp_sub:plan(Filter), tenant(Identity)),
                    {stream, #{kind => listen, id => Id, entries => Entries}};
                Outcome ->
                    Outcome
            end;
        _Other ->
            rpc_error(invalid_params, Id,
                  #{<<"reason">> => <<"params.notifications must be an object">>})
    end.

%% Bind each notification destination to the partition that owns it. A
%% destination the router cannot place is dropped from the plan instead of
%% failing the whole subscription: the acknowledgment reports what was agreed.
resolve_entries(Entries, Tenant) ->
    lists:filtermap(
      fun(Entry) ->
              Destination = maps:get(dest, Entry),
              case partition(Destination, Tenant) of
                  {ok, Partition} -> {true, {Entry, Partition}};
                  error -> false
              end
      end, Entries).

%%%_* Pass-through call -----------------------------------------------------

call(Method, Term, Params, Id, Transport, Identity, Tenant) ->
    Destination = <<"$mcp/", Method/binary>>,
    case authorize(Identity, publish, Destination, Transport, Id) of
        ok ->
            serve_call(Term, Params, Id, Destination, Tenant);
        Outcome ->
            Outcome
    end.

serve_call(Term, Params, Id, Destination, Tenant) ->
    Token = token(),
    ReplyTo = <<"$mcp/reply/", Tenant/binary, "/", Token/binary>>,
    case partition(ReplyTo, Tenant) of
        {ok, ReplyPartition} ->
            %% Anchor the reply read at the partition tail **before** the
            %% request is published, so a fast backend cannot answer into a gap
            %% and the follow phase never replays unrelated traffic.
            Start = tail(ReplyPartition),
            Meta0 = meta(Params),
            Meta = Meta0#{<<"unified-mq">> =>
                              #{<<"replyTo">> => ReplyTo,
                                <<"requestToken">> => Token,
                                <<"tenantId">> => Tenant}},
            Request = Term#{<<"params">> => Params#{<<"_meta">> => Meta}},
            case publish(Destination, Tenant, ReplyTo, Token,
                         agent_rpc_json:encode(Request)) of
                {ok, _RequestPartition} ->
                    {stream, #{kind => call, id => Id, reply_to => ReplyTo,
                               partition => ReplyPartition, from => Start,
                               progress_token => progress_token(Meta0),
                               timeout_ms => request_timeout_ms()}};
                {error, Reason} ->
                    logger:warning("mcp publish failed for ~s: ~0p",
                                   [Destination, Reason]),
                    rpc_error(internal_error, Id, undefined)
            end;
        error ->
            rpc_error(internal_error, Id, undefined)
    end.

%% The request is one `broker_message' whose payload is the client's JSON-RPC
%% object verbatim (plus the gateway-private `unified-mq' meta), so a backend in
%% any language can answer without understanding this node (ADR-027 §D2).
publish(Destination, Tenant, ReplyTo, Token, Payload) ->
    Reliability = request_reliability(),
    Msg = case partition(Destination, Tenant) of
              {ok, RequestPartition} ->
                  {ok, #broker_message{message_id = token(),
                                       tenant_id = Tenant,
                                       destination = Destination,
                                       partition_id = RequestPartition,
                                       payload = iolist_to_binary(Payload),
                                       payload_encoding = json,
                                       headers = #{<<"reply_to">> => ReplyTo,
                                                   <<"tenant_id">> => Tenant,
                                                   <<"request_token">> => Token},
                                       proto_meta =
                                           #{<<"unified-mq">> =>
                                                 #{<<"kind">> => <<"mcpRequest">>}},
                                       persistence = durable,
                                       reliability = Reliability,
                                       created_at = erlang:system_time(millisecond),
                                       expires_at = undefined,
                                       event = undefined}};
              error ->
                  {error, no_partition}
          end,
    case Msg of
        {ok, #broker_message{partition_id = Partition} = Message} ->
            case broker_queue:append(Partition, Message, Reliability) of
                {ok, _Offset} -> {ok, Partition};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

%%%_* Router ----------------------------------------------------------------

partition(Destination, Tenant) ->
    case broker_router:env() of
        {ok, Env} ->
            Ctx = #route_ctx{tenant_id = Tenant, publisher = <<"mcp">>,
                             reliability = request_reliability()},
            %% `route/3' always returns a non-empty assignment list: the hash
            %% picks at least one partition. An empty list therefore means the
            %% router is not ready, and the case clause crash is caught below
            %% and turned into the same `error' as a thrown one.
            try broker_router_hash:route(Destination, Ctx, Env) of
                {ok, [{Partition, _Refs} | _]} -> {ok, Partition}
            catch
                _:_ -> error
            end;
        {error, _Reason} ->
            error
    end.

tail(Partition) ->
    case broker_queue:watermarks(Partition) of
        {ok, _Committed, High} -> High;
        {error, _Reason} -> 0
    end.

%%%_* Authorization ---------------------------------------------------------

%% The check is deliberately **not** tenant-scoped. `$mcp/*' is the gateway's
%% own namespace, and the tenant stamped on every message the gateway publishes
%% is the identity's broker-assigned one -- a client never chooses it, so the
%% "you may not act in another tenant" rule has nothing to compare. Passing the
%% tenant anyway would turn the anonymous stdio identity (whose principal has no
%% user record to derive a tenant from) into a denial for a reason that has
%% nothing to do with policy. `broker_security' already treats a missing
%% `tenant_id' as "no isolation check, ACL only", which is exactly the gate
%% wanted here; `agent_rpc_auth:authorize/3' stays the entry point for callers
%% whose resources are tenant-owned (A2A tasks).
authorize(Identity, Action, Resource, Transport, Id) ->
    Principal = maps:get(principal, Identity),
    Decision = broker_security:authorize(Principal, #{action => Action,
                                                      destination => Resource}),
    case Decision of
        allow ->
            ok;
        deny ->
            %% HTTP: an ACL refusal is a transport decision, not a JSON-RPC
            %% error (same posture as the A2A front end). stdio has no status
            %% code to spend, so the refusal has to be visible in the message.
            case Transport of
                http ->
                    {bare, 403};
                stdio ->
                    rpc_error(internal_error, Id,
                          #{<<"reason">> => <<"denied by policy">>,
                            <<"resource">> => Resource})
            end
    end.

%%%_* Errors ----------------------------------------------------------------

rpc_error(Reason, Id, undefined) ->
    {json, status(Reason), agent_rpc_json:response_error(Id, Reason, undefined)};
rpc_error(Reason, Id, Data) ->
    {json, status(Reason), agent_rpc_json:response_error(Id, Reason, Data)}.

%% MCP's status mapping differs from A2A's: an unknown method is a 404
%% (`streamable-http`) and every envelope-level failure is a 400, while A2A
%% keeps JSON-RPC bodies on 200. Everything else follows `agent_rpc_error'.
status(method_not_found) -> 404;
status(invalid_params) -> 400;
status(invalid_request) -> 400;
%% `parse_error' is answered by the transport, not here: the body never
%% decoded into an envelope, so this layer is never reached with one.
status(Reason) -> agent_rpc_error:http_status(Reason).

%%%_* Params ---------------------------------------------------------------

params(#{<<"params">> := Params}) when is_map(Params) -> {ok, Params};
params(#{<<"params">> := _Other}) -> {error, invalid_params};
params(_Term) -> {ok, #{}}.

meta(Params) when is_map(Params) ->
    case maps:get(<<"_meta">>, Params, #{}) of
        Meta when is_map(Meta) -> Meta;
        _ -> #{}
    end.

protocol_version(Params) ->
    case maps:get(<<"io.modelcontextprotocol/protocolVersion">>, meta(Params),
                  undefined) of
        Version when is_binary(Version), Version =/= <<>> -> Version;
        _ -> undefined
    end.

%% A progress token opts the request into `notifications/progress'
%% (`basic/patterns/progress'); it is forwarded to the backend through the
%% request `_meta' and reused when a reply frame is a progress update.
progress_token(Meta) ->
    case maps:get(<<"progressToken">>, Meta, undefined) of
        Token when is_binary(Token); is_integer(Token) -> Token;
        _ -> undefined
    end.

%%%_* Config ---------------------------------------------------------------

features() ->
    application:get_env(broker_proto_mcp, features, []).

request_reliability() ->
    application:get_env(broker_proto_mcp, request_reliability, fsync).

request_timeout_ms() ->
    application:get_env(broker_proto_mcp, request_timeout_ms, 30000).

tenant(#{tenant_id := TenantId}) -> TenantId.

refused_reason(sampling) -> <<"sampling is not supported">>;
refused_reason(elicitation) -> <<"elicitation is not supported">>.

%%%_* Ids ------------------------------------------------------------------

token() ->
    base64url(crypto:strong_rand_bytes(16)).

base64url(Bin) ->
    Encoded = base64:encode(Bin),
    iolist_to_binary([url_char(B) || <<B/integer>> <= Encoded, B =/= $=]).

url_char($+) -> $-;
url_char($/) -> $_;
url_char(B) -> B.
