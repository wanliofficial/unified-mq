%% @doc The CoAP resource layer: RFC 7252 methods mapped onto the message kernel.
%%
%% The mapping is deliberately boring, because ADR-031 §D4 froze it: a resource
%% path IS a `broker_router' destination, `a/b/c' with no namespace of its own.
%%
%% | CoAP | kernel | answer |
%% |---|---|---|
%% | `GET /a/b' | `broker_retain:lookup(<<"a/b">>)' | 2.05 Content, or 4.04 |
%% | `PUT /a/b' | publish (retain) | 2.04 Changed |
%% | `POST /a/b' | publish (not retained) | 2.01 Created |
%% | `DELETE /a/b' | `broker_retain:delete(<<"a/b">>)' | 2.02 Deleted, or 4.04 |
%% | `GET /.well-known/core' | link format over this adapter's resources | 2.05 |
%%
%% Why GET reads retained state and not the log: CoAP models a resource as a
%% *representation* the client can fetch, and a retained message is exactly the
%% broker's "current representation of a destination". Reading the log would
%% make GET depend on a cursor, and a cursor is what Observe (S11-B) will own --
%% inventing one here would be the second subscription mechanism ADR-013 forbids.
%%
%% Observe (RFC 7641) is handled in two halves. This module owns only the
%% *answer*: a GET with `Observe: 0' reads the representation exactly like a
%% plain GET (a 4.04 means there is nothing to observe), and a GET with
%% `Observe: 1' answers 2.01 because deregistering is benign (§3.5). The
%% subscription itself is minted by the listener, which owns the socket and the
%% relationship table -- keeping this module a pure function of its inputs.
%% Non-GET methods ignore Observe entirely (§3.1).
%%
%% Reliability (ADR-031 §D3) is per-datagram, never per-message-kind:
%%
%%   * a NON write is dispatched live and NOT appended to the log -- CoAP NON is
%%     at-most-once and promising durability for it would be a lie;
%%   * a CON write goes through the kernel's configured tier
%%     (`broker_queue:reliability/0'), and the ACK still only proves the
%%     datagram arrived here. Neither shape is "QoS 1".
-module(coap_resource).

-export([handle/3, well_known/1, entries/1]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-type ctx() :: #{endpoint := {inet:ip_address(), inet:port_number()},
                 tenant := binary(),
                 principal := binary() | undefined}.
-type reply() :: {reply, coap_codec:code(), [{coap_codec:option_name(), term()}],
                  binary()}.

%% @doc Handle one request. The caller (the listener) owns the socket and turns
%% the returned code/options/payload into the right message type. `ctx' is the
%% listener's per-datagram context (endpoint, tenant, principal), threaded in
%% so the resource layer stays a pure function of its inputs.
-spec handle(coap_codec:message(), map(), ctx()) -> reply().
handle(Req, Cfg, Ctx) ->
    case coap_codec:code(Req) of
        {method, get} -> get(Req, Cfg, Ctx);
        {method, put} -> write(Req, Cfg, Ctx, put);
        {method, post} -> write(Req, Cfg, Ctx, post);
        {method, delete} -> delete(Req, Ctx);
        _ -> {reply, {response, 4, 5}, [], <<>>}
    end.

%%%_* GET -------------------------------------------------------------------

get(Req, Cfg, Ctx) ->
    case coap_codec:destination(Req) of
        <<".well-known/core">> ->
            Body = well_known(Cfg),
            {reply, {response, 2, 5},
             [{content_format, coap_codec:content_format_id(link_format)}], Body};
        Destination ->
            case coap_codec:option(observe, Req) of
                undefined ->
                    read_resource(Destination, Ctx);
                0 ->
                    %% The subscription is minted by the listener (it owns the
                    %% socket and the relationship table). Here we just read the
                    %% current representation; a 4.04 means there is nothing to
                    %% observe yet.
                    read_resource(Destination, Ctx);
                1 ->
                    %% Deregister: the listener drops the relationship. Answer
                    %% 2.01 "Created" (RFC 7641 §3.5 -- deregistering a
                    %% relationship that is not there is benign).
                    {reply, {response, 2, 1}, [], <<>>};
                _ObsVal ->
                    %% Any other Observe value is malformed; treat as a plain read.
                    read_resource(Destination, Ctx)
            end
    end.

read_resource(Destination, Ctx) ->
    case authorized(Ctx, subscribe, Destination) of
        true ->
            case catch broker_retain:lookup(Destination) of
                {ok, #broker_message{} = Msg} ->
                    {reply, {response, 2, 5}, content_format_options(Msg),
                     Msg#broker_message.payload};
                _ ->
                    {reply, {response, 4, 4}, [], <<>>}
            end;
        false ->
            forbidden(Destination, subscribe, "GET")
    end.

%%%_* PUT / POST ------------------------------------------------------------

write(Req, Cfg, Ctx, Method) ->
    Destination = coap_codec:destination(Req),
    Payload = coap_codec:payload(Req),
    case legal_destination(Destination) of
        {error, _} ->
            {reply, {response, 4, 0}, [], <<>>};
        ok ->
            case authorized(Ctx, publish, Destination) of
                false ->
                    forbidden(Destination, publish, method_name(Method));
                true ->
                    case byte_size(Payload) =< broker_limits_limit() of
                        false ->
                            {reply, {response, 4, 13}, [], <<>>};
                        true ->
                            publish(Req, Cfg, Ctx, Destination, Method)
                    end
            end
    end.

publish(Req, _Cfg, Ctx, Destination, Method) ->
    Payload = coap_codec:payload(Req),
    Retain = Method =:= put,
    Msg = new_message(Destination, Payload, Req, Ctx, Retain),
    case deliver(Msg, coap_codec:type(Req), Retain) of
        ok ->
            {reply, ok_code(Method), [], <<>>};
        {error, Reason} ->
            logger:warning("coap ~p ~p failed: ~p",
                           [method_name(Method), Destination, Reason]),
            {reply, {response, 5, 0}, [], <<>>}
    end.

%% CoAP PUT replaces the resource representation, so it sets retained state;
%% POST is an event on the resource and does not. That is also why PUT answers
%% 2.04 Changed and POST answers 2.01 Created.
ok_code(put) -> {response, 2, 4};
ok_code(post) -> {response, 2, 1}.

new_message(Destination, Payload, Req, Ctx, Retain) ->
    #broker_message{message_id = crypto:strong_rand_bytes(16),
                    tenant_id = maps:get(tenant, Ctx,
                                         broker_security:default_tenant()),
                    destination = Destination,
                    partition_id = 0,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{client => <<"coap">>,
                                   content_format => content_format(Req),
                                   retain => Retain,
                                   method => method_name(coap_codec:code(Req))},
                    persistence = durable,
                    reliability = broker_queue:reliability(),
                    created_at = erlang:system_time(millisecond),
                    expires_at = undefined,
                    event = undefined}.

%% A NON write is at-most-once: dispatch it live, update retained state, and do
%% not append to the log (ADR-031 §D3). A CON write takes the kernel's
%% configured tier and follows ADR-018's append -> retain -> dispatch order.
deliver(Msg0, non, Retain) ->
    _ = retain_update(Msg0, Retain, async),
    _ = (catch broker_subscription:dispatch([], {undefined, undefined, Msg0})),
    dispatch_routes(Msg0, non, Retain);
deliver(Msg0, _Type, Retain) ->
    dispatch_routes(Msg0, sync, Retain).

dispatch_routes(Msg0, Mode, Retain) ->
    try
        {ok, Env} = broker_router:env(),
        Ctx = #route_ctx{tenant_id = Msg0#broker_message.tenant_id,
                         publisher = maps:get(publisher,
                                              Msg0#broker_message.proto_meta,
                                              undefined),
                         reliability = Msg0#broker_message.reliability},
        {ok, Routes} = broker_router_hash:route(Msg0#broker_message.destination,
                                                Ctx, Env),
        dispatch_partitions(Routes, Msg0, Mode, Retain)
    catch
        Class:Err:Stack ->
            logger:warning("coap publish path failed ~p:~p ~p",
                           [Class, Err, Stack]),
            {error, {crash, Err}}
    end.

dispatch_partitions([], _Msg, _Mode, _Retain) ->
    ok;
dispatch_partitions([{Partition, Refs} | Rest], Msg0, Mode, Retain) ->
    Msg = Msg0#broker_message{partition_id = Partition},
    case Mode of
        non ->
            %% Nothing is owed to a client that was not listening, so retained
            %% state and the live push are the whole story.
            _ = retain_update(Msg, Retain, async),
            _ = (catch broker_subscription:dispatch(Refs,
                                                   {undefined, undefined, Msg})),
            dispatch_partitions(Rest, Msg0, Mode, Retain);
        sync ->
            case broker_event:apply_policy(Msg) of
                {ok, Accepted} ->
                    case catch broker_queue:append(Partition, Accepted,
                                                   Msg#broker_message.reliability) of
                        {ok, Offset} ->
                            case retain_update(Accepted, Retain, sync) of
                                ok ->
                                    _ = (catch broker_subscription:dispatch(
                                                 Refs, {Partition, Offset, Accepted})),
                                    dispatch_partitions(Rest, Msg0, Mode, Retain);
                                {error, Reason} ->
                                    {error, {retain_store, Reason}}
                            end;
                        {error, Reason} ->
                            {error, {append, Reason}};
                        Other ->
                            {error, {append_crashed, Other}}
                    end;
                {error, Reason} ->
                    {error, {event_policy, Reason}}
            end
    end.

retain_update(_Msg, false, _Mode) ->
    ok;
retain_update(#broker_message{payload = <<>>} = Msg, true, Mode) ->
    broker_retain:delete(Msg#broker_message.destination, Mode);
retain_update(Msg, true, Mode) ->
    case broker_retain:put(Msg, Mode) of
        ok -> ok;
        %% Bounded refusal: the publisher is still answered, only the retained
        %% copy is skipped (same posture as `mqtt_sn_conn').
        {error, overflow} -> ok;
        {error, _} = Err -> Err
    end.

%%%_* DELETE ---------------------------------------------------------------

delete(Req, Ctx) ->
    Destination = coap_codec:destination(Req),
    case authorized(Ctx, publish, Destination) of
        false ->
            forbidden(Destination, publish, "DELETE");
        true ->
            case catch broker_retain:delete(Destination, sync) of
                ok -> {reply, {response, 2, 2}, [], <<>>};
                _ -> {reply, {response, 4, 4}, [], <<>>}
            end
    end.

%%%_* Authorization ---------------------------------------------------------

%% ADR-031 §D5: the identity table is `broker_security''s, and plain UDP has no
%% credential, so `principal' is `undefined' here and `broker_security' treats
%% that as anonymous/allowed. Per-resource ACLs only bite once S11-C's DTLS
%% client identity exists -- calling the same facade now means the policy hook
%% is wired, not that it can distinguish anyone yet.
authorized(Ctx, Action, Destination) ->
    Principal = maps:get(principal, Ctx, undefined),
    broker_security:authorize(Principal, #{action => Action,
                                           tenant_id => maps:get(tenant, Ctx),
                                           destination => Destination})
        =:= allow.

forbidden(Destination, Action, Method) ->
    _ = broker_metrics:inc(authz_failures),
    logger:warning("coap ~s not authorized destination=~p action=~p",
                   [Method, Destination, Action]),
    {reply, {response, 4, 3}, [], <<>>}.

%%%_* .well-known/core -----------------------------------------------------

%% @doc The CoRE Link Format document this adapter publishes (RFC 6690).
%%
%% It lists the statically declared resources from config plus every destination
%% that currently holds retained state -- those are the resources a GET can
%% actually answer for. The well-known resource itself is included, which
%% RFC 6690 §4.1 recommends.
-spec well_known(map()) -> binary().
well_known(Cfg) ->
    coap_codec:link_format(entries(Cfg)).

-spec entries(map()) -> [map()].
entries(Cfg) ->
    Static = [normalise_entry(E) || E <- maps:get(resources, Cfg, [])],
    Retained = [entry_for(Topic) || {Topic, _} <- retained_entries()],
    Deduped = dedupe([well_known_entry() | Static ++ Retained]),
    lists:sort(fun(#{path := A}, #{path := B}) -> A =< B end, Deduped).

well_known_entry() ->
    #{path => [<<".well-known">>, <<"core">>],
      attrs => [{rt, <<"core.well-known">>}, {ct, 40}]}.

normalise_entry(#{path := Path} = Entry) when is_binary(Path) ->
    %% `binary:split' keeps leading and trailing empty segments; drop both ends
    %% so <<"a/b/">> is [<<"a">>, <<"b">>] rather than [<<"a">>, <<"b">>, <<>>],
    %% which would render as `</a/b/>' in the link format.
    Trimmed = trim_empties(binary:split(Path, <<"/">>, [global])),
    Entry#{path => Trimmed};
normalise_entry(#{path := Path} = Entry) when is_list(Path) ->
    Entry.

entry_for(<<>>) ->
    #{path => [], attrs => []};
entry_for(Topic) ->
    #{path => binary:split(Topic, <<"/">>, [global]), attrs => []}.

dedupe(Entries) ->
    lists:foldl(fun(#{path := Path} = E, Acc) ->
                        case lists:member(Path, [P || #{path := P} <- Acc]) of
                            true -> Acc;
                            false -> Acc ++ [E]
                        end
                end, [], Entries).

trim_empties([<<>> | Rest]) -> trim_empties(Rest);
trim_empties(Path) -> Path.

retained_entries() ->
    case catch broker_retain:entries() of
        List when is_list(List) -> List;
        _ -> []
    end.

%%%_* Helpers --------------------------------------------------------------

content_format(Req) -> coap_codec:option(content_format, Req).

content_format_options(Msg) ->
    case maps:get(content_format, Msg#broker_message.proto_meta, undefined) of
        undefined -> [];
        Format -> [{content_format, Format}]
    end.

method_name({method, Method}) -> string:uppercase(atom_to_list(Method));
method_name(Other) -> io_lib:format("~p", [Other]).

%% The kernel's size cap is enforced again inside `broker_queue:append/3'; this
%% one exists so an oversized PUT is refused with 4.13 (the CoAP-correct answer)
%% before any work is done.
broker_limits_limit() ->
    broker_limits:max_message_bytes().

%% `' is the root resource: it maps to the empty destination, which is a legal
%% MQTT topic name but not something this front-end should accept as a publish
%% target -- an empty string is far more likely to be a client bug than intent.
legal_destination(<<>>) -> {error, root};
legal_destination(_) -> ok.
