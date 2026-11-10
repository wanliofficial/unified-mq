%% @doc W3C Trace Context propagation for messages crossing the broker
%% (README §10.2).
%%
%% Trace headers are client-controlled.  This module is the single seam that
%% decides whether they travel with a message: it validates the W3C
%% `traceparent' / `tracestate' values, normalises them, and otherwise strips
%% them.  Validated values stay in `#broker_message.headers', which is
%% persisted in the WAL and echoed by every protocol adapter on delivery --
%% that is the "propagation" §10.2 asks for; no `#broker_message' field is
%% added (the record is frozen by ADR-009).
%%
%% Trust boundary: nothing here participates in identity, tenant or ACL
%% decisions.  `propagate/1' only rewrites the two trace header keys, so a
%% forged `traceparent' can at worst travel with a message; it cannot change
%% who published it or which tenant it belongs to.  Propagation is off by
%% default and can be limited to an explicit list of trusted tenants, which is
%% the "on demand, in trusted contexts, privacy" part of §10.2.
-module(broker_event_trace).

-include_lib("broker_core/include/broker_message.hrl").

-export([propagate/1, enabled/0, parse_traceparent/1, parse_tracestate/1]).

-define(TRACEPARENT, <<"traceparent">>).
-define(TRACESTATE, <<"tracestate">>).
-define(MAX_TRACESTATE_MEMBERS, 32).
-define(MAX_TRACESTATE_BYTES, 512).

%% @doc Whether the deployment opted into trace propagation at all.
%% Protocol adapters whose header channel is a whitelist (WS/NATS) use this
%% to decide whether to admit `traceparent'/'tracestate' into the message.
-spec enabled() -> boolean().
enabled() ->
    setting() =/= off.

%% @doc Apply the configured propagation policy to one inbound message.
%% Called from `broker_event:apply_policy/2', the seam all six adapters share.
-spec propagate(#broker_message{}) -> #broker_message{}.
propagate(#broker_message{headers = Headers, tenant_id = Tenant} = Msg)
  when is_map(Headers) ->
    Msg#broker_message{headers = apply_context(Headers, Tenant)};
propagate(Msg) ->
    Msg.

apply_context(Headers, Tenant) ->
    %% Start from a header map free of both trace keys.  When propagation is
    %% off (or this tenant is not trusted), that clean map is the answer: the
    %% keys are dropped rather than forwarded, which is the privacy default.
    Clean = maps:without([?TRACEPARENT, ?TRACESTATE], Headers),
    case trusted(Tenant) of
        false ->
            Clean;
        true ->
            case parse_traceparent(maps:get(?TRACEPARENT, Headers, undefined)) of
                {ok, Parent} ->
                    WithParent = Clean#{?TRACEPARENT => Parent},
                    %% W3C: a tracestate without a valid traceparent is not a
                    %% usable context and MUST be ignored, so only parse it
                    %% after the parent has been accepted.
                    case parse_tracestate(maps:get(?TRACESTATE, Headers, undefined)) of
                        {ok, State} -> WithParent#{?TRACESTATE => State};
                        none -> WithParent
                    end;
                none ->
                    Clean
            end
    end.

setting() ->
    case application:get_env(broker_event, trace_context, off) of
        off -> off;
        false -> off;
        on -> on;
        true -> on;
        {trusted_tenants, List} when is_list(List) -> {trusted_tenants, List};
        _ -> off
    end.

trusted(Tenant) ->
    %% `setting/0' normalises every unknown value to `off', so these three
    %% clauses are exhaustive (dialyzer confirms the closed union).
    case setting() of
        off -> false;
        on -> true;
        {trusted_tenants, List} -> lists:member(Tenant, List)
    end.

%%%_* W3C parsing -----------------------------------------------------------

%% @doc Validate a `traceparent' (W3C Trace Context Level 1, version 00).
%% Only version 00 is supported: an unknown/future version is not guessed at,
%% it is dropped.  Returns a lower-case normalised value, or `none' for a
%% malformed, all-zero, or `ff' version.
-spec parse_traceparent(binary() | undefined) -> {ok, binary()} | none.
parse_traceparent(Value) when is_binary(Value), byte_size(Value) =:= 55 ->
    case binary:split(Value, <<"-">>, [global]) of
        [V0, T0, P0, F0] ->
            V = lower(V0), T = lower(T0), P = lower(P0), F = lower(F0),
            case {is_hex(V, 2), is_hex(T, 32), is_hex(P, 16), is_hex(F, 2),
                  V =:= <<"00">>, not_zero(T), not_zero(P)} of
                {true, true, true, true, true, true, true} ->
                    {ok, <<V/binary, $-, T/binary, $-, P/binary, $-, F/binary>>};
                _ ->
                    none
            end;
        _ ->
            none
    end;
parse_traceparent(_) ->
    none.

%% @doc Validate a `tracestate': <=32 members, <=512 bytes, each `key=value'
%% with a spec-shaped key and a printable value.  OWS is stripped.  One bad
%% member invalidates the whole header -- forwarding a partial tracestate
%% would silently corrupt the vendor chain.
-spec parse_tracestate(binary() | undefined) -> {ok, binary()} | none.
parse_tracestate(Value) when is_binary(Value),
                             byte_size(Value) =< ?MAX_TRACESTATE_BYTES ->
    Members = binary:split(Value, <<",">>, [global]),
    case length(Members) =< ?MAX_TRACESTATE_MEMBERS of
        true ->
            Parsed = [parse_member(M) || M <- Members],
            Valid = [M || M <- Parsed, is_binary(M)],
            case length(Valid) =:= length(Parsed) of
                true -> {ok, iolist_to_binary(lists:join(<<",">>, Valid))};
                false -> none
            end;
        false ->
            none
    end;
parse_tracestate(_) ->
    none.

parse_member(Member0) ->
    %% OWS around a list-member is allowed; inside the value it is not, so
    %% stripping all spaces/tabs is equivalent for a valid header.
    Member = << <<C>> || <<C>> <= Member0, C =/= $\s, C =/= $\t >>,
    case binary:split(Member, <<"=">>) of
        [Key, Val] when Key =/= <<>>, Val =/= <<>> ->
            case valid_key(Key) andalso valid_value(Val) of
                true -> Member;
                false -> invalid
            end;
        _ ->
            invalid
    end.

valid_key(Key) ->
    byte_size(Key) =< 256 andalso
        lists:all(fun(C) ->
                          (C >= $a andalso C =< $z) orelse
                          (C >= $0 andalso C =< $9) orelse
                          C =:= $_ orelse C =:= $- orelse C =:= $* orelse
                          C =:= $/ orelse C =:= $@
                  end, binary_to_list(Key)).

valid_value(Val) ->
    byte_size(Val) =< 256 andalso
        lists:all(fun(C) -> C >= 16#20 andalso C =/= 16#7f andalso
                            C =/= $, andalso C =/= $= end,
                  binary_to_list(Val)).

is_hex(Bin, Len) ->
    byte_size(Bin) =:= Len andalso
        lists:all(fun(C) ->
                          (C >= $0 andalso C =< $9) orelse
                          (C >= $a andalso C =< $f)
                  end, binary_to_list(Bin)).

not_zero(Bin) ->
    not lists:all(fun(C) -> C =:= $0 end, binary_to_list(Bin)).

lower(Bin) ->
    list_to_binary([lower_char(C) || <<C>> <= Bin]).

lower_char(C) when C >= $A, C =< $F -> C + 32;
lower_char(C) -> C.
