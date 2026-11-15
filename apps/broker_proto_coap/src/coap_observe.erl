%% @doc RFC 7641 Observe relationship bookkeeping for the CoAP listener.
%%
%% Pure functions only -- no process, no socket. The listener owns the maps
%% (`observes' keyed by {IP,Port,Token}, `pending' keyed by {IP,Port,Token})
%% and calls into here, so the whole subscription/notification policy is
%% unit-testable without a datagram in flight. ADR-031 §D2: the subscription
%% itself lives in `broker_subscription'; this module only mints the filter,
%% holds the per-relationship metadata, and builds the notification message.
-module(coap_observe).

-export([filter/1, register/5, seq/1, next_seq/1,
         notify_message/4, deregister/2]).
-export_type([relationship/0, key/0, observes/0]).

-include_lib("broker_core/include/broker_message.hrl").

%% A relationship is identified by {IP,Port,Token}: the token is the CoAP
%% request token, so a re-registration under the same token replaces it.
-type key() :: {inet:ip_address(), inet:port_number(), binary()}.
-type relationship() :: #{dest := binary(),
                          tenant := binary(),
                          sub_ref := term(),
                          max_age := non_neg_integer(),
                          seq := non_neg_integer(),
                          last_notified := integer()}.
-type observes() :: #{key() => relationship()}.

%% The CoAP-Observe filter: the resource path IS the broker destination
%% (ADR-031 §D4 / OQ-5), so the subscription filter is the destination itself.
-spec filter(binary()) -> binary().
filter(Destination) ->
    Destination.

%% Build a relationship entry from the successful subscribe result. `Seq' starts
%% at 0 (RFC 7641 §3.3 initial value); `NowMs' is the caller's clock reading at
%% registration (the Max-Age backstop). The clock is passed in rather than read
%% here because `erlang:monotonic_time(millisecond)' is negative on this VM: a
%% sentinel such as 0 would make a never-notified relationship immortal.
-spec register(binary(), binary(), term(), non_neg_integer(), integer()) ->
          relationship().
register(Dest, Tenant, SubRef, MaxAge, NowMs) ->
    #{dest => Dest, tenant => Tenant, sub_ref => SubRef,
      max_age => MaxAge, seq => 0, last_notified => NowMs}.

-spec seq(relationship()) -> non_neg_integer().
seq(#{seq := Seq}) ->
    Seq.

%% Advance the Observe sequence, wrapping at 2^24 (RFC 7641 §3.2). Returns
%% {NextSeq, Relationship'}.
-spec next_seq(relationship()) -> {non_neg_integer(), relationship()}.
next_seq(#{seq := Seq} = R) ->
    Next = (Seq + 1) band 16#FFFFFF,
    {Next, R#{seq => Next}}.

%% The notification shape for a dispatched message: a CON or NON 2.05 Content
%% carrying the incremented Observe sequence, the message payload, and the
%% request's token. `Type' is con | non (never ack -- a notification is a
%% separate message, RFC 7641 §4.1). The Message ID is left unset: the listener
%% mints it at send time, since only it knows the current ID space.
-spec notify_message(con | non, non_neg_integer(), binary(), #broker_message{}) ->
          coap_codec:message().
notify_message(Type, Seq, Token,
               #broker_message{payload = Payload, proto_meta = Meta}) ->
    Msg = coap_codec:put_option(
            observe, Seq,
            (coap_codec:response(Type, {response, 2, 5}, Payload))#{token => Token}),
    %% Mirror the initial GET's content format, exactly like
    %% `coap_resource:content_format_options/1', so the representation looks the
    %% same on the first response and on every notification.
    case Meta of
        #{content_format := Format} when Format =/= undefined ->
            coap_codec:put_option(content_format, Format, Msg);
        _ ->
            Msg
    end.

%% Drop the relationship for a token (Observe=1 / RST). Returns the removed
%% relationship alongside the remaining table, so the caller can unsubscribe
%% with its stored SubRef.
-spec deregister(key(), observes()) -> {relationship() | undefined, observes()}.
deregister(Key, Observes) ->
    case maps:take(Key, Observes) of
        {Relationship, Rest} -> {Relationship, Rest};
        error -> {undefined, Observes}
    end.
