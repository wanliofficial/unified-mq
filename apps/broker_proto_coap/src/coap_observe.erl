%% @doc RFC 7641 Observe relationship bookkeeping for the CoAP listener.
%%
%% Pure functions only -- no process, no socket. The listener owns the maps
%% (`observes' keyed by {IP,Port,Token}, `pending' keyed by {IP,Port,Token})
%% and calls into here, so the whole subscription/notification policy is
%% unit-testable without a datagram in flight. ADR-031 §D2: the subscription
%% itself lives in `broker_subscription'; this module only mints the filter,
%% holds the per-relationship metadata, and builds the notification message.
-module(coap_observe).

-export([filter/1, register/3, relationship_value/1, next_seq/1,
         notify_message/3, deregister/2]).

-include_lib("broker_core/include/broker_message.hrl").

-type relationship() :: #{dest := binary(),
                         tenant := binary(),
                         sub_ref := term(),
                         max_age := pos_integer(),
                         seq := non_neg_integer(),
                         last_notified := integer()}.

%% The CoAP-Observe filter: the resource path IS the broker destination
%% (ADR-031 §D4 / OQ-5), so the subscription filter is the destination itself.
-spec filter(binary()) -> binary().
filter(Destination) ->
    Destination.

%% Build a relationship entry from the successful subscribe result. `Seq' starts
%% at 0 (RFC 7641 §3.3 initial value); `LastNotified' is the monotonic ms of the
%% registration (the Max-Age backstop clock).
-spec register(binary(), binary(), pos_integer()) -> relationship().
register(Dest, Tenant, MaxAge) ->
    #{dest => Dest, tenant => Tenant, sub_ref => undefined,
      max_age => MaxAge, seq => 0, last_notified => 0}.

-spec relationship_value(relationship()) -> non_neg_integer().
relationship_value(#{seq := Seq}) ->
    Seq.

%% Advance the Observe sequence, wrapping at 2^24 (RFC 7641 §3.2). Returns
%% {NextSeq, Relationship'}.
-spec next_seq(relationship()) -> {non_neg_integer(), relationship()}.
next_seq(#{seq := Seq} = R) ->
    Next = (Seq + 1) band 16#FFFFFF,
    {Next, R#{seq => Next}}.

%% The notification to send for a dispatched message: a CON or NON response
%% carrying the incremented Observe sequence and the message payload, echoing the
%% request token. `Type' is con | non (never ack -- a notification is a separate
%% message, RFC 7641 §4.1).
-spec notify_message(con | non, non_neg_integer(), #broker_message{}) ->
          coap_codec:message().
notify_message(Type, Seq, #broker_message{payload = Payload}) ->
    coap_codec:put_option(observe, Seq,
                          coap_codec:response(Type, {response, 2, 5}, Payload)).

%% Drop the relationship for a token (Observe=1 / RST). Returns the map without
%% it; the caller unsubscribes with the stored SubRef.
-spec deregister({inet:ip_address(), inet:port_number(), binary()}, map()) -> map().
deregister(Key, Observes) ->
    maps:remove(Key, Observes).
