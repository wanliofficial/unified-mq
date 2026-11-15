%% @doc RFC 7641 Observe relationship bookkeeping for the CoAP listener.
%%
%% Pure functions only -- no process, no socket. The listener owns the maps
%% (`observes' keyed by {IP,Port,Token}, `pending' keyed by {IP,Port,Token})
%% and calls into here, so the whole subscription/notification policy is
%% unit-testable without a datagram in flight. ADR-031 §D2: the subscription
%% itself lives in `broker_subscription'; this module only mints the filter,
%% holds the per-relationship metadata, and builds the notification message.
-module(coap_observe).

-export([filter/1, register/5, seq/1, next_seq/2,
         sweep_window_ms/2, expired/3, notify_message/4, deregister/2]).
-export_type([relationship/0, key/0, observes/0]).

-include_lib("broker_core/include/broker_message.hrl").

%% A relationship is identified by {IP,Port,Token}: the token is the CoAP
%% request token, so a re-registration under the same token replaces it.
-type key() :: {inet:ip_address(), inet:port_number(), binary()}.
-type relationship() :: #{dest := binary(),
                          tenant := binary(),
                          sub_ref := broker_subscription:sub_ref(),
                          max_age := non_neg_integer(),
                          seq := non_neg_integer(),
                          last_notified := integer(),
                          %% The registering request's type (CON or NON), which
                          %% decides the notification type (RFC 7641 §3.4/§4.1), set
                          %% by the listener when it records the relationship. A GET is
                          %% never an ACK/RST, so when present it is always `con|non'.
                          req_type => con | non}.
-type observes() :: #{key() => relationship()}.

%% The CoAP-Observe filter: the resource path IS the broker destination
%% (ADR-031 §D4 / OQ-5), so the subscription filter is the destination itself.
-spec filter(binary()) -> binary().
filter(Destination) ->
    Destination.

%% Build a relationship entry from the successful subscribe result. `Seq' starts
%% at 0 (RFC 7641 §3.3 initial value), which is also what the initial piggybacked
%% 2.05 answers with (`seq/1'). `NowMs' is the caller's clock reading at
%% registration -- it is passed in rather than read here because
%% `erlang:monotonic_time(millisecond)' is negative on this VM, so a sentinel such
%% as 0 would make a never-notified relationship immortal. `MaxAge' is the
%% client's Max-Age option (seconds); the listener sweeps on a clamped window
%% derived from it, see `window_ms/1'.
-spec register(binary(), binary(), broker_subscription:sub_ref(),
               non_neg_integer(), integer()) ->
          relationship().
register(Dest, Tenant, SubRef, MaxAge, NowMs) ->
    #{dest => Dest, tenant => Tenant, sub_ref => SubRef,
      max_age => MaxAge, seq => 0, last_notified => NowMs}.

-spec seq(relationship()) -> non_neg_integer().
seq(#{seq := Seq}) ->
    Seq.

%% The Max-Age sweep window for a relationship, in milliseconds. The client's
%% Max-Age option says how long the representation stays fresh; the listener
%% stops watching a relationship that has produced nothing for that long.
%% Clamped: at least 1 s, so a client's `Max-Age: 0' cannot kill its own
%% observation on the very next tick, and at most `CapS', so a typo'd or hostile
%% `Max-Age: 604800' cannot pin a table slot for a week.
-spec sweep_window_ms(non_neg_integer(), pos_integer()) -> pos_integer().
sweep_window_ms(ClientAgeS, CapS) ->
    Age = max(ClientAgeS, 1),
    min(Age, max(CapS, 1)) * 1000.

%% True when the relationship has been silent for at least its window. `NowMs'
%% and the stored `last_notified' come from the same clock, so a negative epoch
%% cannot invert the comparison.
-spec expired(relationship(), integer(), pos_integer()) -> boolean().
expired(#{max_age := AgeS, last_notified := At}, NowMs, CapS) ->
    (NowMs - At) >= sweep_window_ms(AgeS, CapS).

%% Advance the Observe sequence, wrapping at 2^24 (RFC 7641 §3.2), and stamp the
%% notification time in the same step so the Max-Age sweep clock cannot go stale
%% by forgetting to refresh it. Returns {NextSeq, Relationship'}.
-spec next_seq(relationship(), integer()) -> {non_neg_integer(), relationship()}.
next_seq(#{seq := Seq} = R, NowMs) ->
    Next = (Seq + 1) band 16#FFFFFF,
    {Next, R#{seq => Next, last_notified => NowMs}}.

%% The notification shape for a dispatched message: a CON or NON 2.05 Content
%% carrying the incremented Observe sequence, the message payload, and the
%% request's token. `Type' is con | non (never ack -- a notification is a
%% separate message, RFC 7641 §4.1). The Message ID is a 0 placeholder from
%% `coap_codec:response/3': the listener overwrites it at send time, since only
%% it knows the current ID space.
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
%% relationship alongside the remaining table. The caller reads `sub_ref' back
%% off the returned relationship -- `broker_subscription' is addressed by
%% (ClientId, Filter), not by SubRef, so SubRef is only needed for assertions
%% and `broker_subscription:subscriptions/1'.
-spec deregister(key(), observes()) -> {relationship() | undefined, observes()}.
deregister(Key, Observes) ->
    case maps:take(Key, Observes) of
        {Relationship, Rest} -> {Relationship, Rest};
        error -> {undefined, Observes}
    end.
