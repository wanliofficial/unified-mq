%% @doc CloudEvents policy seam (ADR-004, ADR-020).
%%
%% Five policies from README §3.4, resolved per tenant + destination:
%%
%%   * `disabled' / `pass_through' -- identity. No payload is inspected, no
%%     `event' view is invented. This is the default and the S1/S2 behavior.
%%   * `validate_if_present' -- decode+validate when the message *is* a
%%     CloudEvents message; ordinary messages pass through untouched.
%%   * `required' -- the message must be a valid CloudEvents event.
%%   * `normalize' -- like `required', and the decoded event view is attached to
%%     `#broker_message.event' while the original payload bytes stay intact.
%%
%% The module is pure except for two explicitly side-effecting helpers:
%% `duplicate/1' (bounded dedup window, off by default) and `quarantine/2'
%% (optional fsync copy of a rejected message to a dead-letter destination).
%% Every exported function is total: a hand-built message or a broken config
%% yields a tagged error, never a crash in the publisher's process.
-module(broker_event).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").
-include("cloud_event.hrl").

-export([policy/0, apply_policy/1, apply_policy/2,
         policy_for/1, dedup_window/1, duplicate/1, quarantine/2]).

-export_type([policy/0, cloud_event/0]).

%% README §3.4 in full.
-type policy() :: disabled | pass_through | validate_if_present | required | normalize.

%% The reserved README §3.1 shape; runtime code uses the map from ADR-020 §D2.
-type cloud_event() :: #cloud_event{}.

%%%_* Configuration -----------------------------------------------------------

%% @doc The default policy, verbatim and unvalidated. A typo fails where a
%% message is (`apply_policy/2'), not at config load.
-spec policy() -> term().
policy() ->
    application:get_env(broker_event, policy, pass_through).

%% @doc Policy for one message. Order matters: the first matching
%% `{Tenant, Destination, Policy}' wins; `<<"*">>' is the wildcard in both
%% positions, and the destination may be an MQTT filter. No match falls back to
%% `policy/0'.
-spec policy_for(#broker_message{}) -> term().
policy_for(#broker_message{tenant_id = Tenant, destination = Destination}) ->
    case first_match(Tenant, Destination, policies()) of
        {ok, Policy} -> Policy;
        none -> policy()
    end;
policy_for(_) ->
    policy().

policies() ->
    case application:get_env(broker_event, policies, []) of
        List when is_list(List) -> List;
        _ -> []
    end.

%%%_* Policy application ------------------------------------------------------

%% @doc Apply the configured policy to one inbound message.
-spec apply_policy(#broker_message{}) -> {ok, #broker_message{}} | {error, term()}.
apply_policy(Msg) ->
    apply_policy(Msg, policy_for(Msg)).

%% @doc Apply a policy supplied by the caller (also the test seam).
-spec apply_policy(#broker_message{}, policy() | term()) ->
          {ok, #broker_message{}} | {error, term()}.
apply_policy(#broker_message{} = Msg, Policy) ->
    try
        case apply_policy0(Msg, Policy) of
            {ok, Accepted} ->
                %% README §10.2: trace context propagation is orthogonal to
                %% the CloudEvents policy, so it runs on the accepted message
                %% regardless of which policy was selected.  Off by default
                %% (broker_event_trace:propagate/1 is then the identity).
                {ok, broker_event_trace:propagate(Accepted)};
            {error, _} = Error ->
                Error
        end
    catch
        Class:Reason:Stack ->
            %% The seam must not take the publisher down. A defect is reported
            %% as a rejection, which the caller already handles.
            {error, {event_policy_crashed, {Class, Reason, Stack}}}
    end;
apply_policy(Other, _Policy) ->
    {error, {not_a_broker_message, Other}}.

apply_policy0(Msg, disabled) ->
    {ok, Msg};
apply_policy0(Msg, pass_through) ->
    {ok, Msg};
apply_policy0(Msg, validate_if_present) ->
    case broker_event_binding:decode(Msg) of
        {ok, Event, _Mode} ->
            {ok, attach_event(Msg, Event)};
        {error, not_cloudevent} ->
            {ok, Msg};
        {error, Reason} ->
            reject(Reason)
    end;
apply_policy0(Msg, required) ->
    require_event(Msg);
apply_policy0(Msg, normalize) ->
    require_event(Msg);
apply_policy0(_Msg, Other) ->
    {error, {invalid_policy, Other}}.

require_event(Msg) ->
    case broker_event_binding:decode(Msg) of
        {ok, Event, _Mode} ->
            {ok, attach_event(Msg, Event)};
        {error, not_cloudevent} ->
            reject(event_required);
        {error, Reason} ->
            reject(Reason)
    end.

attach_event(Msg, Event) ->
    inc(cloudevents_validate_ok),
    Msg#broker_message{event = Event}.

reject(Reason) ->
    inc(cloudevents_validate_failed),
    {error, {event_invalid, Reason}}.

%%%_* Deduplication -----------------------------------------------------------

%% @doc The configured window for this message, or `none'. Off by default.
-spec dedup_window(#broker_message{}) -> none | {some, non_neg_integer()}.
dedup_window(#broker_message{tenant_id = Tenant, destination = Destination}) ->
    case application:get_env(broker_event, dedup, []) of
        List when is_list(List) ->
            case first_window(Tenant, Destination, List) of
                {ok, Window} when is_integer(Window), Window >= 0 ->
                    {some, Window};
                _ ->
                    none
            end;
        _ ->
            none
    end;
dedup_window(_) ->
    none.

%% @doc `true' when this event was already seen inside the configured window.
%% The first sighting returns `false'. No configured window means `false'.
-spec duplicate(#broker_message{}) -> boolean().
duplicate(#broker_message{event = Event} = Msg) when is_map(Event) ->
    case dedup_window(Msg) of
        none ->
            false;
        {some, Window} ->
            case {maps:get(source, Event, undefined),
                  maps:get(id, Event, undefined)} of
                {Source, Id} when is_binary(Source), is_binary(Id) ->
                    Key = {Msg#broker_message.tenant_id,
                           Msg#broker_message.destination, Source, Id},
                    case broker_event_dedup:check(Key, Window) of
                        duplicate ->
                            inc(cloudevents_duplicates),
                            true;
                        fresh ->
                            false
                    end;
                _ ->
                    false
            end
    end;
duplicate(_) ->
    false.

%%%_* Quarantine --------------------------------------------------------------

%% @doc Optional isolation copy for a rejected message (ADR-020 §D8).
%%
%% `not_configured' means the operator did not ask for isolation; the rejection
%% still stands. A configured target that cannot be written returns an error --
%% the caller reports it but still rejects, because an unaccepted publish is
%% never silently promoted to accepted.
-spec quarantine(#broker_message{}, term()) ->
          ok | not_configured | {error, term()}.
quarantine(#broker_message{} = Msg, Reason) ->
    case quarantine_target(Msg) of
        none ->
            not_configured;
        {ok, Destination} ->
            try write_quarantine(Msg, Destination, Reason) of
                ok ->
                    inc(cloudevents_quarantined),
                    ok;
                {error, _} = Error ->
                    Error
            catch
                Class:Why ->
                    {error, {quarantine_crashed, {Class, Why}}}
            end
    end;
quarantine(Other, _Reason) ->
    {error, {not_a_broker_message, Other}}.

write_quarantine(Msg, Destination, Reason) ->
    {ok, Env} = broker_router:env(),
    Ctx = #route_ctx{tenant_id = Msg#broker_message.tenant_id,
                     publisher = <<"broker_event">>,
                     reliability = fsync},
    %% ADR-010 makes `route/3' answer at least the destination's own partition
    %% -- the empty list is a contract break, not a case to handle here. The
    %% surrounding try turns one into `{error, {quarantine_crashed, _}}', which
    %% still leaves the rejection in force.
    {ok, [{Partition, _Refs} | _]} = broker_router_hash:route(Destination, Ctx, Env),
    Copy = Msg#broker_message{
             message_id = crypto:strong_rand_bytes(16),
             destination = Destination,
             partition_id = Partition,
             headers = maps:merge(
                         Msg#broker_message.headers,
                         #{event_rejection => Reason,
                           event_rejection_text => reason_text(Reason),
                           original_destination =>
                               Msg#broker_message.destination}),
             proto_meta = #{},
             persistence = durable,
             reliability = fsync,
             expires_at = undefined,
             event = undefined},
    case broker_queue:append(Partition, Copy, fsync) of
        {ok, _Offset} -> ok;
        {error, _} = Error -> Error
    end.

reason_text(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).

quarantine_target(#broker_message{tenant_id = Tenant, destination = Destination}) ->
    case application:get_env(broker_event, quarantine, []) of
        List when is_list(List) ->
            quarantine_match(Tenant, Destination, List);
        _ ->
            none
    end.

quarantine_match(_T, _D, []) ->
    none;
quarantine_match(T, D, [{PT, PD, Target} | Rest]) ->
    case tenant_matches(PT, T) andalso destination_matches(PD, D) of
        true -> {ok, render_target(Target, T, D)};
        false -> quarantine_match(T, D, Rest)
    end;
quarantine_match(T, D, [_ | Rest]) ->
    quarantine_match(T, D, Rest).

render_target(Target, Tenant, Destination) when is_binary(Target) ->
    binary:replace(
      binary:replace(Target, <<"{tenant}">>, Tenant, [global]),
      <<"{destination}">>, Destination, [global]);
render_target(Target, _Tenant, _Destination) ->
    iolist_to_binary(io_lib:format("~p", [Target])).

%%%_* Matching ----------------------------------------------------------------

first_match(_T, _D, []) ->
    none;
first_match(T, D, [{PT, PD, Policy} | Rest]) ->
    case tenant_matches(PT, T) andalso destination_matches(PD, D) of
        true -> {ok, Policy};
        false -> first_match(T, D, Rest)
    end;
first_match(T, D, [_ | Rest]) ->
    first_match(T, D, Rest).

first_window(_T, _D, []) ->
    none;
first_window(T, D, [{PT, PD, Window} | Rest]) ->
    case tenant_matches(PT, T) andalso destination_matches(PD, D) of
        true -> {ok, Window};
        false -> first_window(T, D, Rest)
    end;
first_window(T, D, [_ | Rest]) ->
    first_window(T, D, Rest).

tenant_matches(<<"*">>, _Tenant) -> true;
tenant_matches(Tenant, Tenant) -> true;
tenant_matches(_, _) -> false.

destination_matches(<<"*">>, _Destination) -> true;
destination_matches(Filter, Destination) when is_binary(Filter) ->
    case mqtt_filter_matches(Destination, Filter) of
        true -> true;
        false -> false
    end;
destination_matches(_, _) -> false.

%% Same §4.7 semantics as `mqtt_filter:matches/2'; kept local because the
%% protocol app depends on broker_event, not the other way round.
mqtt_filter_matches(Topic, Filter) when is_binary(Topic), is_binary(Filter) ->
    match_levels(levels(Topic), levels(Filter));
mqtt_filter_matches(_, _) ->
    false.

levels(Bin) -> binary:split(Bin, <<"/">>, [global]).

match_levels(_Topic, [<<"#">>]) -> true;
match_levels([], []) -> true;
match_levels([_ | T], [<<"+">> | F]) -> match_levels(T, F);
match_levels([L | T], [L | F]) -> match_levels(T, F);
match_levels(_, _) -> false.

%%%_* Misc --------------------------------------------------------------------

inc(Key) ->
    _ = (catch broker_metrics:inc(Key)),
    ok.
