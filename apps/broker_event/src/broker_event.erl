%% @doc CloudEvents policy seam (T11, ADR-004): a pure-function stub whose
%% only S1 job is to be honest about doing nothing.
%%
%% README §3.4 names five policies. S1 implements the two that require no
%% parsing (`pass_through' and `disabled') and rejects the other three instead
%% of pretending: a deployment configured for `required' or `normalize' must
%% fail at the call site, not silently ship unvalidated events behind a
%% validated-looking config file.
%%
%% Why S1 does not even detect (OQ-5 option (a)): MQTT 3.1.1 has no PUBLISH
%% properties, so `content-type' is simply not on the S1 wire. The only way to
%% "see" CloudEvents would be sniffing payload bytes, which ADR-004 forbids
%% (CloudEvents is optional and never forced) and which S0.5 measured as the
%% CPU cost centre (~53 µs/event even with zero extensions, 8 ms at 1024).
%% So the stub is the identity, and the S1 data path does not call it at all:
%% `mqtt_conn_fsm:build_message/5' leaves `event = undefined'. `apply_policy/1'
%% exists so S3 can wire the seam in without a shape change.
%%
%% Reads are pure `application:get_env/3' (J3), like `broker_security'; there
%% is no process here and no state to lose.
-module(broker_event).

-include_lib("broker_core/include/broker_message.hrl").
-include("cloud_event.hrl").

-export([policy/0, apply_policy/1, apply_policy/2]).

-export_type([policy/0, cloud_event/0]).

%% README §3.4 in full. The type is complete even though S1 implements two
%% arms: the S3 policy switch should not have to widen a frozen type.
-type policy() :: disabled | pass_through | validate_if_present | required | normalize.

%% The S3 codec's output shape, exported so downstream code can name it.
-type cloud_event() :: #cloud_event{}.

%% @doc The configured value, verbatim. Deliberately total and unvalidated: a
%% typo should fail where a message is, not at config load, and
%% `apply_policy/2' is that place. A well-formed deployment returns a
%% `policy()'; anything else comes back as-is for `apply_policy/2' to reject.
-spec policy() -> term().
policy() ->
    application:get_env(broker_event, policy, pass_through).

%% @doc Apply the configured policy to one inbound message.
%%
%% `pass_through' (S1 default) and `disabled' are both the identity: the
%% message is returned term-for-term, so payload bytes, `headers', `event' and
%% `proto_meta' are exactly what the protocol layer built. `pass_through' means
%% "keep the raw event, do not validate it"; `disabled' means "treat it as an
%% ordinary message". Neither invents an event view, which is why both are
%% legal S1 answers.
-spec apply_policy(#broker_message{}) -> {ok, #broker_message{}} | {error, term()}.
apply_policy(Msg) ->
    apply_policy(Msg, policy()).

%% @doc Same, with the policy supplied by the caller (per-tenant/destination
%% policies are S3's job; the arity exists so that change needs no new seam).
-spec apply_policy(#broker_message{}, policy() | term()) ->
    {ok, #broker_message{}} | {error, term()}.
apply_policy(#broker_message{} = Msg, Policy) ->
    case Policy of
        pass_through ->
            {ok, Msg};
        disabled ->
            {ok, Msg};
        %% Explicit, not a silent identity: these three promise validation,
        %% normalisation or rejection semantics S1 does not implement.
        validate_if_present -> {error, {policy_not_implemented, Policy, s3}};
        required -> {error, {policy_not_implemented, Policy, s3}};
        normalize -> {error, {policy_not_implemented, Policy, s3}};
        _ ->
            {error, {invalid_policy, Policy}}
    end;
apply_policy(Other, _Policy) ->
    %% A hand-built caller passing something else is a bug; report it rather
    %% than crash the publisher's process (the seam must stay total).
    {error, {not_a_broker_message, Other}}.
