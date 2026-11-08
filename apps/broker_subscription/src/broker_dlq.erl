%% @doc Dead letters (ADR-015 §15.3 / §15.5; S2-E execution plan T-E2).
%%
%% A dead letter is **an ordinary destination** with a configurable name, not a
%% second storage system: the copy is routed by the same `broker_router_hash'
%% and appended with the same `broker_queue:append/3', so a plain subscription
%% to `$dlq/#' consumes it with the code paths that already exist. That is the
%% whole design -- ADR-015 §15.3 refused a `broker_redelivery' app precisely to
%% avoid a second answer to "where does this message live".
%%
%% Three rules this module keeps:
%%
%%  * **The copy is durable.** `reliability = fsync' regardless of the original
%%    level (ADR-015 §15.5): dead-lettering a message weaker than its original
%%    would make the fallback the lossy copy. The original may have been
%%    `os_buffer'; its dead letter may not.
%%  * **The copy can be consumed.** `expires_at = undefined' -- inheriting the
%%    original TTL would make an expired message dead-letter itself into a
%%    record that expires before anyone can read it (S2-E E-4).
%%  * **The evidence survives.** The original id/destination/attempt count travel
%%    in `headers', and the payload bytes are untouched.
%%
%% Naming is a template (`dlq_name_pattern', default `"$dlq/{tenant}/{original}"')
%% with exactly four variables: `{tenant}', `{original}', `{qos}', `{kind}'.
%% `validate_pattern/1' runs at boot, so a typo fails the application instead of
%% waiting for the first dead letter (a config error that only shows up during an
%% incident is the worst kind).
-module(broker_dlq).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-export([validate_pattern/1, name/2, dead_letter/3]).

-define(DEFAULT_PATTERN, <<"$dlq/{tenant}/{original}">>).

-type meta() :: #{qos := 0..2, kind := exclusive | shared | failover,
                  attempts := pos_integer()}.

%%%_* API ---------------------------------------------------------------------

%% @doc Boot-time validation of a naming template: no MQTT wildcards (a
%% dead-letter destination containing `+'/`#' could never be published to
%% unambiguously) and no unknown `{variable}'.
-spec validate_pattern(binary()) -> ok | {error, term()}.
validate_pattern(Pattern) when is_binary(Pattern), Pattern =/= <<>> ->
    case binary:match(Pattern, [<<"+">>, <<"#">>]) of
        nomatch ->
            case variables(Pattern, []) of
                {ok, _} -> ok;
                {error, _} = Error -> Error
            end;
        _ ->
            {error, wildcard_in_pattern}
    end;
validate_pattern(_) ->
    {error, {invalid_pattern, not_a_nonempty_binary}}.

%% @doc Render the destination for one dead letter.
-spec name(binary(), map()) -> binary().
name(Pattern, #{tenant := Tenant, original := Original,
                qos := QoS, kind := Kind}) ->
    Bindings = #{<<"tenant">> => Tenant,
                 <<"original">> => Original,
                 <<"qos">> => integer_to_binary(QoS),
                 <<"kind">> => atom_to_binary(Kind, utf8)},
    iolist_to_binary(render(Pattern, Bindings)).

%% @doc Read the original record at `{PartitionId, Offset}', append a wrapped
%% copy to the dead-letter destination at `fsync', and account for it.
%%
%% Failure is returned, never swallowed: the caller (the retry scheduler) must
%% leave the cursor where it is, or a full disk would silently delete the
%% message it was supposed to preserve.
-spec dead_letter(partition_id(), offset(), meta()) ->
          ok | {error, term()}.
dead_letter(PartitionId, Offset, #{qos := QoS, kind := Kind} = Meta) ->
    case broker_queue:read_from(PartitionId, Offset, 1) of
        {ok, [{Offset, #broker_message{} = Original}], _Next} ->
            case is_dead_letter(Original) of
                true ->
                    %% A dead letter that timed out again. Re-dead-lettering it
                    %% would nest `$dlq/tenant/$dlq/tenant/...' forever; the
                    %% evidence is already in the DLQ, so the caller only owes
                    %% the cursor an advance.
                    {error, already_dead_letter};
                false ->
                    append_copy(Original, QoS, Kind, Meta)
            end;
        {ok, _Other, _Next} ->
            %% The reservation (`High') named an offset whose append had not
            %% landed yet, or the segment is gone: nothing to copy. It is still
            %% a dead letter that could not be produced, so the failure counter
            %% moves -- the caller will park the offset and try again (§15.5).
            broker_metrics:inc(dlq_append_failed),
            {error, {original_not_readable, PartitionId, Offset}};
        {error, Reason} ->
            broker_metrics:inc(dlq_append_failed),
            {error, {read_failed, Reason}}
    end.

%% @doc Was this record produced by `dead_letter/3'? Used to refuse nested
%% dead-lettering (and by tests).
-spec is_dead_letter(#broker_message{}) -> boolean().
is_dead_letter(#broker_message{headers = Headers}) when is_map(Headers) ->
    maps:is_key(<<"x-original-message-id">>, Headers).

%%%_* Internals ----------------------------------------------------------------

append_copy(#broker_message{} = Original, QoS, Kind, Meta) ->
    Pattern = application:get_env(broker_subscription, dlq_name_pattern,
                                  ?DEFAULT_PATTERN),
    Tenant = Original#broker_message.tenant_id,
    Destination = name(Pattern, #{tenant => Tenant,
                                  original => Original#broker_message.destination,
                                  qos => QoS, kind => Kind}),
    Attempts = maps:get(attempts, Meta, 1),
    Copy = #broker_message{
              message_id = crypto:strong_rand_bytes(16),
              tenant_id = Tenant,
              destination = Destination,
              partition_id = Original#broker_message.partition_id,
              payload = Original#broker_message.payload,
              payload_encoding = Original#broker_message.payload_encoding,
              headers = maps:merge(
                          Original#broker_message.headers,
                          #{<<"x-original-message-id">> =>
                                Original#broker_message.message_id,
                            <<"x-original-destination">> =>
                                Original#broker_message.destination,
                            <<"x-failure-reason">> => <<"max_attempts">>,
                            <<"x-attempts">> => integer_to_binary(Attempts),
                            <<"x-tenant">> => Tenant}),
              proto_meta = #{},
              persistence = durable,
              reliability = fsync,
              created_at = erlang:system_time(millisecond),
              expires_at = undefined,
              event = undefined},
    case route(Destination, Tenant) of
        {ok, Partition} ->
            case (catch broker_queue:append(Partition,
                                            Copy#broker_message{partition_id = Partition},
                                            fsync)) of
                {ok, _Offset} ->
                    broker_metrics:inc(dlq_appends),
                    _ = broker_metrics:gauge_add(dlq_size, 1),
                    ok;
                Other ->
                    broker_metrics:inc(dlq_append_failed),
                    {error, {append_failed, Other}}
            end;
        {error, Reason} ->
            broker_metrics:inc(dlq_append_failed),
            {error, Reason}
    end.

route(Destination, Tenant) ->
    try
        {ok, Env} = broker_router:env(),
        Ctx = #route_ctx{tenant_id = Tenant, publisher = <<"$dlq">>,
                         reliability = fsync},
        %% The hash router's contract is a non-empty list (`{ok, Matches}'
        %% always names at least one partition), so the head is the destination.
        %% The surrounding `try' keeps a violated contract a value, not a crash.
        {ok, [{Partition, _Refs} | _]} =
            broker_router_hash:route(Destination, Ctx, Env),
        {ok, Partition}
    catch
        Class:Err -> {error, {route_failed, {Class, Err}}}
    end.

%%%_* Template ----------------------------------------------------------------

variables(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
variables(Bin, Acc) ->
    case binary:split(Bin, <<"{">>) of
        [_NoMoreVariables] ->
            {ok, lists:reverse(Acc)};
        [_Before, Rest] ->
            case binary:split(Rest, <<"}">>) of
                [Name, Tail] ->
                    case known(Name) of
                        true -> variables(Tail, [Name | Acc]);
                        false -> {error, {unknown_variable, Name}}
                    end;
                _ ->
                    {error, unterminated_variable}
            end
    end.

known(<<"tenant">>) -> true;
known(<<"original">>) -> true;
known(<<"qos">>) -> true;
known(<<"kind">>) -> true;
known(_) -> false.

render(<<>>, _Bindings) ->
    [];
render(Bin, Bindings) ->
    case binary:split(Bin, <<"{">>) of
        [_NoMoreVariables] ->
            [Bin];
        [Before, Rest] ->
            case binary:split(Rest, <<"}">>) of
                [Name, Tail] ->
                    [Before, maps:get(Name, Bindings, <<>>) | render(Tail, Bindings)];
                _ ->
                    %% validate_pattern/1 rejected this at boot; a literal is the
                    %% least surprising fallback for a caller that skipped it.
                    [Before, Rest]
            end
    end.
