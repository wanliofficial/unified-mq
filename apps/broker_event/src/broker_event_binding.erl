%% @doc MQTT binding mapper for CloudEvents (ADR-020 §D5).
%%
%% The transport-facing half of S3: detects which encoding a broker message
%% carries, decodes it through `broker_event_codec', and builds MQTT parts for
%% an event (payload, `proto_meta' updates, conversion report).
%%
%% MQTT 5 has User Properties, so Binary Mode is expressible. MQTT 3.1.1 has
%% no metadata channel at all: Binary Mode there is *lossy by construction*,
%% and this module says so in the report instead of dropping the context
%% silently (README §4.1). Structured Mode works on both versions.
-module(broker_event_binding).

-include_lib("broker_core/include/broker_message.hrl").

-export([detect/1, decode/1, to_mqtt/3, report/4]).

-export_type([mode/0, report/0]).

-type mode() :: structured | binary.
-type report() :: #{mode := mode(),
                    target := mqtt,
                    version := 4 | 5,
                    losses := [binary()],
                    preserved := [binary()]}.

-define(CE_CONTENT_TYPE, <<"application/cloudevents+json">>).

%%%_* Detection ---------------------------------------------------------------

%% @doc Which encoding, if any, this message carries. Binary metadata wins over
%% a structured payload because a v5 client that sets `ce-*' properties and a
%% JSON body means Binary Mode with JSON data.
-spec detect(#broker_message{}) -> {binary, [{binary(), binary()}]}
                                  | {structured, binary()} | none.
detect(#broker_message{payload = Payload, proto_meta = Meta}) ->
    Props = maps:get(mqtt_user_properties, Meta, []),
    case has_ce_props(Props) of
        true ->
            {binary, Props};
        false ->
            case claims_ce(maps:get(mqtt_content_type, Meta, undefined)) of
                true -> {structured, Payload};
                false -> detect_by_payload(Payload)
            end
    end.

has_ce_props(Props) when is_list(Props) ->
    lists:keymember(<<"ce-specversion">>, 1, Props);
has_ce_props(_) ->
    false.

claims_ce(undefined) -> false;
claims_ce(CT) when is_binary(CT) ->
    case binary:split(CT, <<";">>) of
        [Base | _] -> trim(Base) =:= ?CE_CONTENT_TYPE;
        [] -> false
    end;
claims_ce(_) -> false.

trim(B) -> string:trim(B).

%% `payload' is a binary by record type; a hand-built non-binary one is a
%% defect the caller's try reports, not something detection should hide.
detect_by_payload(Payload) ->
    try json:decode(Payload) of
        Map when is_map(Map) ->
            case broker_event_codec:is_event(Map) of
                true -> {structured, Payload};
                false -> none
            end;
        _ ->
            none
    catch
        _:_ -> none
    end.

%%%_* Decode ------------------------------------------------------------------

%% @doc Decode the message's event view. `none' is not an error here: callers
%% apply the policy (some policies accept ordinary messages).
-spec decode(#broker_message{}) ->
          {ok, broker_event_codec:event(), mode()} | {error, term()}.
decode(#broker_message{payload = Payload, proto_meta = Meta} = Msg) ->
    case detect(Msg) of
        {structured, Body} ->
            case broker_event_codec:decode_structured(Body) of
                {ok, Event} -> {ok, Event, structured};
                {error, _} = Error -> Error
            end;
        {binary, Props} ->
            CT = maps:get(mqtt_content_type, Meta, undefined),
            case broker_event_codec:decode_binary(Payload, Props, CT) of
                {ok, Event} -> {ok, Event, binary};
                {error, _} = Error -> Error
            end;
        none ->
            {error, not_cloudevent}
    end.

%%%_* Encode to MQTT ----------------------------------------------------------

%% @doc Build MQTT parts for an event. Returns the payload, the `proto_meta'
%% updates the publisher should merge, and the conversion report.
%%
%% The caller must inspect `losses': a non-empty list means the target version
%% cannot carry that metadata (README §4.1 requires it not be silent).
-spec to_mqtt(broker_event_codec:event(), 4 | 5, mode()) ->
          {ok, binary(), map(), report()} | {error, term()}.
to_mqtt(Event, Vsn, structured) when Vsn =:= 4; Vsn =:= 5 ->
    case broker_event_codec:encode_structured(Event) of
        {ok, Body} ->
            Meta = case Vsn of
                       5 -> #{mqtt_content_type => ?CE_CONTENT_TYPE};
                       4 -> #{}
                   end,
            Preserved = [<<"specversion">>, <<"id">>, <<"source">>,
                         <<"type">>] ++ maps:keys(maps:get(extensions, Event, #{})),
            {ok, Body, Meta, report(structured, Vsn, [], Preserved)};
        {error, _} = Error ->
            Error
    end;
to_mqtt(Event, 5, binary) ->
    case broker_event_codec:encode_binary(Event) of
        {ok, Body, Props, ContentType} ->
            Meta0 = #{mqtt_user_properties => Props},
            Meta = case ContentType of
                       undefined -> Meta0;
                       _ -> Meta0#{mqtt_content_type => ContentType}
                   end,
            {ok, Body, Meta,
             report(binary, 5, binary_losses(Event), ce_names(Event))};
        {error, _} = Error ->
            Error
    end;
to_mqtt(Event, 4, binary) ->
    case broker_event_codec:encode_binary(Event) of
        {ok, Body, _Props, _ContentType} ->
            %% 3.1.1 cannot carry the context. The data bytes still travel; every
            %% attribute is reported as lost.
            {ok, Body, #{},
             report(binary, 4, binary_losses(Event) ++ ce_names(Event), [])};
        {error, _} = Error ->
            Error
    end;
to_mqtt(_Event, Vsn, Mode) ->
    {error, {unsupported_binding, mqtt, Vsn, Mode}}.

%% Binary Mode carries bytes, not JSON terms. A structured event whose `data'
%% is a JSON object is re-serialized, and a numeric extension becomes its text
%% form -- both are non-lossless and are named in the report.
binary_losses(Event) ->
    Data = case {maps:is_key(data_base64, Event), maps:find(data, Event)} of
               {true, _} -> [<<"data_base64">>];
               {false, {ok, Bin}} when is_binary(Bin) -> [];
               {false, {ok, _Term}} -> [<<"data">>];
               {false, error} -> []
           end,
    ExtLosses = [<<"ce-", Name/binary>>
                 || {Name, V} <- maps:to_list(maps:get(extensions, Event, #{})),
                    not is_binary(V)],
    Data ++ ExtLosses.

ce_names(Event) ->
    Core = [<<"ce-", (atom_to_binary(K, utf8))/binary>>
            || K <- [specversion, id, source, type, subject, time,
                     datacontenttype, dataschema],
               maps:is_key(K, Event)],
    Ext = [<<"ce-", Name/binary>> || Name <- maps:keys(
                                                maps:get(extensions, Event, #{}))],
    Core ++ Ext.

-spec report(mode(), 4 | 5, [binary()], [binary()]) -> report().
report(Mode, Vsn, Losses, Preserved) ->
    #{mode => Mode, target => mqtt, version => Vsn,
      losses => Losses, preserved => Preserved}.
