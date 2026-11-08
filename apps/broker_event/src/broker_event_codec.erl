%% @doc CloudEvents 1.0 JSON codec (ADR-020 §D1~D3).
%%
%% Two directions, both pure:
%%
%%   * Structured Mode: the whole event is a JSON object in the payload.
%%   * Binary Mode: the event context is a set of `ce-*' metadata entries and
%%     the payload is the event data bytes.
%%
%% The decoded view is the map ADR-020 §D2 freezes for
%% `#broker_message{}.event'. Optional attributes are absent rather than set to
%% `undefined'; `extensions' is always present so callers never branch on its
%% presence.
%%
%% Validation is deliberately strict about the things the spec makes strict
%% (required attributes, attribute types, extension name/value shape,
%% `data'/`data_base64' exclusivity) and deliberately does not re-serialize
%% `data': the caller decides whether bytes survive (ADR-005 / README §4.1).
-module(broker_event_codec).

-include("cloud_event.hrl").

-export([decode_structured/1, encode_structured/1,
         decode_binary/3, encode_binary/1,
         validate/1, is_event/1,
         to_map/1]).

-export_type([event/0, mode/0]).

-type mode() :: structured | binary.
-type extension_value() :: binary() | boolean() | integer() | float().
-type event() :: #{specversion := binary(),
                   id := binary(),
                   source := binary(),
                   type := binary(),
                   extensions := #{binary() => extension_value()},
                   atom() => term()}.

-define(SPECVERSION, <<"1.0">>).
-define(MAX_EXTENSIONS, 32).
-define(MAX_EXTENSION_NAME, 20).
-define(CE_CONTENT_TYPE, <<"application/cloudevents+json">>).

-define(CORE, [<<"specversion">>, <<"id">>, <<"source">>, <<"type">>,
               <<"subject">>, <<"time">>, <<"datacontenttype">>,
               <<"dataschema">>]).

%%%_* Structured Mode ---------------------------------------------------------

%% @doc Parse a structured JSON event. Returns `{error, {invalid_event, Why}}'
%% for anything that is JSON but not a legal CloudEvents 1.0 event, and
%% `{error, {invalid_json, Why}}' when the bytes are not JSON at all.
-spec decode_structured(binary()) ->
          {ok, event()} | {error, {invalid_json | invalid_event, term()}}.
decode_structured(Bin) when is_binary(Bin) ->
    try json:decode(Bin) of
        Map when is_map(Map) ->
            from_json_map(Map);
        _ ->
            {error, {invalid_event, not_a_json_object}}
    catch
        Class:Reason ->
            {error, {invalid_json, {Class, Reason}}}
    end;
decode_structured(_) ->
    {error, {invalid_json, not_a_binary}}.

%% @doc Encode an event map as a structured JSON body. Validation runs first:
%% encoding an invalid event would produce bytes a peer must then reject.
-spec encode_structured(event()) -> {ok, binary()} | {error, term()}.
encode_structured(Event) when is_map(Event) ->
    case validate(Event) of
        ok -> {ok, iolist_to_binary(json:encode(to_json_map(Event)))};
        {error, _} = Error -> Error
    end;
encode_structured(Other) ->
    {error, {not_an_event, Other}}.

%% @doc Validate a decoded view. Total: never crashes on a hand-built map.
-spec validate(map()) -> ok | {error, {invalid_event, term()}}.
validate(Event) when is_map(Event) ->
    Required = [specversion, id, source, type],
    case missing(Required, Event) of
        [] ->
            validate_required(Required, Event);
        Missing ->
            {error, {invalid_event, {missing, Missing}}}
    end;
validate(Other) ->
    {error, {invalid_event, {not_a_map, Other}}}.

%% @doc Cheap "does this JSON object look like a CloudEvents event?" used for
%% detection. It does NOT validate; `decode_structured/1' does.
-spec is_event(term()) -> boolean().
is_event(#{<<"specversion">> := _}) -> true;
is_event(#{specversion := _}) -> true;
is_event(_) -> false.

%%%_* Binary Mode -------------------------------------------------------------

%% @doc Build an event view from `ce-*' metadata, a content type and the
%% payload bytes. Missing `ce-specversion' means "not a CloudEvents message",
%% which is a distinct result from "a malformed CloudEvents message".
-spec decode_binary(binary(), [{binary(), binary()}], binary() | undefined) ->
          {ok, event()} | {error, not_cloudevent | {invalid_event, term()}}.
decode_binary(Body, Props, ContentType)
  when is_binary(Body), is_list(Props) ->
    CeProps = [{strip_ce(K), V} || {K, V} <- Props, is_ce_key(K)],
    case lists:keyfind(<<"specversion">>, 1, CeProps) of
        false ->
            {error, not_cloudevent};
        _ ->
            Context = context_from_ce(CeProps),
            Event0 = Context#{extensions => extensions_from_ce(CeProps),
                              data => Body},
            Event = maybe_content_type(Event0, ContentType),
            case validate(Event) of
                ok -> {ok, Event};
                {error, Reason} -> {error, Reason}
            end
    end;
decode_binary(_Body, _Props, _ContentType) ->
    {error, {invalid_event, bad_binary_input}}.

%% @doc Turn an event into binary-mode parts. `Data' is the raw payload; when
%% the event only carries a JSON `data' term it is serialized, and the content
%% type default is `application/json' when the event did not name one.
-spec encode_binary(event()) ->
          {ok, binary(), [{binary(), binary()}], binary() | undefined}
              | {error, term()}.
encode_binary(Event) when is_map(Event) ->
    case validate(Event) of
        ok ->
            case data_bytes(Event) of
                {ok, Data, Reserialized} ->
                    Props = [{<<"ce-", (name_of(K))/binary>>, to_bin(V)}
                             || {K, V} <- core_for_binary(Event)],
                    Ext = [{<<"ce-", Name/binary>>, to_bin(V)}
                           || {Name, V} <- maps:to_list(
                                              maps:get(extensions, Event, #{}))],
                    CT = binary_content_type(Event, Reserialized),
                    {ok, Data, Props ++ Ext, CT};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end;
encode_binary(Other) ->
    {error, {not_an_event, Other}}.

%%%_* Map <-> record ---------------------------------------------------------

%% @doc Convert the reserved `#cloud_event{}' interface (README §3.1) into the
%% runtime map. Kept for compatibility; the runtime never stores the record.
to_map(Record) when is_tuple(Record) ->
    Fields = record_info(fields, cloud_event),
    Values = tl(tuple_to_list(Record)),
    Pairs = lists:zip(Fields, Values),
    Map = maps:from_list(
            [{K, V} || {K, V} <- Pairs, V =/= undefined,
                       K =/= extensions]),
    Map#{extensions => maps:get(extensions, maps:from_list(Pairs), #{})};
to_map(Map) when is_map(Map) ->
    Map.

%%%_* Structured internals ----------------------------------------------------

from_json_map(Map0) ->
    %% JSON keys are binaries. Atomize only the eight reserved names; every
    %% other key stays a binary and is treated as an extension.
    Reserved = [{K, atom_of(K)} || K <- ?CORE ++ [<<"data">>, <<"data_base64">>]],
    {Core, Exts0} = split_reserved(Map0, Reserved),
    Exts = maps:filter(fun(_, V) -> V =/= null end, Exts0),
    Event0 = Core#{extensions => Exts},
    case has_both_data(Event0) of
        true ->
            {error, {invalid_event, {mutually_exclusive, data, data_base64}}};
        false ->
            case validate(Event0) of
                ok -> {ok, Event0};
                {error, _} = Error -> Error
            end
    end.

split_reserved(Map, Reserved) ->
    maps:fold(
      fun(Key, Value, {Core, Exts}) ->
              case lists:keyfind(Key, 1, Reserved) of
                  {_, Atom} -> {Core#{Atom => Value}, Exts};
                  false -> {Core, Exts#{Key => Value}}
              end
      end, {#{}, #{}}, Map).

has_both_data(#{data := _, data_base64 := _}) -> true;
has_both_data(_) -> false.

atom_of(<<"specversion">>) -> specversion;
atom_of(<<"id">>) -> id;
atom_of(<<"source">>) -> source;
atom_of(<<"type">>) -> type;
atom_of(<<"subject">>) -> subject;
atom_of(<<"time">>) -> time;
atom_of(<<"datacontenttype">>) -> datacontenttype;
atom_of(<<"dataschema">>) -> dataschema;
atom_of(<<"data">>) -> data;
atom_of(<<"data_base64">>) -> data_base64.

to_json_map(Event) ->
    Core = maps:with([specversion, id, source, type, subject, time,
                      datacontenttype, dataschema, data, data_base64], Event),
    CoreJson = maps:from_list([{atom_to_binary(K, utf8), V}
                               || {K, V} <- maps:to_list(Core)]),
    Exts = maps:get(extensions, Event, #{}),
    maps:merge(CoreJson, Exts).

%%%_* Validation internals ----------------------------------------------------

missing([], _Event) -> [];
missing([K | Rest], Event) ->
    case maps:find(K, Event) of
        {ok, _} -> missing(Rest, Event);
        error -> [K | missing(Rest, Event)]
    end.

validate_required([], Event) ->
    validate_optional(Event);
validate_required([K | Rest], Event) ->
    case valid_string(maps:get(K, Event)) of
        true -> validate_required(Rest, Event);
        false -> {error, {invalid_event, {bad_type, K}}}
    end.

validate_optional(Event) ->
    case maps:find(specversion, Event) of
        {ok, ?SPECVERSION} ->
            validate_optional_fields(Event);
        {ok, Other} ->
            {error, {invalid_event, {unsupported_specversion, Other}}};
        error ->
            {error, {invalid_event, {missing, [specversion]}}}
    end.

validate_optional_fields(Event) ->
    Checks = [{subject, fun valid_string/1},
              {datacontenttype, fun valid_string/1},
              {dataschema, fun valid_string/1},
              {time, fun valid_time/1}],
    case first_bad(Checks, Event) of
        none ->
            validate_extensions(maps:get(extensions, Event, #{}), Event);
        {Bad, Value} ->
            {error, {invalid_event, {bad_type, Bad, Value}}}
    end.

first_bad([], _Event) -> none;
first_bad([{K, F} | Rest], Event) ->
    case maps:find(K, Event) of
        {ok, V} ->
            case F(V) of
                true -> first_bad(Rest, Event);
                false -> {K, V}
            end;
        error ->
            first_bad(Rest, Event)
    end.

validate_extensions(Exts, _Event) when map_size(Exts) > ?MAX_EXTENSIONS ->
    {error, {invalid_event, {too_many_extensions, map_size(Exts)}}};
validate_extensions(Exts, Event) ->
    validate_ext_list(maps:to_list(Exts), Event).

validate_ext_list([], Event) ->
    validate_data(Event);
validate_ext_list([{Name, Value} | Rest], Event) ->
    case valid_extension_name(Name) of
        false ->
            {error, {invalid_event, {bad_extension_name, Name}}};
        true ->
            case valid_extension_value(Value) of
                true -> validate_ext_list(Rest, Event);
                false -> {error, {invalid_event, {bad_extension_value, Name}}}
            end
    end.

validate_data(Event) ->
    case {maps:find(data, Event), maps:find(data_base64, Event)} of
        {{ok, _}, {ok, _}} ->
            {error, {invalid_event, {mutually_exclusive, data, data_base64}}};
        {_, {ok, B64}} ->
            case valid_base64(B64) of
                true -> ok;
                false -> {error, {invalid_event, {bad_base64, data_base64}}}
            end;
        _ ->
            ok
    end.

valid_string(V) when is_binary(V), byte_size(V) > 0 ->
    case unicode:characters_to_binary(V, utf8, utf8) of
        V -> true;
        _ -> false
    end;
valid_string(_) ->
    false.

valid_time(V) when is_binary(V) ->
    try calendar:rfc3339_to_system_time(binary_to_list(V),
                                       [{unit, microsecond}]) of
        _ -> true
    catch
        _:_ -> false
    end;
valid_time(_) ->
    false.

valid_extension_name(Name) when is_binary(Name) ->
    byte_size(Name) >= 1 andalso
        byte_size(Name) =< ?MAX_EXTENSION_NAME andalso
        re:run(Name, "^[a-z0-9]+$", [{capture, none}]) =:= match;
valid_extension_name(_) ->
    false.

valid_extension_value(V) when is_binary(V) -> true;
valid_extension_value(V) when is_boolean(V) -> true;
valid_extension_value(V) when is_integer(V) -> true;
valid_extension_value(V) when is_float(V) -> true;
valid_extension_value(_) -> false.

valid_base64(V) when is_binary(V) ->
    try base64:decode(V) of
        _ -> true
    catch
        _:_ -> false
    end;
valid_base64(_) ->
    false.

%%%_* Binary internals --------------------------------------------------------

is_ce_key(<<"ce-", _/binary>>) -> true;
is_ce_key(_) -> false.

strip_ce(<<"ce-", Rest/binary>>) -> Rest.

context_from_ce(CeProps) ->
    lists:foldl(
      fun({K, V}, Acc) ->
              case context_key(K) of
                  undefined -> Acc;
                  Key -> Acc#{Key => V}
              end
      end, #{}, CeProps).

context_key(<<"specversion">>) -> specversion;
context_key(<<"id">>) -> id;
context_key(<<"source">>) -> source;
context_key(<<"type">>) -> type;
context_key(<<"subject">>) -> subject;
context_key(<<"time">>) -> time;
context_key(<<"datacontenttype">>) -> datacontenttype;
context_key(<<"dataschema">>) -> dataschema;
context_key(_) -> undefined.

extensions_from_ce(CeProps) ->
    lists:foldl(
      fun({K, V}, Acc) ->
              case context_key(K) of
                  undefined -> Acc#{K => V};
                  _ -> Acc
              end
      end, #{}, CeProps).

maybe_content_type(Event, undefined) ->
    Event;
maybe_content_type(Event, CT) ->
    case maps:is_key(datacontenttype, Event) of
        true -> Event;
        false -> Event#{datacontenttype => CT}
    end.

core_for_binary(Event) ->
    [{K, maps:get(K, Event)}
     || K <- [specversion, id, source, type, subject, time,
              datacontenttype, dataschema],
        maps:is_key(K, Event)].

name_of(specversion) -> <<"specversion">>;
name_of(id) -> <<"id">>;
name_of(source) -> <<"source">>;
name_of(type) -> <<"type">>;
name_of(subject) -> <<"subject">>;
name_of(time) -> <<"time">>;
name_of(datacontenttype) -> <<"datacontenttype">>;
name_of(dataschema) -> <<"dataschema">>.

data_bytes(#{data_base64 := B64}) ->
    try {ok, base64:decode(B64), false}
    catch _:_ -> {error, {invalid_event, {bad_base64, data_base64}}}
    end;
data_bytes(#{data := Bin}) when is_binary(Bin) ->
    {ok, Bin, false};
data_bytes(#{data := Term}) ->
    {ok, iolist_to_binary(json:encode(Term)), true};
data_bytes(_) ->
    {ok, <<>>, false}.

binary_content_type(#{datacontenttype := CT}, _Reserialized) ->
    CT;
binary_content_type(_Event, true) ->
    <<"application/json">>;
binary_content_type(_Event, false) ->
    undefined.

to_bin(V) when is_binary(V) -> V;
to_bin(true) -> <<"true">>;
to_bin(false) -> <<"false">>;
to_bin(V) when is_integer(V) -> integer_to_binary(V);
to_bin(V) when is_float(V) -> float_to_binary(V, [short]);
to_bin(V) -> iolist_to_binary(io_lib:format("~p", [V])).
