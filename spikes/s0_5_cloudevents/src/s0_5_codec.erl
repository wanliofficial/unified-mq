%% S0.5 - minimal CloudEvents 1.0 JSON codec + core attribute validation.
%%
%% Scope is deliberately narrow: this exists to MEASURE cost, not to be the
%% production implementation. It implements only what README 3.1/3.2/3.3
%% requires so the benchmark reflects real validation work:
%%
%%   - Structured Mode: whole event as application/cloudevents+json
%%   - core attribute presence/type checks
%%   - data / data_base64 mutual exclusion (README 3.1 explicitly warns about it)
%%   - extension attribute naming rules
-module(s0_5_codec).

-export([decode/1, encode_structured/1, validate/1, event/3]).
-export([bench/3, generate_json/2]).

-define(REQUIRED, [<<"specversion">>, <<"id">>, <<"source">>, <<"type">>]).

%% generate_json(NumExt, DataSize) -> binary()
%% Builds a valid structured-mode JSON event with NumExt extension attributes.
generate_json(NumExt, DataSize) ->
    Base = #{
        <<"specversion">> => <<"1.0">>,
        <<"id">> => <<"1234-5678">>,
        <<"source">> => <<"/unifiedmq/spike">>,
        <<"type">> => <<"com.example.thing.created">>,
        <<"datacontenttype">> => <<"application/json">>,
        <<"time">> => <<"2026-10-14T12:00:00Z">>,
        <<"data">> => #{<<"k">> => binary:copy(<<"v">>, DataSize)}
    },
    Ext = maps:from_list(
        [{extension_name(N), <<"value-", (integer_to_binary(N))/binary>>}
         || N <- lists:seq(1, NumExt)]),
    jsx:encode(maps:merge(Base, Ext)).

%% decode(Bin) -> {ok, Event} | {error, Reason}
decode(Bin) when is_binary(Bin) ->
    try jsx:decode(Bin, [return_maps]) of
        Map when is_map(Map) -> {ok, Map};
        Other -> {error, {not_an_object, Other}}
    catch
        _:Reason -> {error, {invalid_json, Reason}}
    end.

%% validate(Event) -> ok | {error, {invalid, Details}}
%%
%% Core attribute presence and type checks, per README 3.2.
validate(Event) when is_map(Event) ->
    Missing = [K || K <- ?REQUIRED, not maps:is_key(K, Event)],
    case Missing of
        [] ->
            BadType = [{K, maps:get(K, Event)}
                       || K <- ?REQUIRED,
                          not is_binary(maps:get(K, Event))],
            case BadType of
                [] -> validate_optional(Event);
                _ -> {error, {invalid, {non_string_required, BadType}}}
            end;
        _ ->
            {error, {invalid, {missing_required, Missing}}}
    end.

%% encode_structured(Event) -> binary()
encode_structured(Event) ->
    jsx:encode(Event).

%% event(Id, Type, Data) -> map()
event(Id, Type, Data) ->
    #{<<"specversion">> => <<"1.0">>,
      <<"id">> => Id,
      <<"source">> => <<"/unifiedmq">>,
      <<"type">> => Type,
      <<"data">> => Data}.

%% ---------------------------------------------------------------- internals

%% data and data_base64 are mutually exclusive per README 3.1.
validate_optional(Event) ->
    case {maps:is_key(<<"data">>, Event), maps:is_key(<<"data_base64">>, Event)} of
        {true, true} ->
            {error, {invalid, {data_and_data_base64}}};
        _ ->
            validate_time(Event)
    end.

validate_time(Event) ->
    case maps:get(<<"time">>, Event, undefined) of
        undefined -> validate_extensions(Event);
        T when is_binary(T) ->
            case valid_rfc3339(T) of
                true -> validate_extensions(Event);
                false -> {error, {invalid, {bad_time, T}}}
            end;
        T ->
            {error, {invalid, {bad_time, T}}}
    end.

%% Extension names must be lowercase alphanumeric (CloudEvents 1.0).
validate_extensions(Event) ->
    Core = ?REQUIRED ++ [<<"subject">>, <<"time">>, <<"datacontenttype">>,
                         <<"dataschema">>, <<"data">>, <<"data_base64">>],
    Extensions = maps:keys(Event) -- Core,
    case [E || E <- Extensions, not valid_extension_name(E)] of
        [] -> ok;
        Bad -> {error, {invalid, {bad_extension_names, Bad}}}
    end.

%% CloudEvents 1.0 requires extension names to be non-empty lowercase
%% alphanumeric. Non-binary values (numbers, booleans, nested objects) are
%% not extension names at all -- only keys can be -- so anything that is
%% already in Core is excluded before this check runs.
valid_extension_name(<<>>) -> false;
valid_extension_name(Name) when is_binary(Name) ->
    lists:all(fun is_lower_alnum/1, binary_to_list(Name)).

is_lower_alnum(C) ->
    (C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9).

%% Minimal RFC3339 shape check: 2026-10-14T12:00:00Z or with offset.
%% Multi-byte binary patterns bind one byte per segment, so each digit is
%% destructured individually.
valid_rfc3339(<<Y1, Y2, Y3, Y4, $-, M1, M2, $-, D1, D2, $T,
                H1, H2, $:, Mi1, Mi2, $:, S1, S2, Rest/binary>>)
  when Y1 >= $0, Y1 =< $9, Y2 >= $0, Y2 =< $9,
       Y3 >= $0, Y3 =< $9, Y4 >= $0, Y4 =< $9,
       M1 >= $0, M1 =< $1, M2 >= $0, M2 =< $9,
       D1 >= $0, D1 =< $3, D2 >= $0, D2 =< $9,
       H1 >= $0, H1 =< $2, H2 >= $0, H2 =< $9,
       Mi1 >= $0, Mi1 =< $5, Mi2 >= $0, Mi2 =< $9,
       S1 >= $0, S1 =< $6, S2 >= $0, S2 =< $9 ->
    suffix_ok(Rest);
valid_rfc3339(_) -> false.

suffix_ok(<<"Z">>) -> true;
suffix_ok(<<"+", H1, H2, $:, M1, _/binary>>)
  when H1 >= $0, H1 =< $2, H2 >= $0, H2 =< $9, M1 >= $0, M1 =< $5 ->
    true;
suffix_ok(_) -> false.

extension_name(N) ->
    <<"ext", (pad(N))/binary>>.

pad(N) when N < 10 -> <<"0", (integer_to_binary(N))/binary>>;
pad(N) -> integer_to_binary(N).

%% bench(Iterations, NumExt, DataSize) -> map()
bench(Iterations, NumExt, DataSize) ->
    Json = generate_json(NumExt, DataSize),
    %% Warm up so the first timed pass does not pay for code loading.
    _ = loop_validate(200, Json),
    {ok, Event} = decode(Json),

    T0 = erlang:monotonic_time(microsecond),
    loop_decode(Iterations, Json),
    TDecode = erlang:monotonic_time(microsecond) - T0,

    T1 = erlang:monotonic_time(microsecond),
    loop_validate(Iterations, Json),
    TValidate = erlang:monotonic_time(microsecond) - T1,

    T2 = erlang:monotonic_time(microsecond),
    loop_encode(Iterations, Event),
    TEncode = erlang:monotonic_time(microsecond) - T2,

    #{extensions => NumExt,
      data_bytes => DataSize,
      json_bytes => byte_size(Json),
      iterations => Iterations,
      decode_us => per_us(TDecode, Iterations),
      validate_us => per_us(TValidate, Iterations),
      encode_us => per_us(TEncode, Iterations),
      total_us => per_us(TDecode + TValidate + TEncode, Iterations)}.

per_us(Total, N) -> round(Total / N * 10) / 10.

loop_decode(0, _Json) -> ok;
loop_decode(N, Json) ->
    {ok, _} = decode(Json),
    loop_decode(N - 1, Json).

loop_validate(0, _Json) -> ok;
loop_validate(N, Json) ->
    {ok, Event} = decode(Json),
    ok = validate(Event),
    loop_validate(N - 1, Json).

loop_encode(0, _Event) -> ok;
loop_encode(N, Event) ->
    _ = encode_structured(Event),
    loop_encode(N - 1, Event).