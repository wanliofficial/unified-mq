%% @doc Agent Card discovery (ADR-026 §D3, convergence report §5).
%%
%% Two HTTP surfaces, both tenant-scoped even when anonymous:
%%
%%   * `GET /.well-known/agent-card.json' (RFC 8615) -- the public card. When
%%     the caller presents a valid credential, the extended card is returned if
%%     the row has one; otherwise the public card. `ETag' comes from the
%%     monotonic `version' plus a content hash, and `If-None-Match' answers 304.
%%   * `GET /agents' -- a project-private catalog. The A2A spec does not define
%%     a registry API (R-D4), so this is explicitly ours: filters by skill/tag/
%%     provider/capability, default page size 50, hard cap 100.
%%
%% Cards live in `broker_meta.agent_card' (low-frequency control plane). Nothing
%% here writes on the read path; the only writer is `seed/0' at boot. Cards carry
%% no static secrets (R-D6): `securitySchemes' may only declare an external
%% scheme/URL.
-module(a2a_card).

-export([route/1, handle/3, extended/2, seed/0, validate/1,
         etag/2, matches_etag/2]).

-define(WELL_KNOWN, <<"/.well-known/agent-card.json">>).
-define(CATALOG, <<"/agents">>).
-define(PAGE_DEFAULT, 50).
-define(PAGE_MAX, 100).

%%%_* Routing ---------------------------------------------------------------

%% @doc Classify a GET path. The query string stays with the caller so the
%% JSON-RPC router can keep matching exact paths.
-spec route(binary()) -> {ok, well_known | catalog, map()} | error.
route(Path) when is_binary(Path) ->
    {PathPart, QueryPart} = split_query(Path),
    case PathPart of
        ?WELL_KNOWN -> {ok, well_known, parse_query(QueryPart)};
        ?CATALOG -> {ok, catalog, parse_query(QueryPart)};
        _ -> error
    end.

%% @doc Answer one discovery request. `KeepAlive' is taken from the connection
%% so a discovery GET does not accidentally close a keep-alive socket.
-spec handle(well_known | catalog, map(), map()) ->
    {non_neg_integer(), [{binary(), binary()}], iodata(), boolean()}.
handle(well_known, Query, Req) ->
    with_identity(Req,
                  fun(Identity) -> well_known(Identity, Query, Req) end);
handle(catalog, Query, Req) ->
    with_identity(Req,
                  fun(Identity) -> catalog(Identity, Query, Req) end).

%% @doc `GetExtendedAgentCard' (JSON-RPC). Returns
%% `{error, extended_agent_card_not_configured}' when the selected card has no
%% extended variant -- the same -32007 the spec defines.
-spec extended(binary(), map()) -> {ok, map()} | {error, term()}.
extended(TenantId, Params) ->
    case select(TenantId, Params) of
        {ok, _Row, _Version, _Status, _Card, undefined} ->
            {error, extended_agent_card_not_configured};
        {ok, _Row, _Version, _Status, _Card, Extended} ->
            {ok, Extended};
        not_found ->
            {error, extended_agent_card_not_configured}
    end.

%%%_* Well-known and catalog ------------------------------------------------

well_known(Identity, Query, Req) ->
    Tenant = maps:get(tenant_id, Identity),
    case select(Tenant, Query) of
        not_found ->
            json(404, [], #{<<"error">> => <<"agent_card_not_found">>}, Req);
        {ok, _Row, Version, _Status, Card, Extended} ->
            Body = expose(Identity, Card, Extended),
            ETag = etag(Version, Body),
            Headers = [{<<"etag">>, ETag},
                       {<<"cache-control">>, cache_control()}],
            case matches_etag(if_none_match(Req), ETag) of
                true -> {304, Headers, <<>>, keep(Req)};
                false -> json(200, Headers, Body, Req)
            end
    end.

catalog(Identity, Query, Req) ->
    Tenant = maps:get(tenant_id, Identity),
    Rows = [Row || Row <- broker_meta:agent_cards(Tenant),
                   row_status(Row) =:= active],
    Matching = [Row || Row <- Rows, matches_filters(Row, Query)],
    Sorted = lists:sort(
               fun(A, B) -> row_agent(A) =< row_agent(B) end, Matching),
    {PageSize, Token} = page(Query),
    After = case Token of
                undefined -> Sorted;
                _ -> [Row || Row <- Sorted, row_agent(Row) > Token]
            end,
    {Taken, Rest} = take(PageSize, After),
    Base = #{<<"agents">> => [row_public_card(Row) || Row <- Taken]},
    Body = case Rest of
               [] -> Base;
               _ -> Base#{<<"nextPageToken">> => row_agent(lists:last(Taken))}
           end,
    json(200, [{<<"cache-control">>, cache_control()}], Body, Req).

%% Authenticated callers may see the extended variant; anonymous callers only
%% the public one. The trim is the adapter's job, never broker_meta's.
expose(_Identity, Card, undefined) ->
    Card;
expose(Identity, Card, Extended) ->
    case maps:get(principal, Identity) of
        <<"anonymous">> -> Card;
        _ -> Extended
    end.

%%%_* Selection and filters ------------------------------------------------

select(Tenant, Query) ->
    case maps:get(<<"agent">>, Query, undefined) of
        undefined -> first_active(Tenant);
        AgentId ->
            case broker_meta:get_agent_card(Tenant, AgentId) of
                {ok, Row} -> as_selection(Row);
                not_found -> not_found
            end
    end.

first_active(Tenant) ->
    Active = [Row || Row <- broker_meta:agent_cards(Tenant),
                     row_status(Row) =:= active],
    case lists:sort(fun(A, B) -> row_agent(A) =< row_agent(B) end, Active) of
        [] -> not_found;
        [Row | _] -> as_selection(Row)
    end.

as_selection({agent_card, _Ref, _Tenant, Version, Status, Card, Extended, _}) ->
    {ok, undefined, Version, Status, Card, Extended}.

matches_filters(Row, Query) ->
    Card = row_public_card(Row),
    skill_ok(Card, maps:get(<<"skill">>, Query, undefined)) andalso
        tag_ok(Card, maps:get(<<"tag">>, Query, undefined)) andalso
        provider_ok(Card, maps:get(<<"provider">>, Query, undefined)) andalso
        capability_ok(Card, maps:get(<<"capability">>, Query, undefined)).

skill_ok(_Card, undefined) -> true;
skill_ok(Card, Value) ->
    lists:any(fun(Skill) ->
                      maps:get(<<"id">>, Skill, undefined) =:= Value orelse
                          maps:get(<<"name">>, Skill, undefined) =:= Value orelse
                          lists:member(Value, maps:get(<<"tags">>, Skill, []))
              end, skills(Card)).

tag_ok(_Card, undefined) -> true;
tag_ok(Card, Value) ->
    lists:any(fun(Skill) ->
                      lists:member(Value, maps:get(<<"tags">>, Skill, []))
              end, skills(Card)).

provider_ok(_Card, undefined) -> true;
provider_ok(Card, Value) ->
    Provider = maps:get(<<"provider">>, Card, #{}),
    maps:get(<<"organization">>, Provider, undefined) =:= Value.

capability_ok(_Card, undefined) -> true;
capability_ok(Card, Value) ->
    Capabilities = maps:get(<<"capabilities">>, Card, #{}),
    maps:get(Value, Capabilities, false) =:= true orelse
        lists:any(fun(Extension) ->
                          maps:get(<<"uri">>, Extension, undefined) =:= Value
                  end, maps:get(<<"extensions">>, Capabilities, [])).

skills(Card) ->
    [S || S <- maps:get(<<"skills">>, Card, []), is_map(S)].

%%%_* Seeding ---------------------------------------------------------------

%% @doc Write the configured boot cards into `broker_meta'. Idempotent: an
%% unchanged seed does not bump the version, so a restart does not invalidate
%% every client cache. A malformed seed fails the boot instead of being skipped.
-spec seed() -> ok | {error, term()}.
seed() ->
    Specs = application:get_env(broker_proto_a2a, agent_cards, []),
    seed_specs(Specs).

seed_specs([]) ->
    ok;
seed_specs([Spec | Rest]) ->
    case seed_one(Spec) of
        ok -> seed_specs(Rest);
        {error, _} = Error -> Error
    end;
seed_specs(_Other) ->
    {error, invalid_agent_cards}.

seed_one(#{tenant_id := Tenant, agent_id := Agent} = Spec)
  when is_binary(Tenant), is_binary(Agent) ->
    Card = maps:get(card, Spec, #{}),
    case validate(Card) of
        ok ->
            maybe_put(Tenant, Agent, Spec, Card);
        {error, Reason} ->
            {error, {invalid_agent_card, Agent, Reason}}
    end;
seed_one(Other) ->
    {error, {invalid_agent_card_spec, Other}}.

maybe_put(Tenant, Agent, Spec, Card) ->
    Status = maps:get(status, Spec, active),
    Extended = maps:get(extended_card, Spec, undefined),
    case broker_meta:get_agent_card(Tenant, Agent) of
        {ok, {agent_card, _, _, _, Status, Card, Extended, _}} ->
            %% Same content and status: keep the version so ETags stay stable.
            ok;
        _ ->
            case broker_meta:put_agent_card(Spec) of
                {ok, _Version} -> ok;
                {error, Reason} -> {error, Reason}
            end
    end.

%% @doc The required Agent Card fields from the convergence report §5 / R-D1.
-spec validate(map()) -> ok | {error, term()}.
validate(Card) when is_map(Card) ->
    Required = [<<"name">>, <<"description">>, <<"supportedInterfaces">>,
                <<"version">>, <<"capabilities">>, <<"defaultInputModes">>,
                <<"defaultOutputModes">>, <<"skills">>],
    case [Key || Key <- Required, not maps:is_key(Key, Card)] of
        [] -> validate_shapes(Card);
        Missing -> {error, {missing_fields, Missing}}
    end;
validate(_Other) ->
    {error, not_an_object}.

validate_shapes(Card) ->
    case {maps:get(<<"supportedInterfaces">>, Card),
          maps:get(<<"skills">>, Card),
          maps:get(<<"capabilities">>, Card),
          maps:get(<<"defaultInputModes">>, Card),
          maps:get(<<"defaultOutputModes">>, Card)} of
        {[_ | _], Skills, Capabilities, [_ | _], [_ | _]}
          when is_list(Skills), is_map(Capabilities) ->
            ok;
        _ ->
            {error, invalid_shapes}
    end.

%%%_* ETag ------------------------------------------------------------------

%% @doc `"<version>-<content hash>"'. The version alone is enough for our own
%% writers (it only moves when the row changes), but mixing in the payload makes
%% a manually edited row invalidate caches too.
-spec etag(integer(), map()) -> binary().
etag(Version, Card) ->
    Digest = crypto:hash(sha256, iolist_to_binary(json:encode(Card))),
    Hash = base64url(binary:part(Digest, 0, 12)),
    <<"\"", (integer_to_binary(Version))/binary, "-", Hash/binary, "\"">>.

%% @doc `If-None-Match' comparison. Handles a comma-separated list, the `W/'
%% weak prefix and `*', which is what real caches send.
-spec matches_etag(binary() | undefined, binary()) -> boolean().
matches_etag(undefined, _ETag) -> false;
matches_etag(<<"*">>, _ETag) -> true;
matches_etag(Header, ETag) when is_binary(Header) ->
    Candidates = [trim(Part) || Part <- binary:split(Header, <<",">>, [global])],
    lists:any(fun(Candidate) ->
                      strip_weak(Candidate) =:= ETag
              end, Candidates).

strip_weak(<<"W/", Rest/binary>>) -> Rest;
strip_weak(Other) -> Other.

%%%_* HTTP helpers ----------------------------------------------------------

with_identity(Req, Fun) ->
    case identity(Req) of
        {ok, Identity} ->
            Fun(Identity);
        {error, unauthorized} ->
            {401, [{<<"www-authenticate">>, agent_rpc_auth:challenge()}],
             <<>>, keep(Req)}
    end.

identity(Req) ->
    Headers = maps:get(headers, Req),
    case agent_rpc_auth:credential(Headers) of
        none ->
            {ok, agent_rpc_auth:anonymous()};
        {error, ambiguous} ->
            {error, unauthorized};
        {ok, Credential} ->
            case agent_rpc_auth:authenticate(Credential) of
                {ok, Identity} -> {ok, Identity};
                {error, unauthorized} -> {error, unauthorized}
            end
    end.

json(Status, Headers, Body, Req) ->
    {Status, [{<<"content-type">>, <<"application/json">>} | Headers],
     iolist_to_binary(json:encode(Body)), keep(Req)}.

keep(#{keep_alive := Keep}) -> Keep;
keep(_) -> false.

cache_control() ->
    MaxAge = application:get_env(broker_proto_a2a, agent_card_max_age, 60),
    iolist_to_binary(["max-age=", integer_to_list(MaxAge)]).

if_none_match(Req) ->
    maps:get(<<"if-none-match">>, maps:get(headers, Req), undefined).

page(Query) ->
    Size = case maps:get(<<"pageSize">>, Query, undefined) of
               N when is_integer(N), N > 0 -> min(N, ?PAGE_MAX);
               Bin when is_binary(Bin) ->
                   %% Query-string values are text, not JSON numbers.
                   case string:to_integer(Bin) of
                       {N, <<>>} when N > 0 -> min(N, ?PAGE_MAX);
                       _ -> ?PAGE_DEFAULT
                   end;
               _ -> ?PAGE_DEFAULT
           end,
    Token = case maps:get(<<"pageToken">>, Query, undefined) of
                T when is_binary(T) -> T;
                _ -> undefined
            end,
    {Size, Token}.

take(N, List) when N >= length(List) -> {List, []};
take(N, List) -> lists:split(N, List).

%%%_* Row accessors ---------------------------------------------------------

row_agent({agent_card, {_Tenant, Agent}, _T, _V, _S, _C, _E, _U}) -> Agent.
row_status({agent_card, _Ref, _T, _V, Status, _C, _E, _U}) -> Status.
row_public_card({agent_card, _Ref, _T, _V, _S, Card, _E, _U}) -> Card.

%%%_* Query string ----------------------------------------------------------

split_query(Path) ->
    case binary:split(Path, <<"?">>) of
        [PathPart, QueryPart] -> {PathPart, QueryPart};
        [PathPart] -> {PathPart, <<>>}
    end.

parse_query(<<>>) ->
    #{};
parse_query(Query) ->
    try uri_string:dissect_query(Query) of
        Pairs ->
            maps:from_list([{key(K), V} || {K, V} <- Pairs])
    catch
        _:_ -> #{}
    end.

%% `dissect_query/1' hands back binaries for a binary query and strings for a
%% string one; both shapes are normalized here so a caller's choice of input
%% type cannot silently make every filter inert.
key(K) when is_binary(K) -> K;
key(K) when is_list(K) -> unicode:characters_to_binary(K).

trim(Bin) when is_binary(Bin) ->
    string:trim(Bin, both, " \t").

base64url(Bin) ->
    Encoded = base64:encode(Bin),
    iolist_to_binary([url_char(B) || <<B/integer>> <= Encoded, B =/= $=]).

url_char($+) -> $-;
url_char($/) -> $_;
url_char(B) -> B.
