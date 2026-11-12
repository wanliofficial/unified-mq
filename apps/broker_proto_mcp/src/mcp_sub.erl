%% @doc `subscriptions/listen' filter mapping (ADR-027 §D5).
%%
%% Pure: maps a request's `params.notifications' filter to the set of kernel
%% destinations the gateway subscribes to, plus the MCP notification method each
%% destination carries. The destination itself encodes the notification type, so
%% forwarding is a lookup with no per-message interpretation.
-module(mcp_sub).

-export([plan/1, acknowledged/1]).

-type entry() :: #{dest := binary(), method := binary(),
                   uri => binary(), field := binary()}.

%% @doc The subscriptions the gateway will honour for a filter. Fields absent
%% (or false) produce no entry; `resourceSubscriptions' produces one per URI.
-spec plan(map() | term()) -> [entry()].
plan(Filter) when is_map(Filter) ->
    lists:append([
       flag(<<"toolsListChanged">>, Filter,
            <<"$mcp/notify/tools/list_changed">>,
            <<"notifications/tools/list_changed">>),
       flag(<<"promptsListChanged">>, Filter,
            <<"$mcp/notify/prompts/list_changed">>,
            <<"notifications/prompts/list_changed">>),
       flag(<<"resourcesListChanged">>, Filter,
            <<"$mcp/notify/resources/list_changed">>,
            <<"notifications/resources/list_changed">>),
       uris(maps:get(<<"resourceSubscriptions">>, Filter, undefined))
      ]);
plan(_Other) ->
    [].

flag(Field, Filter, Dest, Method) ->
    case maps:get(Field, Filter, false) of
        true -> [#{dest => Dest, method => Method, field => Field}];
        _ -> []
    end.

uris(Uris) when is_list(Uris) ->
    [#{dest => uri_dest(U), method => <<"notifications/resources/updated">>,
       uri => U, field => <<"resourceSubscriptions">>}
     || U <- Uris, valid_uri(U)];
uris(_) ->
    [].

valid_uri(U) when is_binary(U), U =/= <<>> ->
    binary:match(U, <<"\0">>) =:= nomatch;
valid_uri(_) ->
    false.

uri_dest(U) ->
    <<"$mcp/notify/resources/updated/", U/binary>>.

%% @doc The `notifications' object to echo back in the acknowledgment: only the
%% subset the gateway agreed to honour (spec: reflect the agreed subset).
-spec acknowledged([entry()]) -> map().
acknowledged(Entries) ->
    Tools = has_field(Entries, <<"toolsListChanged">>),
    Prompts = has_field(Entries, <<"promptsListChanged">>),
    Resources = has_field(Entries, <<"resourcesListChanged">>),
    Uris = [U || #{uri := U} <- Entries],
    Base = #{},
    B1 = case Tools of true -> Base#{<<"toolsListChanged">> => true}; false -> Base end,
    B2 = case Prompts of true -> B1#{<<"promptsListChanged">> => true}; false -> B1 end,
    B3 = case Resources of true -> B2#{<<"resourcesListChanged">> => true}; false -> B2 end,
    case Uris of
        [] -> B3;
        _ -> B3#{<<"resourceSubscriptions">> => Uris}
    end.

has_field(Entries, Field) ->
    lists:any(fun(#{field := F}) -> F =:= Field end, Entries).
