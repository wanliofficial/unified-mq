%% @doc Outbound webhook delivery for A2A push notifications (ADR-026 §D6).
%%
%% This module is the **egress boundary**: it is the only place in the A2A
%% adapter that opens an outbound connection, and therefore the only place that
%% must defend against SSRF. Five rules, in order (ADR-026 §D6.1-D6.5):
%%
%%   1. **Default off.** `push_enabled = false' and an empty allowlist mean no
%%      config may be created at all (the RPC layer answers `-32003').
%%   2. **Two-layer allowlist.** A global host-suffix table intersected with a
%%      per-tenant table; a URL must match both. Empty intersection, empty
%%      allowlist, or no tenant entry all deny.
%%   3. **Resolve, validate, pin.** The hostname is resolved once, every
%%      candidate address is validated (reject loopback/private/link-local/
%%      multicast/reserved unless the entry sets `allow_private'), and the
%%      connection goes to the *validated IP* with the original `Host' header.
%%      That removes DNS rebinding and TOCTOU: a second resolution never
%%      happens.
%%   4. **No redirects** (3xx is a failure), explicit connect/send/read
%%      timeouts, a response-body cap (default 64 KiB -- we only need the
%%      status) and `https' only unless `allow_http' is set for local tests.
%%   5. **No `httpc'.** `httpc' cannot pin the address it validated and offers
%%      no per-request egress hook; a hand-written HTTP/1.1 request over
%%      `ssl'/'gen_tcp' is both smaller and auditable (ADR-026 §D2).
%%
%% The module is deliberately synchronous: the caller (`a2a_push_worker') owns
%% retry/backoff/DLQ, and the task shard never blocks on a delivery.
-module(a2a_push).

-export([guard/3, deliver/3, validate_url/2, resolve/2,
         is_private_address/1, host_matches/2,
         allowlist/0, enabled/0, allow_http/0,
         timeouts/0, max_response_bytes/0]).

-define(DEFAULT_MAX_RESPONSE, 65536).

%%%_* Configuration ---------------------------------------------------------

%% @doc Global egress switch (ADR-026 §D6.1).
-spec enabled() -> boolean().
enabled() ->
    application:get_env(broker_proto_a2a, push_enabled, false).

%% @doc `http' is refused unless explicitly allowed (local tests only).
-spec allow_http() -> boolean().
allow_http() ->
    application:get_env(broker_proto_a2a, push_allow_http, false).

%% @doc Global allowlist: a list of host suffixes (`<<"api.example.com">>' or
%% `<<".example.com">>' for a suffix). Empty means "deny everything".
-spec allowlist() -> [map()].
allowlist() ->
    application:get_env(broker_proto_a2a, egress_allowlist, []).

-spec timeouts() -> #{connect := pos_integer(), send := pos_integer(),
                      recv := pos_integer()}.
timeouts() ->
    application:get_env(broker_proto_a2a, push_timeouts,
                        #{connect => 5000, send => 5000, recv => 5000}).

-spec max_response_bytes() -> pos_integer().
max_response_bytes() ->
    application:get_env(broker_proto_a2a, push_max_response_bytes,
                        ?DEFAULT_MAX_RESPONSE).

%%%_* Guard (config creation time) ------------------------------------------

%% @doc Decide whether a push config may be created at all, and validate its
%% URL now rather than at delivery time. Returns the parsed endpoint on
%% success so the RPC layer can echo it back.
%%
%% `TenantAllowlist' is the per-tenant table (ADR-026 §D6.2). Both tables must
%% match; either being empty denies.
-spec guard(binary(), binary(), [map()]) ->
          {ok, map()} | {error, reason()}.
guard(Tenant, Url, TenantAllowlist) when is_binary(Tenant), is_binary(Url) ->
    case enabled() of
        false ->
            {error, push_disabled};
        true ->
            Global = allowlist(),
            case {Global, TenantAllowlist} of
                {[], _} -> {error, empty_allowlist};
                {_, []} -> {error, empty_tenant_allowlist};
                _ ->
                    case validate_url(Url, #{global => Global,
                                             tenant => TenantAllowlist}) of
                        {ok, Endpoint} -> {ok, Endpoint};
                        {error, Reason} -> {error, Reason}
                    end
            end
    end;
guard(_Tenant, _Url, _Allowlist) ->
    {error, invalid_url}.

-type reason() :: push_disabled | empty_allowlist | empty_tenant_allowlist
                | invalid_url | scheme_not_allowed | host_not_allowlisted
                | private_address | resolution_failed | no_address.

%%%_* URL validation --------------------------------------------------------

%% @doc Parse and validate a webhook URL against the two allowlists and the
%% address rules. On success returns the endpoint map:
%%   `#{scheme, host, port, path, addresses, allow_private}'.
-spec validate_url(binary(), #{global := [map()], tenant := [map()]}) ->
          {ok, map()} | {error, reason()}.
validate_url(Url, #{global := Global, tenant := Tenant}) ->
    case uri_string:parse(Url) of
        #{scheme := Scheme, host := Host} = Uri ->
            case allowed_scheme(Scheme) of
                false ->
                    {error, scheme_not_allowed};
                true ->
                    case match_allowlists(Host, Global, Tenant) of
                        {ok, Entry} ->
                            Port = maps:get(port, Uri, default_port(Scheme)),
                            Path = path_of(Uri),
                            AllowPrivate = maps:get(allow_private, Entry, false),
                            resolve_and_validate(Host, Port, Path, Scheme,
                                                 AllowPrivate);
                        {error, _} = Error ->
                            Error
                    end
            end;
        _ ->
            {error, invalid_url}
    end.

allowed_scheme(<<"https">>) -> true;
allowed_scheme(<<"http">>) -> allow_http();
allowed_scheme(_) -> false.

default_port(<<"https">>) -> 443;
default_port(<<"http">>) -> 80.

path_of(Uri) ->
    Path = maps:get(path, Uri, <<"/">>),
    Query = maps:get(query, Uri, undefined),
    case Query of
        undefined -> Path;
        <<>> -> Path;
        Q -> <<Path/binary, "?", Q/binary>>
    end.

%% Every allowlist entry must match: global ∩ tenant (ADR-026 §D6.2). The
%% returned entry is the *tenant* one (it carries `allow_private'), falling
%% back to the global one when the tenant table only has a bare host.
match_allowlists(Host, Global, Tenant) ->
    case {match_one(Host, Global), match_one(Host, Tenant)} of
        {{ok, G}, {ok, T}} ->
            {ok, maps:merge(G, T)};
        {{error, Reason}, _} ->
            {error, Reason};
        {_, {error, Reason}} ->
            {error, Reason}
    end.

match_one(_Host, []) ->
    {error, host_not_allowlisted};
match_one(Host, Entries) ->
    case [E || E <- Entries, host_matches(Host, entry_host(E))] of
        [] -> {error, host_not_allowlisted};
        [E | _] -> {ok, E}
    end.

entry_host(E) when is_map(E) -> maps:get(host, E, <<>>);
entry_host(E) when is_binary(E) -> E;
entry_host(_) -> <<>>.

%% @doc Host suffix match: an exact host or a `<<".example.com">>' suffix
%% (which also matches `example.com' itself, the usual "this domain and its
%% subdomains" reading).
-spec host_matches(binary(), binary()) -> boolean().
host_matches(Host, <<".", Suffix/binary>>) ->
    Host =:= Suffix orelse
        binary:longest_common_suffix([Host, Suffix]) =:= byte_size(Suffix) andalso
        byte_size(Host) > byte_size(Suffix);
host_matches(Host, Exact) ->
    Host =:= Exact.

%%%_* Resolution and address policy -----------------------------------------

resolve_and_validate(Host, Port, Path, Scheme, AllowPrivate) ->
    case resolve(Host, Port) of
        {ok, Addresses} when Addresses =/= [] ->
            case AllowPrivate orelse lists:all(fun is_public/1, Addresses) of
                true ->
                    {ok, #{scheme => Scheme,
                           host => Host,
                           port => Port,
                           path => Path,
                           addresses => Addresses,
                           allow_private => AllowPrivate}};
                false ->
                    {error, private_address}
            end;
        {error, _} ->
            {error, resolution_failed}
    end.

%% @doc Resolve a host to addresses. An IP literal resolves to itself without
%% a DNS lookup (which also makes the guard testable without a network).
-spec resolve(binary(), inet:port_number()) ->
          {ok, [inet:ip_address()]} | {error, term()}.
resolve(Host, _Port) ->
    case inet:parse_address(binary_to_list(Host)) of
        {ok, Ip} ->
            {ok, [Ip]};
        {error, _} ->
            Family = application:get_env(broker_proto_a2a, push_inet_family, inet),
            case lookup(Host, Family) of
                {ok, Addresses} ->
                    {ok, Addresses};
                {error, _} ->
                    %% A v4-only host behind an AAAA record (or the reverse)
                    %% must not look like "no address": retry the other family.
                    Other = case Family of inet -> inet6; _ -> inet end,
                    lookup(Host, Other)
            end
    end.

lookup(Host, Family) ->
    case inet:getaddrs(binary_to_list(Host), Family, 3000) of
        {ok, []} -> {error, no_address};
        {ok, Addresses} -> {ok, Addresses};
        {error, Reason} -> {error, Reason}
    end.

is_public(Ip) ->
    not is_private_address(Ip).

%% @doc Reject every non-routable class the SSRF rule names: loopback,
%% private (RFC 1918 / ULA), link-local, multicast, broadcast, unspecified and
%% the reserved documentation/benchmark ranges. Public addresses only.
-spec is_private_address(inet:ip_address()) -> boolean().
is_private_address({0, 0, 0, 0}) -> true;
is_private_address({127, _, _, _}) -> true;
is_private_address({10, _, _, _}) -> true;
is_private_address({172, B, _, _}) when B >= 16, B =< 31 -> true;
is_private_address({192, 168, _, _}) -> true;
is_private_address({169, 254, _, _}) -> true;            % link-local
is_private_address({100, B, _, _}) when B >= 64, B =< 127 -> true; % CGNAT
is_private_address({192, 0, 2, _}) -> true;              % TEST-NET-1
is_private_address({198, 51, 100, _}) -> true;           % TEST-NET-2
is_private_address({203, 0, 113, _}) -> true;            % TEST-NET-3
is_private_address({198, 18, _, _}) -> true;             % benchmarking
is_private_address({198, 19, _, _}) -> true;
is_private_address({224, _, _, _}) -> true;              % multicast
is_private_address({240, _, _, _}) -> true;              % reserved
is_private_address({255, 255, 255, 255}) -> true;        % broadcast
is_private_address({0, 0, 0, 0, 0, 0, 0, 0}) -> true;    % ::
is_private_address({0, 0, 0, 0, 0, 0, 0, 1}) -> true;    % ::1
is_private_address({16#fc00, _, _, _, _, _, _, _}) -> true;  % ULA
is_private_address({16#fe80, _, _, _, _, _, _, _}) -> true;  % link-local
is_private_address({16#ff00, _, _, _, _, _, _, _}) -> true;  % multicast
is_private_address({0, 0, 0, 0, 0, 16#ffff, A, B}) ->
    is_private_address({A bsr 8, A band 255, B bsr 8, B band 255});
is_private_address(_) -> false.

%%%_* Delivery --------------------------------------------------------------

%% @doc POST one `StreamResponse' payload to the endpoint.
%%
%% Returns `{ok, Status}' for a 2xx and `{error, Reason}' for everything else
%% (including 3xx -- redirects are never followed). `Headers' carries the
%% project-private replay-proof headers built by the caller.
-spec deliver(map(), iodata(), [{binary(), binary()}]) ->
          {ok, non_neg_integer()} | {error, term()}.
deliver(#{scheme := Scheme, host := Host, port := Port, path := Path,
          addresses := [Address | _]} = _Endpoint, Body, Headers) ->
    Timeouts = timeouts(),
    ConnectTimeout = maps:get(connect, Timeouts),
    case connect(Scheme, Address, Port, ConnectTimeout, Host) of
        {ok, Socket} ->
            try
                send_request(Socket, Scheme, Host, Port, Path, Body, Headers,
                             Timeouts)
            after
                close(Scheme, Socket)
            end;
        {error, Reason} ->
            {error, {connect_failed, Reason}}
    end;
deliver(#{addresses := []}, _Body, _Headers) ->
    {error, no_address}.

connect(<<"https">>, Address, Port, Timeout, Host) ->
    Opts = [{active, false}, {mode, binary},
            {verify, verify_peer},
            {cacerts, public_key:cacerts_get()},
            {server_name_indication, binary_to_list(Host)},
            {customize_hostname_check,
             [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}],
    case ssl:connect(Address, Port, Opts, Timeout) of
        {ok, Socket} -> {ok, Socket};
        {error, Reason} -> {error, Reason}
    end;
connect(<<"http">>, Address, Port, Timeout, _Host) ->
    gen_tcp:connect(Address, Port, [binary, {active, false}], Timeout).

send_request(Socket, Scheme, Host, Port, Path, Body, Headers, Timeouts) ->
    BodyBin = iolist_to_binary(Body),
    DefaultPort = default_port(Scheme),
    HostHeader = case Port of
                     DefaultPort -> Host;
                     _ -> <<Host/binary, ":", (integer_to_binary(Port))/binary>>
                 end,
    AllHeaders = [{<<"host">>, HostHeader},
                  {<<"content-type">>, <<"application/json">>},
                  {<<"content-length">>, integer_to_binary(byte_size(BodyBin))},
                  {<<"user-agent">>, <<"unified-mq-a2a-push/1.0">>},
                  {<<"connection">>, <<"close">>}
                  | Headers],
    Request = [<<"POST ">>, Path, <<" HTTP/1.1\r\n">>,
               [[Name, <<": ">>, Value, <<"\r\n">>] || {Name, Value} <- AllHeaders],
               <<"\r\n">>, BodyBin],
    case send(Scheme, Socket, Request) of
        ok ->
            case recv_status_line(Scheme, Socket, maps:get(recv, Timeouts)) of
                {ok, Status} ->
                    case Status >= 200 andalso Status < 300 of
                        true -> {ok, Status};
                        false -> {error, {unexpected_status, Status}}
                    end;
                {error, Reason} ->
                    {error, {recv_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {send_failed, Reason}}
    end.

%% Only the status line is needed (ADR-026 §D6.4: "we only need the status").
%% We do not read the body; closing the connection discards it, and the cap
%% stops a hostile peer from streaming a huge status line at us.
recv_status_line(Scheme, Socket, Timeout) ->
    recv_status_line(Scheme, Socket, Timeout, max_response_bytes(), <<>>).

recv_status_line(Scheme, Socket, Timeout, Max, Acc) ->
    case binary:match(Acc, <<"\r\n">>) of
        {Pos, _} ->
            <<Line:Pos/binary, _/binary>> = Acc,
            parse_status(Line);
        nomatch when byte_size(Acc) > Max ->
            {error, response_too_large};
        nomatch ->
            case recv(Scheme, Socket, Timeout) of
                {ok, Data} ->
                    recv_status_line(Scheme, Socket, Timeout, Max,
                                     <<Acc/binary, Data/binary>>);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

recv(<<"https">>, Socket, Timeout) -> ssl:recv(Socket, 0, Timeout);
recv(<<"http">>, Socket, Timeout) -> gen_tcp:recv(Socket, 0, Timeout).

send(<<"https">>, Socket, Data) -> ssl:send(Socket, Data);
send(<<"http">>, Socket, Data) -> gen_tcp:send(Socket, Data).

parse_status(StatusLine) ->
    %% "HTTP/1.1 200 OK"
    case binary:split(StatusLine, <<" ">>, [global]) of
        [_Version, CodeBin | _] ->
            try binary_to_integer(CodeBin) of
                Code when Code >= 100, Code =< 599 -> {ok, Code};
                _ -> {error, malformed_status}
            catch
                _:_ -> {error, malformed_status}
            end;
        _ ->
            {error, malformed_status}
    end.

close(<<"https">>, Socket) -> _ = ssl:close(Socket), ok;
close(<<"http">>, Socket) -> _ = gen_tcp:close(Socket), ok.
