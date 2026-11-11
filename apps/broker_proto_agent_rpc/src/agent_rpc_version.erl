%% @doc `A2A-Version' negotiation.
%%
%% A2A 1.0.0 transmits the protocol version as a service parameter, i.e. an HTTP
%% request header (spec §9.2); a missing or empty header means `0.3' (the
%% pre-1.0 reading the spec keeps for compatibility). Only the header form is
%% implemented -- the query-parameter form the spec also allows is listed as
%% "not implemented" in the capability matrix (convergence report §13).
-module(agent_rpc_version).

-export([parse/1, negotiate/2, to_binary/1]).
-export_type([version/0]).

-type version() :: {non_neg_integer(), non_neg_integer()}.

%% @doc `Major.Minor' only. `"1"' and `"1.0.0"' are rejected rather than
%% best-effort parsed: a client that sends either is speaking something other
%% than the documented shape, and guessing hides that.
-spec parse(binary() | undefined | term()) -> {ok, version()} | {error, version_not_supported}.
parse(undefined) ->
    {ok, {0, 3}};
parse(<<>>) ->
    {ok, {0, 3}};
parse(Bin) when is_binary(Bin) ->
    case binary:split(trim(Bin), <<".">>) of
        [Maj, Min] ->
            case {digits(Maj), digits(Min)} of
                {{ok, M}, {ok, N}} -> {ok, {M, N}};
                _ -> {error, version_not_supported}
            end;
        _ ->
            {error, version_not_supported}
    end;
parse(_Other) ->
    {error, version_not_supported}.

%% @doc `ok' only when the perceived version is in the adapter's supported list.
%% The list is configured as binaries (`[<<"1.0">>]') so `sys.config' needs no
%% Erlang tuple syntax; an unparsable entry is skipped rather than crashing the
%% request path.
-spec negotiate(version(), [binary()]) -> ok | {error, version_not_supported}.
negotiate(Version, Supported) when is_list(Supported) ->
    Parsed = [V || Bin <- Supported, {ok, V} <- [parse(Bin)]],
    case lists:member(Version, Parsed) of
        true -> ok;
        false -> {error, version_not_supported}
    end.

-spec to_binary(version()) -> binary().
to_binary({Major, Minor}) ->
    <<(integer_to_binary(Major))/binary, ".", (integer_to_binary(Minor))/binary>>.

%%%_* Internal --------------------------------------------------------------

digits(<<>>) ->
    error;
digits(Bin) ->
    try binary_to_integer(Bin) of
        N when N >= 0 -> {ok, N};
        _ -> error
    catch
        error:badarg -> error
    end.

trim(Bin) ->
    string:trim(Bin, both, " \t").
