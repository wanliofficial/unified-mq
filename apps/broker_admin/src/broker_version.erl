%% @doc Release / data-format version reporting (S6, roadmap §8.2).
%%
%% A rolling upgrade needs two facts an operator can query before flipping a
%% node: which release this process runs, and which on-disk/protocol versions
%% it can read. Everything here is derived from real data -- the loaded
%% applications' `vsn' and the codec's own `current_vsn/0' -- never from a
%% hard-coded marketing string. Nothing in this module mutates state.
-module(broker_version).

-export([info/0, release/0, data_version/0, compatible/1]).

%% The versions this build can read on disk. A future build that raises the
%% WAL format broadens this window; until then there is exactly one version.
-define(DATA_VERSIONS, [1]).

%% @doc Everything the admin API and the upgrade runbook need.
-spec info() -> map().
info() ->
    #{release => release(),
      otp => otp_version(),
      apps => apps(),
      data_version => data_version(),
      compatible_data_versions => ?DATA_VERSIONS,
      config_version => config_version()}.

%% @doc The release version from the umbrella's `.app' (falls back to the
%% running `unified_mq' release, then `unknown').
-spec release() -> binary().
release() ->
    case application:get_key(broker_core, vsn) of
        {ok, Vsn} -> to_bin(Vsn);
        undefined -> <<"unknown">>
    end.

%% @doc The WAL record format version new writes carry.
-spec data_version() -> non_neg_integer().
data_version() ->
    try wal_codec:current_vsn() of
        V -> V
    catch
        _:_ -> 0
    end.

%% @doc Can this node read data written by `Version'? A rolling upgrade is only
%% safe when every node answers true for the range it may meet on disk.
-spec compatible(non_neg_integer()) -> boolean().
compatible(Version) when is_integer(Version) ->
    lists:member(Version, ?DATA_VERSIONS);
compatible(_) ->
    false.

%%%_* Internal --------------------------------------------------------------

apps() ->
    lists:sort(
      [{Name, to_bin(Vsn)}
       || {Name, _Desc, Vsn} <- application:loaded_applications(),
          is_broker_app(Name)]).

is_broker_app(Name) ->
    lists:prefix("broker_", atom_to_list(Name)).

otp_version() ->
    to_bin(erlang:system_info(otp_release)).

config_version() ->
    try broker_meta:version() of
        V -> V
    catch
        _:_ -> 0
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).
