%% @doc Boot a UnifiedMQ node whose only listener is CoAP over DTLS (PSK), on
%% an ephemeral UDP port, print the real port and the derived PSK, then block
%% until killed.
%%
%% `libcoap_compat.py' launches this via `erl -pa <app ebins> -noshell -s', reads
%% the `PORT' and `KEY' lines, drives libcoap's real `coap-client' binary against
%% it, then terminates the node.
%%
%% This mirrors `coap_dtls_SUITE init_per_testcase' -- same env, same
%% `port => 0' ephemeral binding, same `coap_listener:port/1' readback -- so the
%% third-party run exercises the identical boot path the CT suite does. Only
%% `broker_proto_coap' starts: there is no MQTT/WS/HTTP listener, so a passing
%% case cannot have accidentally gone over another protocol.
%%
%% The PSK is not configured: it is derived from the password exactly as a real
%% device would derive it, and only the derivation output is printed. That keeps
%% the harness free of a second source of truth for the key.
-module(coap_dtls_broker_boot).
-export([run/0]).

-define(USER, <<"lc-dev">>).
-define(PASSWORD, <<"lc-pw">>).
-define(REALM, <<"unifiedmq">>).

run() ->
    DataDir = os:getenv("BROKER_DATA_DIR", default_data_dir()),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    lists:foreach(fun(A) -> _ = application:load(A) end,
                  [broker_storage, broker_router, broker_queue,
                   broker_subscription, broker_retain, broker_event,
                   broker_security, broker_proto_coap]),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, 2),
    ok = application:set_env(broker_router, partitions, 2),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_event, policy, pass_through),
    %% One device whose PSK is derived from the password; the empty ACL means
    %% the only thing standing between the wire and the resource is the
    %% handshake, which is exactly what this harness is measuring.
    ok = application:set_env(broker_security, users,
                             [{?USER, #{password => ?PASSWORD,
                                        tenant_id => <<"lc">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_security, coaps, #{realm => ?REALM}),
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners,
                             [#{port => 0, transport => dtls}]),
    ok = application:set_env(broker_proto_coap, resources, []),
    %% Storage first (its WAL wants the data dir), then partitions, then CoAP:
    %% the queue workers want pre-created partitions, and the listener wants the
    %% kernel up.
    {ok, _} = application:ensure_all_started(broker_storage),
    create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    Port = listener_port(),
    {ok, Psk} = broker_security:psk_for_identity(?USER),
    ok = io:format("PORT ~p~n", [Port]),
    ok = io:format("USER ~s~n", [?USER]),
    ok = io:format("KEY ~s~n", [string:lowercase(binary_to_list(
                                             binary:encode_hex(Psk)))]),
    ok = io:format("READY~n", []),
    block_forever().

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> halt({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

%% The single DTLS listener this node starts.
listener_port() ->
    [Pid | _] = [P || {_, P, _, _} <- supervisor:which_children(
                                    broker_proto_coap_sup), is_pid(P)],
    {ok, Port} = coap_listener:port(Pid),
    Port.

default_data_dir() ->
    filename:join(["/tmp", "unifiedmq-coap-dtls-compat"]).

block_forever() ->
    receive
        stop -> ok
    after infinity ->
            ok
    end.
