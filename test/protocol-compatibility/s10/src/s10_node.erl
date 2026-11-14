%% S10-D evidence node: boots the full broker kernel with the WebRTC signaling
%% front end (WSS on an ephemeral loopback port) and the ICE helper (STUN/TURN
%% on an ephemeral UDP port) both turned ON.
%%
%% This is the node the real third-party client (Python `aiortc', driven by
%% `webrtc_evidence.py') talks to for D.1, and the node the "no persistence"
%% runtime proof (`s10_no_persist_driver') drives for D.2. The aiortc
%% `RTCPeerConnection' is a full, independent WebRTC implementation, so two of
%% them completing a real offer/answer/candidate exchange through our signaling
%% relay, plus a real STUN Binding and a real TURN relay allocation, is the
%% strongest "it actually works" evidence S10-D can produce short of a browser.
%%
%% `boot/0' is exported separately from `run/0' so the D.2 driver can take a
%% snapshot in the *same* VM as the kernel and the listeners. `run/0' is the
%% `erl -s s10_run main' entry point: boot, print the readiness line, block.
%%
%% Like `s9f_node', the data directory is outside the repo (tmpfs by default;
%% override with S10_DATA). Ports are ephemeral (ranch `port => 0',
%% `broker_ice' `port => 0') so the run never collides with the fixed
%% 8080/8083/1883/8081/8082/8084 or the relay range the CT suite uses.
-module(s10_node).
-export([run/0, boot/0, ice_ip/0]).

run() ->
    #{wss_port := WssPort, stun_port := StunPort, ice_ip := IceIp} = boot(),
    io:format("S10-NODE-READY wss=~p stun=~p ice=~s room=e2e~n",
              [WssPort, StunPort, inet:ntoa(IceIp)]),
    receive after infinity -> ok end.

%% @doc Boot the kernel plus both listeners; returns the facts a driver needs.
%% Idempotent only in the sense that it must run once per VM -- it wipes the
%% data dir first, exactly like a fresh node.
-spec boot() -> #{wss_port := inet:port_number(),
                 stun_port := inet:port_number(),
                 ice_ip := inet:ip4_address(),
                 data_dir := file:filename_all()}.
boot() ->
    DataDir = os:getenv("S10_DATA", "/tmp/opencode/s10-evidence/data"),
    _ = os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    [application:load(A) || A <- [broker_storage, broker_router, broker_queue,
                                  broker_meta, broker_security, broker_event,
                                  broker_subscription, broker_retain,
                                  broker_metrics, broker_ws,
                                  broker_proto_webrtc, broker_ice]],
    application:set_env(broker_storage, backend, broker_storage_wal),
    application:set_env(broker_storage, data_dir, DataDir),
    application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    application:set_env(broker_router, partitions, 2),
    application:set_env(broker_queue, partitions, 2),
    application:set_env(broker_meta, table_storage, ram_copies),
    application:set_env(broker_meta, nodes, []),
    application:set_env(broker_metrics, enabled, true),
    %% `broker_subscription''s data dir has its own env key (layers read their
    %% own config, J8). It must point at the same node dir, otherwise the D.2
    %% cursor snapshot would look in `./data' and always see nothing.
    application:set_env(broker_subscription, data_dir, DataDir),
    %% One credential for both the signaling authenticate and the TURN
    %% long-term credential (R-I3: same `users' table). Loopback listener, so
    %% `require_auth' stays off (ADR-028 §D5) and no fixed port is used.
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>,
                               #{password => <<"alicepw">>,
                                 tenant_id => <<"t1">>}}]),
    application:set_env(broker_security, acl, allow),
    application:set_env(broker_security, require_auth, []),
    {ok, _} = application:ensure_all_started(broker_security),
    broker_meta:reset(),
    {ok, _} = application:ensure_all_started(broker_meta),
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_router),
    {ok, _} = application:ensure_all_started(broker_queue),
    %% D.2 needs the subscription kernel *live*: its 100 ms cursor tick is the
    %% only writer that could create a `.cursors' file. A node that never
    %% started it would make "no cursor file" a tautology.
    {ok, _} = application:ensure_all_started(broker_subscription),
    _ = application:load(broker_ws),
    application:set_env(broker_ws, idle_timeout_ms, 60000),
    application:set_env(broker_ws, max_frame_bytes, 262144),
    {ok, _} = application:ensure_all_started(broker_ws),

    %% --- Signaling front end (WSS on an ephemeral loopback port) ---
    CertDir = os:getenv("S10_CERTDIR", "/tmp/opencode/s10-evidence/certs"),
    ok = filelib:ensure_dir(filename:join([CertDir, "x"])),
    CertFile = filename:join(CertDir, "s10-cert.pem"),
    KeyFile = filename:join(CertDir, "s10-key.pem"),
    case {filelib:is_regular(CertFile), filelib:is_regular(KeyFile)} of
        {true, true} -> ok;
        _ ->
            _ = os:cmd("openssl req -x509 -newkey rsa:2048 -nodes "
                       "-keyout '" ++ KeyFile ++ "' -out '" ++ CertFile ++ "' "
                       "-days 1 -subj '/CN=127.0.0.1' >/dev/null 2>&1")
    end,
    application:set_env(broker_security, tls,
                        #{enabled => true,
                          certfile => CertFile,
                          keyfile => KeyFile,
                          verify => verify_none,
                          fail_if_no_peer_cert => false,
                          versions => ['tlsv1.2', 'tlsv1.3']}),
    _ = application:load(broker_proto_webrtc),
    application:set_env(broker_proto_webrtc, listeners,
                        [#{port => 0, ref => {webrtc_evidence, 0},
                           num_acceptors => 4, max_connections => 64}]),
    application:set_env(broker_proto_webrtc, subprotocols,
                        [<<"webrtc.v1">>]),
    application:set_env(broker_proto_webrtc, require_auth, false),
    application:set_env(broker_proto_webrtc, auth_timeout_ms, 5000),
    application:set_env(broker_proto_webrtc, idle_timeout_ms, 60000),
    application:set_env(broker_proto_webrtc, max_frame_bytes, 262144),
    {ok, _} = application:ensure_all_started(broker_proto_webrtc),
    WssPort = ranch:get_port({webrtc_evidence, 0}),

    %% --- ICE helper (STUN Binding + TURN over UDP, ephemeral port) ---
    %% Narrow relay pool so the run does not grab a wide range: 20 ports is
    %% far more than this single-allocation evidence needs.
    _ = application:load(broker_ice),
    application:set_env(broker_ice, enabled, true),
    application:set_env(broker_ice, port, 0),
    %% The relay address handed back in XOR-RELAYED-ADDRESS. It must be an
    %% address a real ICE agent can actually send *to*: on a loopback-only
    %% evidence node the aiortc client (which binds its UDP sockets to the
    %% host's real interface IPs, then sends to this address) would hit a
    %% sandbox `EPERM' on a loopback destination. So we advertise the node's
    %% primary non-loopback IPv4, discovered at boot, overridable with
    %% S10_ICE_IP. Binding is still 0.0.0.0 (see broker_ice_udp init); only the
    %% *advertised* relay address changes.
    IceIp = ice_ip(),
    application:set_env(broker_ice, turn_ip, IceIp),
    application:set_env(broker_ice, realm, <<"unified-mq">>),
    application:set_env(broker_ice, nonce_secret, <<"s10-evidence-nonce-secret">>),
    application:set_env(broker_ice, relay_port_range, {49500, 49519}),
    application:set_env(broker_ice, max_allocations, 16),
    application:set_env(broker_ice, allocation_lifetime_s, 600),
    StunPort =
        case application:ensure_all_started(broker_ice) of
            {ok, _} ->
                broker_ice_udp:port();
            {error, {App, Reason}} ->
                io:format("S10-START-ERROR ~p ~p~n", [App, Reason]),
                halt(1)
        end,
    #{wss_port => WssPort, stun_port => StunPort, ice_ip => IceIp,
      data_dir => DataDir}.

%% @doc The IPv4 address advertised in XOR-RELAYED-ADDRESS (and printed so the
%% aiortc driver can point its `stun:'/`turn:' server here). A real ICE agent
%% binds its UDP sockets to the host's interface IPs and sends to this
%% address; a loopback address would be an `EPERM' cross-scope send under the
%% evidence sandbox. `S10_ICE_IP' overrides; otherwise the primary non-loopback
%% IPv4 is used; falling back to loopback if none is found (the raw-UDP STUN
%% evidence still works loopback, only the aiortc path needs the routable IP).
ice_ip() ->
    case os:getenv("S10_ICE_IP") of
        false -> primary_ipv4();
        "" -> primary_ipv4();
        S -> parse_ip(S)
    end.

primary_ipv4() ->
    case inet:getifaddrs() of
        {ok, IfList} ->
            Candidates =
                [IP || {_Name, Opts} <- IfList,
                        {addr, IP} <- Opts,
                        tuple_size(IP) =:= 4,
                        IP =/= {127, 0, 0, 1}],
            case Candidates of
                [IP | _] -> IP;
                [] -> {127, 0, 0, 1}
            end;
        _ ->
            {127, 0, 0, 1}
    end.

parse_ip(S) ->
    case inet:parse_ipv4_address(S) of
        {ok, IP} -> IP;
        _ -> {127, 0, 0, 1}
    end.
