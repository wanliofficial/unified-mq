%% S1's test network layer for the MQTT connection suites (design §9 T9): a real
%% TCP pair, the real `mqtt_conn_fsm' child, a blocking frame reader. No
%% listener -- that is T12's ranch -- and no fixed port numbers, so suites can
%% run in parallel and nothing here can collide with another node's config.
%%
%% The socket-controlling-process dance is the reason this file exists: the FSM
%% may not touch its socket until `controlling_process' has completed, so the
%% handoff order (start child -> transfer ownership -> `! connect') is encoded
%% once, here, and is the same order T12's `on_accept' will use.
%%
%% Include it in ONE module per node: it defines functions (no -export), and the
%% per-socket read buffer lives in the including process's dictionary. Every CT
%% testcase runs in its own process, so buffers cannot leak between cases.
%%
%% `Conn' is the map #{sock => ClientSocket, pid => FsmPid}.

%%%_* Transport ---------------------------------------------------------------

mqtt_pair() -> mqtt_pair(#{}).

mqtt_pair(Opts) ->
    {ok, Ls} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true},
                                  {nodelay, true}]),
    {ok, Port} = inet:port(Ls),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                   [binary, {active, false}, {nodelay, true}]),
    {ok, Server} = gen_tcp:accept(Ls),
    ok = gen_tcp:close(Ls),
    {ok, Pid} = mqtt_conn_sup:start_conn(Server, Opts),
    ok = gen_tcp:controlling_process(Server, Pid),
    Pid ! connect,
    {Client, Pid}.

%% CONNECT + CONNACK in one step, the way a client does it. Knobs:
%%   client_id (required), clean (default true), keep_alive (default 0),
%%   username/password, will_topic/will_payload.
mqtt_open(Knobs) ->
    {Client, Pid} = mqtt_pair(),
    Frame = mqtt_connect_frame(Knobs),
    ok = gen_tcp:send(Client, mqtt_frame:encode(Frame)),
    Connack = mqtt_recv(Client),
    {#{sock => Client, pid => Pid}, Connack}.

%% S2.5: the same open dance with level 5 and CONNECT properties.
mqtt_open_v5(Knobs) ->
    {Client, Pid} = mqtt_pair(),
    Frame = mqtt_connect_frame_v5(Knobs),
    ok = gen_tcp:send(Client, mqtt_frame:encode(Frame, 5)),
    Connack = mqtt_recv_v5(Client),
    {#{sock => Client, pid => Pid}, Connack}.

mqtt_connect_frame_v5(Knobs) ->
    (mqtt_connect_frame(Knobs))#connect{
      proto_level = 5, props = maps:get(props, Knobs, #{})}.

mqtt_send_v5(#{sock := Client}, Frame) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(Frame, 5)).

mqtt_subscribe_v5(Conn, PacketId, Topics, Props) ->
    ok = mqtt_send_v5(Conn, #subscribe{packet_id = PacketId, topics = Topics,
                                       props = Props}),
    mqtt_recv_v5(csock(Conn)).

mqtt_publish_v5(Conn, PacketId, Topic, Payload, QoS, Props) ->
    ok = mqtt_send_v5(Conn, #publish{qos = QoS, topic = Topic,
                                     packet_id = PacketId, payload = Payload,
                                     props = Props}).

mqtt_puback_v5(Conn, PacketId) ->
    ok = mqtt_send_v5(Conn, #puback{packet_id = PacketId}).

mqtt_connect_frame(Knobs) ->
    ClientId = maps:get(client_id, Knobs),
    WillTopic = maps:get(will_topic, Knobs, undefined),
    #connect{client_id = ClientId,
             clean_session = case maps:get(clean, Knobs, true) of
                                 true -> 1; false -> 0
                             end,
             keep_alive = maps:get(keep_alive, Knobs, 0),
             username = maps:get(username, Knobs, undefined),
             password = maps:get(password, Knobs, undefined),
             will_flag = case WillTopic of undefined -> 0; _ -> 1 end,
             will_qos = maps:get(will_qos, Knobs, 0),
             will_retain = maps:get(will_retain, Knobs, 0),
             will_topic = WillTopic,
             will_payload = maps:get(will_payload, Knobs, undefined)}.

mqtt_send(#{sock := Client}, Bin) -> ok = gen_tcp:send(Client, Bin).

mqtt_monitor(#{pid := Pid}) -> erlang:monitor(process, Pid).

mqtt_expect_down(Monitor, Pid) ->
    receive
        {'DOWN', Monitor, process, Pid, Reason} -> {ok, Reason}
    after 5000 -> exit({mqtt_no_down, Pid}) end.

%% A refused connection is usually dead before the client has even read the
%% CONNACK, so a monitor started afterwards answers `noproc' rather than
%% `normal' -- the same fact seen from the two sides of a race. Normalising it
%% here keeps the suites from asserting `normal orelse noproc' 15 times, and a
%% conn that died of an actual fault still reports its own tuple reason.
mqtt_close(#{sock := Client, pid := Pid}) ->
    Monitor = erlang:monitor(process, Pid),
    ok = gen_tcp:close(Client),
    case mqtt_expect_down(Monitor, Pid) of
        {ok, noproc} -> {ok, normal};
        Down -> Down
    end.

%%%_* Protocol actions --------------------------------------------------------

mqtt_subscribe(#{sock := Client}, PacketId, Topics) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(
                                #subscribe{packet_id = PacketId, topics = Topics})),
    mqtt_recv(Client).

mqtt_unsubscribe(#{sock := Client}, PacketId, Filters) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(
                                #unsubscribe{packet_id = PacketId, topics = Filters})),
    mqtt_recv(Client).

mqtt_publish(#{sock := Client}, PacketId, Topic, Payload, QoS) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(
                                #publish{qos = QoS, topic = Topic,
                                         packet_id = PacketId,
                                         payload = Payload})).

mqtt_puback(#{sock := Client}, PacketId) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(#puback{packet_id = PacketId})).

%%%_* QoS 2 handshake (ADR-012 / ADR-018 §18.3) --------------------------------

mqtt_pubrec(#{sock := Client}, PacketId) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(#pubrec{packet_id = PacketId})).

mqtt_pubrel(#{sock := Client}, PacketId) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(#pubrel{packet_id = PacketId})).

mqtt_pubcomp(#{sock := Client}, PacketId) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(#pubcomp{packet_id = PacketId})).

mqtt_ping(#{sock := Client}) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(#ping{kind = req})),
    mqtt_recv(Client).

mqtt_disconnect(#{sock := Client}) ->
    ok = gen_tcp:send(Client, mqtt_frame:encode(#disconnect{})).

%%%_* Reading frames ----------------------------------------------------------

mqtt_recv(Client) -> mqtt_recv(Client, 5000).

mqtt_recv(Client, Timeout) ->
    mqtt_recv_vsn(Client, Timeout, auto).

%% S2.5: a v5 peer's CONNACK/PUBLISH/SUBACK carry properties, so the reader has
%% to decode with the negotiated version. `auto' (the default) is the 3.1.1
%% path every existing case uses.
mqtt_recv_v5(Client) ->
    mqtt_recv_vsn(Client, 5000, 5).

mqtt_recv_vsn(Client, Timeout, Vsn) ->
    case mqtt_recv_or(Client, Timeout, timeout, Vsn) of
        timeout -> exit({mqtt_recv_timeout, Timeout, mqtt_buf(Client)});
        {error, closed} -> exit({mqtt_recv_closed, mqtt_buf(Client)});
        Frame -> Frame
    end.

%% Accumulate-and-drain, the streaming contract `decode/1' promises: whatever is
%% already buffered is tried first, and the socket is only asked when the buffer
%% holds a prefix. Bytes of a second frame stay in the buffer for the next call.
mqtt_recv_or(Client, Timeout, Default) ->
    mqtt_recv_or(Client, Timeout, Default, auto).

mqtt_recv_or(Client, Timeout, Default, Vsn) ->
    Buf = mqtt_buf(Client),
    case (catch mqtt_frame:decode(Buf, Vsn)) of
        {ok, Frame, Rest} ->
            mqtt_set_buf(Client, Rest),
            Frame;
        {more, _} ->
            case gen_tcp:recv(Client, 0, Timeout) of
                {ok, Data} ->
                    mqtt_set_buf(Client, <<Buf/binary, Data/binary>>),
                    mqtt_recv_or(Client, Timeout, Default, Vsn);
                {error, timeout} ->
                    Default;
                {error, closed} ->
                    {error, closed}
            end;
        {error, Reason} ->
            exit({mqtt_recv_malformed, Reason, Buf});
        {'EXIT', Reason} ->
            exit({mqtt_recv_crashed, Reason, Buf})
    end.

%% Nothing arrives and the stream stays open for Ms milliseconds.
mqtt_silence(Client, Ms) ->
    case mqtt_recv_or(Client, Ms, silent) of
        silent -> true;
        Other -> exit({mqtt_expected_silence, Other})
    end.

mqtt_buf(Client) ->
    case erlang:get({mqtt_buf, Client}) of
        undefined -> <<>>;
        Buf -> Buf
    end.

mqtt_set_buf(Client, Buf) ->
    erlang:put({mqtt_buf, Client}, Buf),
    ok.

%%%_* Raw bytes (for the frames encode/1 refuses to build) ---------------------

%% A CONNECT whose protocol name is "MQTT" but whose level is not 4 -- §4.5's
%% version posture is exactly about these, and encode/1 only speaks 3.1.1.
mqtt_raw_connect(Level, ClientId) ->
    %% Flags: clean session only. The body carries no Will / username / password
    %% section, so those bits have to stay 0 for the frame to be well-formed --
    %% and the codec reads the level byte before it validates any of this.
    Body = <<4:16, "MQTT", Level, 2#00000010, 0:16,
             (byte_size(ClientId)):16, ClientId/binary>>,
    <<16#10, (byte_size(Body)), Body/binary>>.

%% A PUBLISH whose QoS bits say 3: still reserved, still refused (§3.3.1.2).
mqtt_raw_publish_qos3(Topic, Payload) ->
    Body = <<(byte_size(Topic)):16, Topic/binary, Payload/binary>>,
    %% type 3 with flags 0b0110 (dup=0, qos=3, retain=0)
    <<2#00110110, (byte_size(Body)), Body/binary>>.
