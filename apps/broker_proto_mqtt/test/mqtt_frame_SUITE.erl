%% @doc T8: wire-codec tests. The golden vectors are the packet dumps of
%% MQTT 3.1.1 §2.1 verbatim (the spec dumps stand in for a mosquitto/tcpdump
%% capture, which this box does not have — design §13 L-register), plus the
%% §4.5 strict-rejection posture and six proper properties (roundtrip, stream
%% splitting, never-crash, varint bounds, encode-raising, single-bit-flip).
-module(mqtt_frame_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").
-include("mqtt_frame.hrl").

-define(NUMTESTS, 200).

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(proper),
    Config.

end_per_suite(_Config) ->
    %% J10 rule: exported because init_per_suite is; must exist and must
    %% actually clean up (ok, not _ =, or a crash leaks the app into later
    %% suites on the same node).
    ok = application:stop(proper),
    ok.

all() ->
    [golden_vectors_decode,
     golden_vectors_encode,
     direct_builds_match_records,
     streaming_decode_loop,
     non_minimal_varint_accepted,
     publish_qos0_no_pid,
     puback_zero_pid_rejected,
     subscribe_qos_byte_bounds,
     connect_version_rejections,
     connect_will_without_flag_rejected,
     connect_will_qos_above_one_rejected,
     connect_reserved_bit_rejected,
     connect_username_without_password_rejected,
     utf8_rejections,
     truncated_frames_never_produce_ok,
     fixed_header_flag_rejections,
     filter_shape_and_matching,
     p1_roundtrip,
     p2_stream_splitting,
     p3_never_crashes,
     p4_varint_roundtrip,
     p5_encode_rejects_illegal,
     p6_single_bit_flip].

%%%_* Golden vectors (MQTT 3.1.1 §2.1) ---------------------------------------

golden_vectors_decode(_Config) ->
    V = [{<<"0x10 0x0E 00 04 4D 51 54 54 04 02 00 00 00 02 6D 6F">>,
          #connect{client_id = <<"mo">>, keep_alive = 0, clean_session = 1}},
         {<<"0xE0 0x00">>, #disconnect{}},
         {<<"0xD0 0x00">>, #ping{kind = resp}},
         {<<"0x20 0x02 00 00">>, #connack{session_present = 0, return_code = 0}},
         {<<"0x20 0x02 00 01">>, #connack{session_present = 0, return_code = 1}},
         {<<"0x20 0x02 01 00">>, #connack{session_present = 1, return_code = 0}},
         {<<"0x30 0x09 00 06 2F 61 2F 62 2F 63 00">>,
          #publish{qos = 0, topic = <<"/a/b/c">>, payload = <<0>>}},
         {<<"0x3A 0x15 00 05 61 2F 62 2F 63 00 0A 68 65 6C 6C 6F 20 77 6F 72 6C 64 29">>,
          #publish{dup = 1, qos = 1, topic = <<"a/b/c">>, packet_id = 10,
                   payload = <<"hello world)">>}},
         {<<"0x82 0x19 00 01 00 03 61 2F 23 01 00 0E 73 70 6F 72 74 2F 74 65 6E 6E 69 73 2F 23 02">>,
          #subscribe{packet_id = 1,
                     topics = [{<<"a/#">>, 1}, {<<"sport/tennis/#">>, 2}]}}],
    vectors(V).

golden_vectors_encode(_Config) ->
    V = [{<<"0x10 0x0E 00 04 4D 51 54 54 04 02 00 00 00 02 6D 6F">>,
          #connect{client_id = <<"mo">>, keep_alive = 0, clean_session = 1}},
         {<<"0xE0 0x00">>, #disconnect{}},
         {<<"0x20 0x02 01 00">>, #connack{session_present = 1, return_code = 0}},
         {<<"0x30 0x09 00 06 2F 61 2F 62 2F 63 00">>,
          #publish{qos = 0, topic = <<"/a/b/c">>, payload = <<0>>}},
         {<<"0x3A 0x15 00 05 61 2F 62 2F 63 00 0A 68 65 6C 6C 6F 20 77 6F 72 6C 64 29">>,
          #publish{dup = 1, qos = 1, topic = <<"a/b/c">>, packet_id = 10,
                   payload = <<"hello world)">>}},
         {<<"0x40 0x02 00 0A">>, #puback{packet_id = 10}},
         {<<"0x90 0x03 00 01 00">>, #suback{packet_id = 1, return_codes = [0]}},
         {<<"0xA2 0x0C 00 01 00 03 61 2F 23 00 03 74 2F 31">>,
          #unsubscribe{packet_id = 1, topics = [<<"a/#">>, <<"t/1">>]}},
         {<<"0xB0 0x02 00 05">>, #unsuback{packet_id = 5}},
         {<<"0xC0 0x00">>, #ping{kind = req}},
         {<<"0xD0 0x00">>, #ping{kind = resp}}],
    lists:foreach(
      fun({Hex, Frame}) ->
              ?assertEqual(unhex(Hex), mqtt_frame:encode(Frame))
      end, V),
    ok.

direct_builds_match_records(_Config) ->
    ?assertEqual(mqtt_frame:encode(#puback{packet_id = 10}),
                  mqtt_frame:puback(any, 10)),
    ?assertEqual(mqtt_frame:encode(#connack{session_present = 1,
                                             return_code = 0}),
                  mqtt_frame:connack(1, 0)),
    ?assertEqual(mqtt_frame:encode(#suback{packet_id = 1,
                                            return_codes = [0, 1, 128]}),
                  mqtt_frame:suback(1, [0, 1, 128])),
    ?assertEqual(mqtt_frame:encode(#unsuback{packet_id = 5}),
                  mqtt_frame:unsuback(5)),
    ?assertEqual(<<16#D0, 0>>, mqtt_frame:pingresp()),
    ?assertEqual(mqtt_frame:pingresp(), mqtt_frame:encode(#ping{kind = resp})),
    ok.

vectors(V) ->
    lists:foreach(
      fun({Hex, Expected}) ->
              Bin = unhex(Hex),
              case mqtt_frame:decode(Bin) of
                  {ok, Got, <<>>} -> ?assertEqual(Expected, Got);
                  Other -> ct:fail({decode_failed, Hex, Other})
              end
      end, V),
    ok.

%%%_* Behavior cases ---------------------------------------------------------

streaming_decode_loop(_Config) ->
    %% Three frames glued into one stream: decode must walk them one at a
    %% time, each round returning the untouched remainder.
    Q0 = <<16#30, 3, 0, 1, "x">>,
    Ping = <<16#C0, 0>>,
    Sub = <<16#82, 7, 0, 7, 0, 2, "t/", 1>>,
    {ok, F1, R1} = mqtt_frame:decode(<<Q0/binary, Ping/binary, Sub/binary>>),
    ?assertEqual(#publish{qos = 0, topic = <<"x">>, payload = <<>>}, F1),
    {ok, F2, R2} = mqtt_frame:decode(R1),
    ?assertEqual(#ping{kind = req}, F2),
    {ok, F3, <<>>} = mqtt_frame:decode(R2),
    ?assertEqual(#subscribe{packet_id = 7, topics = [{<<"t/">>, 1}]}, F3),
    ok.

non_minimal_varint_accepted(_Config) ->
    %% §2.2.3's own algorithm parses <<128,0>> as length 0; minimality is
    %% not spec text, so this PINGRESP decodes.
    {ok, #ping{kind = resp}, <<>>} = mqtt_frame:decode(<<16#D0, 128, 0>>),
    ok.

publish_qos0_no_pid(_Config) ->
    %% QoS 0 must not carry a packet id: the bytes after the topic belong
    %% to the payload itself.
    Bin = mqtt_frame:encode(#publish{qos = 0, topic = <<"a">>,
                                     payload = <<0, 1, 2>>}),
    {ok, #publish{qos = 0, packet_id = undefined, payload = <<0, 1, 2>>},
     <<>>} = mqtt_frame:decode(Bin),
    ok.

puback_zero_pid_rejected(_Config) ->
    {error, _} = mqtt_frame:decode(<<16#40, 2, 0, 0>>),
    ok.

subscribe_qos_byte_bounds(_Config) ->
    %% Requested-QoS byte: only 0..2 valid, top six bits must be zero.
    Ok = fun(Byte) ->
             mqtt_frame:decode(<<16#82, 7, 0, 7, 0, 2, "t/", Byte>>)
         end,
    {ok, #subscribe{topics = [{<<"t/">>, 0}]}, <<>>} = Ok(0),
    {ok, #subscribe{topics = [{<<"t/">>, 2}]}, <<>>} = Ok(2),
    {error, _} = Ok(3),
    {error, _} = Ok(2#01000000),
    ok.

connect_version_rejections(_Config) ->
    %% The §4.5 posture feeds off these distinct reasons: 5 -> 0x87, other
    %% levels -> 0x01 (the conn maps them; the codec tells them apart).
    C = fun(Level, Name) ->
            Body = <<(byte_size(Name)):16, Name/binary, Level, 2, 0, 60,
                    0, 2, "mo">>,
            mqtt_frame:decode(<<16#10, (byte_size(Body)), Body/binary>>)
        end,
    {error, {unacceptable_protocol_version, 5}} = C(5, <<"MQTT">>),
    {error, {unsupported_protocol_version, 3}} = C(3, <<"MQTT">>),
    {error, {unsupported_protocol_version, 6}} = C(6, <<"MQTT">>),
    {error, {unsupported_protocol, <<"MQT">>}} = C(4, <<"MQT">>),
    {ok, #connect{proto_level = 4}, <<>>} = C(4, <<"MQTT">>),
    ok.

connect_will_without_flag_rejected(_Config) ->
    %% §3.1.2.4: Will QoS/Retain must be 0 when WillFlag is 0.
    Body = <<0, 4, "MQTT", 4, 2#00100000, 0, 60, 0, 2, "mo">>,
    {error, {malformed_packet, will_flags_without_will}} =
        mqtt_frame:decode(<<16#10, (byte_size(Body)), Body/binary>>),
    ok.

connect_will_qos_above_one_rejected(_Config) ->
    %% S1 stores/delivers QoS 0/1 only; a will asking for 2 is rejected at
    %% the codec (reserved value for a 3.1.1 implementation that stops here).
    Body = <<0, 4, "MQTT", 4, 2#00010100, 0, 60, 0, 2, "mo",
             0, 1, "w", 0, 1, "p">>,
    {error, {unsupported_will_qos, 2}} =
        mqtt_frame:decode(<<16#10, (byte_size(Body)), Body/binary>>),
    ok.

connect_reserved_bit_rejected(_Config) ->
    Body = <<0, 4, "MQTT", 4, 2#00000011, 0, 60, 0, 2, "mo">>,
    {error, {malformed_packet, connect_reserved_bit}} =
        mqtt_frame:decode(<<16#10, (byte_size(Body)), Body/binary>>),
    ok.

connect_username_without_password_rejected(_Config) ->
    %% The record cannot represent "username only" (the pair is a package on
    %% the wire as decoded): rejected, not silently dropped.
    Body = <<0, 4, "MQTT", 4, 2#10000010, 0, 60, 0, 2, "mo", 0, 2, "al">>,
    {error, {malformed_packet, username_without_password}} =
        mqtt_frame:decode(<<16#10, (byte_size(Body)), Body/binary>>),
    %% Both flags set round-trips through the record.
    Body2 = <<0, 4, "MQTT", 4, 2#11000010, 0, 60, 0, 2, "mo",
              0, 2, "al", 0, 3, "pwd">>,
    {ok, #connect{username = <<"al">>, password = <<"pwd">>}, <<>>} =
        mqtt_frame:decode(<<16#10, (byte_size(Body2)), Body2/binary>>),
    ok.

utf8_rejections(_Config) ->
    Sub = fun(Filter) ->
              Len = byte_size(Filter),
              Body = <<0, 7, 0, Len, Filter/binary, 1>>,
              mqtt_frame:decode(<<16#82, (byte_size(Body)), Body/binary>>)
          end,
    {error, {malformed_packet, invalid_utf8}} = Sub(<<237, 160, 128>>),
    {error, {malformed_packet, invalid_utf8}} = Sub(<<192, 47>>),
    {error, {malformed_packet, invalid_utf8}} = Sub(<<244, 144, 128, 128>>),
    {error, {malformed_packet, embedded_null}} = Sub(<<97, 0, 98>>),
    {ok, #subscribe{}, <<>>} = Sub(<<16#C3, 16#A9>>),
    ok.

truncated_frames_never_produce_ok(_Config) ->
    Full = mqtt_frame:encode(#connect{client_id = <<"c1">>, keep_alive = 30,
                                      clean_session = 1,
                                      username = <<"alice">>,
                                      password = <<"s3cret">>}),
    ?assertEqual(<<16#10, 29, 0, 4, "MQTT", 4, 2#11000010, 0, 30,
                    0, 2, "c1", 0, 5, "alice", 0, 6, "s3cret">>, Full),
    N = byte_size(Full),
    lists:foreach(
      fun(K) ->
              <<_:K/binary, Tail/binary>> = Full,
              case mqtt_frame:decode(Tail) of
                  {more, _} -> ok;
                  {error, _} -> ok;
                  {ok, _, _} -> ct:fail({truncated_decoded, K})
              end
      end, lists:seq(1, N - 1)),
    {ok, _, <<>>} = mqtt_frame:decode(Full),
    ok.

fixed_header_flag_rejections(_Config) ->
    Reserved = [<<0, 0>>,                                       % type 0
                <<16#F0, 0>>,                                   % type 15
                <<16#10, 0>>,                                   % CONNECT flags /= 0
                <<16#36, 3, 0, 1, "a">>,                        % PUBLISH QoS 0b11
                <<16#21, 2, 0, 0>>,                             % CONNACK flags /= 0
                <<16#38, 1, 0>>,                                % PUBLISH flags 0b1000 (dup on qos0 ok shape, bad len)
                <<16#50, 0>>, <<16#62, 2, 0, 10>>, <<16#70, 2>>, % QoS2 types 5/6/7: no S1 frame set
                <<16#80, 6, 0, 7, 0, 2, "t/", 1>>,              % SUBSCRIBE flags /= 0b0010
                <<16#A0, 6, 0, 7, 0, 2, "t/">>,                 % UNSUBSCRIBE flags /= 0b0010
                <<16#D0, 1, 0>>, <<16#C0, 1, 0>>],              % bodies where none allowed
    lists:foreach(fun(B) -> {error, _} = mqtt_frame:decode(B) end, Reserved),
    %% PUBLISH dup=1 qos=0 is legal on the wire (a redelivery attempt); the
    %% codec carries it through, the conn decides what to do with DUP.
    {ok, #publish{dup = 1, qos = 0}, <<>>} =
        mqtt_frame:decode(<<16#38, 3, 0, 1, "a">>),
    ok.

filter_shape_and_matching(_Config) ->
    ok = mqtt_filter:validate_filter(<<"sport/#">>),
    ok = mqtt_filter:validate_filter(<<"/#">>),
    ok = mqtt_filter:validate_filter(<<"a/+">>),
    ok = mqtt_filter:validate_filter(<<"+">>),
    ok = mqtt_filter:validate_filter(<<"#">>),
    {error, _} = mqtt_filter:validate_filter(<<"#/a">>),
    {error, _} = mqtt_filter:validate_filter(<<"a/#/b">>),
    {error, _} = mqtt_filter:validate_filter(<<"a/b#">>),
    {error, _} = mqtt_filter:validate_filter(<<"a+">>),
    {error, _} = mqtt_filter:validate_filter(<<>>),
    ok = mqtt_filter:validate_topic(<<"/">>),
    {error, _} = mqtt_filter:validate_topic(<<"a/#">>),
    {error, _} = mqtt_filter:validate_topic(<<>>),
    %% §4.7 match examples from the specification's own table.
    ?assert(mqtt_filter:matches(<<"sport/tennis/player1/ranking">>,
                                <<"sport/#">>)),
    ?assert(mqtt_filter:matches(<<"sport">>, <<"sport/#">>)),
    ?assert(mqtt_filter:matches(<<"sport/tennis/player1">>,
                                <<"sport/tennis/+">>)),
    ?assertNot(mqtt_filter:matches(<<"sport/tennis">>, <<"sport/tennis/+">>)),
    ?assert(mqtt_filter:matches(<<"/finance">>, <<"/+">>)),
    %% §4.7.12 Table 4.2: `+/+' *does* match "/finance" — + matches the empty
    %% level, which is a real level created by the leading slash.
    ?assert(mqtt_filter:matches(<<"/finance">>, <<"+/+">>)),
    ?assert(mqtt_filter:matches(<<"a/b">>, <<"a/b">>)),
    ?assertNot(mqtt_filter:matches(<<"a/b/c">>, <<"a/b">>)),
    ?assertNot(mqtt_filter:matches(<<"sport/tennis/player1/#">>,
                                   <<"sport/tennis/player1">>)),
    ok.

%%%_* proper properties -------------------------------------------------------

p1_roundtrip(_Config) ->
    run_prop(prop_roundtrip()),
    ok.

p2_stream_splitting(_Config) ->
    run_prop(prop_stream_splitting()),
    ok.

p3_never_crashes(_Config) ->
    run_prop(prop_never_crashes()),
    ok.

p4_varint_roundtrip(_Config) ->
    run_prop(prop_varint()),
    ok.

p5_encode_rejects_illegal(_Config) ->
    run_prop(prop_encode_rejects()),
    ok.

p6_single_bit_flip(_Config) ->
    run_prop(prop_bitflip()),
    ok.

%% proper:counterexample/2 wants the property structure itself; a fun/0
%% wrapper reads as a non-boolean result (verified against 1.5.0).
run_prop(Prop) ->
    case proper:counterexample(Prop, [{numtests, ?NUMTESTS}]) of
        true -> ok;
        Cex -> ct:fail({counterexample, Cex})
    end.

prop_roundtrip() ->
    ?FORALL({Tag, Frame}, any_frame(),
            case mqtt_frame:decode(mqtt_frame:encode(Frame)) of
                {ok, Frame, <<>>} ->
                    true;
                Other ->
                    collect({fail, Tag, Other}, false)
            end).

prop_stream_splitting() ->
    ?FORALL({_Tag, Frame}, any_frame(),
            begin
                Bin = mqtt_frame:encode(Frame),
                N = byte_size(Bin),
                split_feed(<<>>, Bin, [1, 1, 2, 3, max(1, N div 3), N], Frame)
            end).

%% A TCP reader hands decode/1 its whole accumulated buffer: every arrival is
%% appended, complete frames are drained out, the leftover waits for more.
%% (A bare tail is never a frame start — feeding one would decode garbage.)
split_feed(_Buf, <<>>, _Sizes, Frame) ->
    mqtt_frame:decode(_Buf) =:= {more, _Buf} orelse
        mqtt_frame:decode(_Buf) =:= {ok, Frame, <<>>};
split_feed(_Buf, _Bin, [], _Frame) ->
    true;
split_feed(Buf, Bin, [K | Ks], Frame) ->
    Sz = min(K, byte_size(Bin)),
    <<Chunk:Sz/binary, Rest/binary>> = Bin,
    NewBuf = <<Buf/binary, Chunk/binary>>,
    case drain(mqtt_frame:decode(NewBuf), Frame) of
        ok -> split_feed(<<>>, Rest, Ks, Frame);
        more -> split_feed(NewBuf, Rest, Ks, Frame);
        {error, Reason} -> {bad, Reason}
    end.

%% `drain' walks the leftover chain: a split may expose the frame plus
%% remainder of the *same* buffer only when the frame ended early, which the
%% encoder never produces — so at most one frame per arrival here.
drain({ok, Frame, <<>>}, Frame) -> ok;
drain({ok, _Other, <<>>}, _Frame) -> {error, wrong_frame};
drain({ok, _F, Leftover}, Frame) -> drain(mqtt_frame:decode(Leftover), Frame);
drain({more, _Buf}, _Frame) -> more;
drain({error, Reason}, _Frame) -> {error, Reason}.

prop_never_crashes() ->
    ?FORALL(Bin, binary(),
            case (catch mqtt_frame:decode(Bin)) of
                {'EXIT', _} -> collect(crashed, false);
                {ok, F, R} -> is_tuple(F) andalso is_binary(R);
                {more, B} -> is_binary(B);
                {error, _} -> true
            end).

prop_varint() ->
    ?FORALL(N, choose(0, 268435455),
            begin
                Enc = mqtt_frame:encode_remaining_length(N),
                {ok, N, <<>>} = mqtt_frame:decode_remaining_length(Enc),
                length(binary_to_list(Enc)) =< 4
            end).

prop_encode_rejects() ->
    ?FORALL(T, oneof([pub_qos2, pub_pid_zero, pub_pid_undefined,
                      pub_empty_topic, pub_wildcard_topic,
                      sub_empty, suback_empty, unsub_empty,
                      conn_asymmetric_creds, conn_will_split,
                      conn_clean_2, rl_too_big]),
            did_raise(illegal_frame(T))).

did_raise(F) ->
    case (catch F()) of
        {'EXIT', _} -> true;
        _ -> false
    end.

illegal_frame(pub_qos2) ->
    fun() -> mqtt_frame:encode(#publish{qos = 2, topic = <<"a">>,
                                         packet_id = 1}) end;
illegal_frame(pub_pid_zero) ->
    fun() -> mqtt_frame:encode(#publish{qos = 1, topic = <<"a">>,
                                         packet_id = 0}) end;
illegal_frame(pub_pid_undefined) ->
    fun() -> mqtt_frame:encode(#publish{qos = 1, topic = <<"a">>,
                                         packet_id = undefined}) end;
illegal_frame(pub_empty_topic) ->
    fun() -> mqtt_frame:encode(#publish{qos = 0, topic = <<>>}) end;
illegal_frame(pub_wildcard_topic) ->
    fun() -> mqtt_frame:encode(#publish{qos = 0, topic = <<"a/#">>}) end;
illegal_frame(sub_empty) ->
    fun() -> mqtt_frame:encode(#subscribe{packet_id = 1, topics = []}) end;
illegal_frame(suback_empty) ->
    fun() -> mqtt_frame:encode(#suback{packet_id = 1, return_codes = []}) end;
illegal_frame(unsub_empty) ->
    fun() -> mqtt_frame:encode(#unsubscribe{packet_id = 1, topics = []}) end;
illegal_frame(conn_asymmetric_creds) ->
    fun() -> mqtt_frame:encode(#connect{client_id = <<"c">>,
                                        username = <<"u">>,
                                        password = undefined}) end;
illegal_frame(conn_will_split) ->
    fun() -> mqtt_frame:encode(#connect{client_id = <<"c">>, will_flag = 1,
                                        will_topic = <<"w">>,
                                        will_payload = undefined}) end;
illegal_frame(conn_clean_2) ->
    fun() -> mqtt_frame:encode(#connect{client_id = <<"c">>,
                                        clean_session = 2}) end;
illegal_frame(rl_too_big) ->
    fun() -> mqtt_frame:encode_remaining_length(268435456) end.

%% The plan drafted "a single bit flip never yields *another legal frame*".
%% No framing codec can promise that: a PUBLISH whose 1-byte RL flips 5→4 is
%% a well-formed 4-byte-body PUBLISH plus a leftover byte, and §2.2.3 obliges
%% any conformant decoder to read it. Nor can one promise a flipped type byte
%% avoids the reserved set — every legal nibble is one bit from 0; what holds
%% is that our decoder rejects those (golden vectors in `t_decode'). The
%% codec's real guarantee: decode never crashes on corrupted input and every
%% verdict is a legal outcome. "Unexpected but structurally legal frames" are
%% the conn FSM's contract (T9: reject by state), not the codec's.
prop_bitflip() ->
    ?FORALL({_Tag, RealFrame}, any_frame(),
            begin
                Bin = mqtt_frame:encode(RealFrame),
                {ok, Orig, <<>>} = mqtt_frame:decode(Bin),
                N = byte_size(Bin),
                %% I in [1, N-1]: the pattern below consumes byte I+1, so a
                %% flip index of N would match nothing (badmatch, not a
                %% codec verdict).
                ?FORALL({I, Mask}, {choose(1, max(1, N - 1)),
                                    oneof([1, 2, 4, 8, 16, 32, 64, 128])},
                        begin
                            J = I,
                            <<Head:J/binary, B:8, Tail/binary>> = Bin,
                            Flipped = <<Head/binary, (B bxor Mask):8,
                                        Tail/binary>>,
                            case (catch mqtt_frame:decode(Flipped)) of
                                {'EXIT', Why} ->
                                    collect({crashed, Why}, false);
                                {error, _} ->
                                    true;
                                {more, _} ->
                                    true;
                                {ok, Decoded, _Rest} ->
                                    case element(1, Decoded) =:=
                                         element(1, Orig) of
                                        true -> collect(same_type, true);
                                        false -> collect(type_confused, true)
                                    end
                            end
                        end)
            end).

%%%_* Generators ---------------------------------------------------------------

%% An MQTT UTF-8 encoded string (Annex A): a *string* field (username,
%% password) — raw binary() would happily produce embedded NULs and invalid
%% sequences, which the decoder is correct to reject.
gen_binary_string() ->
    ?LET({N, Cs}, {choose(1, 16), vector(16, choose(32, 126))},
         list_to_binary(lists:sublist(Cs, N))).

%% Printable, no wildcards (topic names) — the shape MQTT topics take in
%% practice, generated structurally instead of filtered for acceptance.
gen_topic_string() ->
    ?LET({A, B}, {choose(1, 8),
                  vector(8, oneof([$,, $., $-, $_, $:, $=, $@, $a, $m, $z,
                                   $0, $9, $/, 233]))},
           begin
               Body = lists:sublist(B, A),
               case lists:member($/, Body) of
                   true -> unicode:characters_to_binary(Body);
                   false -> unicode:characters_to_binary([$a | Body])
               end
           end).

%% Same alphabet with '/' added; '#' and '+' appear only as complete levels,
%% built structurally so no suchthat retry is needed.
gen_filter_string() ->
    frequency([{5, gen_topic_string()},
               {2, <<"+">>},
               {3, ?LET(N, choose(1, 3),
                        <<"b/", << <<"a/">> || _ <- lists:seq(1, N) >>/binary,
                          "#">>)
               }]).

gen_payload() ->
    ?LET(Bytes, list(choose(0, 255)), list_to_binary(Bytes)).

gen_publish() ->
    ?LET({QoS, Dup, Ret, Topic, Pid, Payload},
         {choose(0, 1), choose(0, 1), choose(0, 1), gen_topic_string(),
          gen_pos_int(), gen_payload()},
         #publish{dup = Dup, qos = QoS, retain = Ret, topic = Topic,
                  packet_id = case QoS of 0 -> undefined; 1 -> Pid end,
                  payload = Payload}).

gen_subscribe() ->
    ?LET({Pid, N}, {gen_pos_int(), choose(1, 4)},
         begin
             Topics = [{<<"t", I, "/#">>, I rem 3} || I <- lists:seq(1, N)],
             #subscribe{packet_id = Pid, topics = Topics}
         end).

gen_unsubscribe() ->
    ?LET({Pid, Filter}, {gen_pos_int(), gen_filter_string()},
         #unsubscribe{packet_id = Pid, topics = [Filter]}).

gen_connect() ->
    ?LET({Id, KA, CS, HasWill, HasCreds, WT, WP, WQ, WR, User, Pass},
         {gen_topic_string(), choose(0, 65535), choose(0, 1),
          choose(0, 1), choose(0, 1), gen_topic_string(), gen_payload(),
          choose(0, 1), choose(0, 1), gen_binary_string(),
          gen_binary_string()},
         #connect{client_id = Id, keep_alive = KA, clean_session = CS,
                  %% will_qos/will_retain ride in the will_flag bit pair:
                  %% with will_flag=0 they are invisible on the wire, so a
                  %% record claiming otherwise cannot roundtrip.
                  will_flag = HasWill,
                  will_qos = case HasWill of 1 -> WQ; 0 -> 0 end,
                  will_retain = case HasWill of 1 -> WR; 0 -> 0 end,
                  will_topic = case HasWill of 1 -> WT; 0 -> undefined end,
                  will_payload = case HasWill of 1 -> WP; 0 -> undefined end,
                  username = case HasCreds of 1 -> User; 0 -> undefined end,
                  password = case HasCreds of 1 -> Pass; 0 -> undefined end}).

gen_connack() ->
    ?LET({SP, RC}, {choose(0, 1), choose(0, 5)},
         #connack{session_present = SP, return_code = RC}).

gen_pos_int() ->
    choose(1, 65535).

any_frame() ->
    oneof([{pub, gen_publish()}, {sub, gen_subscribe()},
           {unsub, gen_unsubscribe()}, {conn, gen_connect()},
           {connack, gen_connack()},
           {ping, oneof([#ping{kind = req}])},
           {pingresp, oneof([#ping{kind = resp}])},
           {disc, oneof([#disconnect{}])}]).

%%%_* Internal ----------------------------------------------------------------

unhex(Hex) ->
    Cleaned = binary:replace(Hex, <<"0x">>, <<>>, [global]),
    Tokens = binary:split(Cleaned, [<<" ">>, <<"\n">>], [global, trim_all]),
    iolist_to_binary([binary:decode_hex(T) || T <- Tokens, T =/= <<>>]).
