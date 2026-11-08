%% MQTT frame records (shared by mqtt_frame and its consumers).
%%
%% MQTT 3.1.1 §2 frame records, plus the MQTT 5.0 (S2.5, ADR-017) additions.
%% The 3.1.1 fields keep their order and types; every S2.5 field is appended
%% with a default, so a 3.1.1 decode never touches it and construction is
%% source-compatible.
%%
%% The codec surface is deliberately opaque (mqtt_frame:frame/0); this header
%% exists for the conn_fsm and the test suite, mirroring how
%% include/broker_router.hrl carries route_ctx out of the frozen behavior.
-ifndef(MQTT_FRAME_HRL).
-define(MQTT_FRAME_HRL, true).

-record(connect,
        {proto_name = <<"MQTT">> :: binary(),
         proto_level = 4         :: byte(),
         keep_alive = 0          :: non_neg_integer(),
         clean_session = 0       :: 0 | 1,       %% MQTT 5: clean start
         will_flag = 0           :: 0 | 1,
         will_qos = 0            :: 0 | 1 | 2,
         will_retain = 0         :: 0 | 1,
         will_topic              :: binary() | undefined,
         will_payload            :: binary() | undefined,
         client_id               :: binary(),
         username                :: binary() | undefined,
         password                :: binary() | undefined,
         %% S2.5 (ADR-017): properties live here, never in broker headers.
         props = #{}             :: map(),
         will_props = #{}        :: map()}).

%% `return_code' is 0..255 rather than 3.1.1's defined 0..5 because this broker
%% sends MQTT 5 reason codes (0x84/0x87/0x97) that the peer reads as a failure
%% byte; the codec's guard covers both halves.
-record(connack, {session_present = 0 :: 0 | 1,
                  return_code = 0      :: 0..255,
                  props = #{}          :: map()}).

-record(publish, {dup = 0    :: 0 | 1,
                  qos = 0    :: 0 | 1 | 2,
                  retain = 0 :: 0 | 1,
                  topic          :: binary(),
                  packet_id      :: pos_integer() | undefined,
                  payload = <<>> :: binary(),
                  props = #{}    :: map()}).

-record(puback, {packet_id :: pos_integer(),
                 reason_code = 0 :: 0..255,
                 props = #{}     :: map()}).

%% QoS 2 handshake (S2-C, ADR-012/018). The fixed header flags are part of the
%% wire contract: PUBREC/PUBCOMP carry 0b0000, PUBREL carries 0b0010.
-record(pubrec, {packet_id :: pos_integer(),
                 reason_code = 0 :: 0..255,
                 props = #{}     :: map()}).
-record(pubrel, {packet_id :: pos_integer(),
                 reason_code = 0 :: 0..255,
                 props = #{}     :: map()}).
-record(pubcomp, {packet_id :: pos_integer(),
                  reason_code = 0 :: 0..255,
                  props = #{}     :: map()}).

-record(subscribe, {packet_id :: pos_integer(),
                    topics = [] :: [{binary(), 0..2}],
                    props = #{} :: map()}).

-record(suback, {packet_id :: pos_integer(),
                 return_codes = [] :: [0 | 1 | 2 | 128],
                 props = #{}       :: map()}).

-record(unsubscribe, {packet_id :: pos_integer(),
                      topics = [] :: [binary()],
                      props = #{} :: map()}).

-record(unsuback, {packet_id :: pos_integer(),
                   reason_codes = [] :: [0..255],
                   props = #{}       :: map()}).

-record(disconnect, {reason_code = 0 :: 0..255,
                     props = #{}     :: map()}).

-record(ping, {kind = resp :: req | resp}).

-endif.
