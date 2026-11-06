%% MQTT 3.1.1 §2 frame records (shared by mqtt_frame and its consumers).
%% The codec surface is deliberately opaque (mqtt_frame:frame/0); this header
%% exists for the conn_fsm and the test suite, mirroring how
%% include/broker_router.hrl carries route_ctx out of the frozen behavior.
-ifndef(MQTT_FRAME_HRL).
-define(MQTT_FRAME_HRL, true).

-record(connect,
        {proto_name = <<"MQTT">> :: binary(),
         proto_level = 4         :: byte(),
         keep_alive = 0          :: non_neg_integer(),
         clean_session = 0       :: 0 | 1,
         will_flag = 0           :: 0 | 1,
         will_qos = 0            :: 0 | 1,
         will_retain = 0         :: 0 | 1,
         will_topic              :: binary() | undefined,
         will_payload            :: binary() | undefined,
         client_id               :: binary(),
         username                :: binary() | undefined,
         password                :: binary() | undefined}).

%% `return_code' is 0..255 rather than 3.1.1's defined 0..5 because design §4.5
%% refuses an MQTT 5.0 CONNECT with 0x87 -- a byte this broker both sends and
%% must be able to read back. The codec's guards on both halves say the same.
-record(connack, {session_present = 0 :: 0 | 1,
                  return_code = 0      :: 0..255}).

-record(publish, {dup = 0    :: 0 | 1,
                  qos = 0    :: 0 | 1,
                  retain = 0 :: 0 | 1,
                  topic          :: binary(),
                  packet_id      :: pos_integer() | undefined,
                  payload = <<>> :: binary()}).

-record(puback, {packet_id :: pos_integer()}).

-record(subscribe, {packet_id :: pos_integer(),
                    topics = [] :: [{binary(), 0..2}]}).

-record(suback, {packet_id :: pos_integer(),
                 return_codes = [] :: [0 | 1 | 2 | 128]}).

-record(unsubscribe, {packet_id :: pos_integer(),
                      topics = [] :: [binary()]}).

-record(unsuback, {packet_id :: pos_integer()}).

-record(disconnect, {}).

-record(ping, {kind = resp :: req | resp}).

-endif.
