%% @doc MQTT over QUIC against a **third-party** client: EMQX's `emqtt'
%% (https://github.com/emqx/emqtt), whose `emqtt_quic' module speaks MQTT over
%% QUIC through `quicer' with ALPN `mqtt' on one client-initiated bidi stream.
%%
%% Everything asserted here is asserted from the client's point of view, over
%% real QUIC: CONNECT/CONNACK, the three QoS levels in both directions, retain,
%% Will, MQTT 5.0 properties, and a persistent session resumed by a second
%% connection. None of this repository's code runs in the client process.
%%
%% `run.sh' starts the broker (`mqtt_quic_broker_boot'), exports its ephemeral
%% port as `BROKER_PORT', and runs this module. Each case prints one line; the
%% process exits non-zero if any case failed, so the shell can gate on it.
%%
%% Not covered, and deliberately so (see README):
%%
%%   * 0-RTT early data -- optional in the CN draft, not implemented;
%%   * multi-stream mode -- an EMQX private extension, not implemented;
%%   * closing one stream and CONNECTing on another *of the same connection* --
%%     `emqtt' has no stream-scoped close in its public API. That half of
%%     "stream close is not connection close" is asserted in
%%     `mqtt_quic_listener_SUITE' instead, with raw `quicer' + the same MQTT
%%     engine.
-module(mqtt_quic_compat).

-export([main/1]).

%% Dispatched by name from `run/2'; listing them keeps the harness free of
%% `export_all' while still allowing one uniform call shape.
-export([case_connect/1, case_qos0/1, case_qos1/1, case_qos2/1,
         case_retain/1, case_will/1, case_mqtt5/1, case_reconnect/1]).

-define(HOST, "localhost").
-define(TIMEOUT, 5000).

%%%_* Entry -------------------------------------------------------------------

main(_Args) ->
    Port = port(),
    Cases = selected_cases(),
    io:format("~n=== MQTT over QUIC compatibility (client: emqtt, port ~p) ===~n",
              [Port]),
    Results = [{Case, run(Case, Port)} || Case <- Cases],
    Failed = [Case || {Case, Result} <- Results, Result =/= ok],
    io:format("~n~p/~p cases passed~n", [length(Results) - length(Failed),
                                         length(Results)]),
    case Failed of
        [] -> halt(0);
        _ -> halt(1)
    end.

selected_cases() ->
    case os:getenv("COMPAT_CASES") of
        false -> all_cases();
        "" -> all_cases();
        "all" -> all_cases();
        Str -> [list_to_atom(C) || C <- string:tokens(Str, ",")]
    end.

all_cases() ->
    [connect, qos0, qos1, qos2, retain, will, mqtt5, reconnect].

port() ->
    case os:getenv("BROKER_PORT") of
        false -> erlang:error(broker_port_not_set);
        Str -> list_to_integer(Str)
    end.

run(Case, Port) ->
    %% Settle before, settle after: every client here reports deliveries to this
    %% process, and a delivery still in flight when a case ends would otherwise
    %% surface in the next case as `unexpected_topic'. `settle/0' is the sleep
    %% that lets the broker finish what the previous case started.
    flush(),
    Fun = list_to_atom("case_" ++ atom_to_list(Case)),
    Result =
        try ?MODULE:Fun(Port) of
            ok ->
                io:format("PASS  ~s~n", [Case]),
                ok;
            Other ->
                io:format("FAIL  ~s: unexpected return ~p~n", [Case, Other]),
                {failed, Other}
        catch
            Class:Reason:Stack ->
                io:format("FAIL  ~s: ~p:~p~n  ~p~n", [Case, Class, Reason, Stack]),
                {failed, Reason}
        end,
    settle(),
    Result.

%%%_* Cases -------------------------------------------------------------------

%% The minimum a QUIC deployment has to get right: the TLS 1.3 handshake with
%% ALPN `mqtt' completes, and the MQTT session on the first stream is live (a
%% PINGREQ gets its PINGRESP).
%%
%% ALPN is not read back from the client here: `emqtt_quic:from_sockopts/1'
%% hard-codes `{alpn, ["mqtt"]}', and the server refuses any other value (the CT
%% suite asserts that refusal from the server side, where it is observable).
case_connect(Port) ->
    C = open(Port),
    pong = emqtt:ping(C),
    stop(C).

%% Down and up at QoS 0: no acknowledgement of any kind, so a successful receive
%% is the broker's delivery path working end to end. `emqtt:publish/4' returns a
%% bare `ok' at QoS 0 and `{ok, _}' at QoS 1/2 (nothing to acknowledge).
case_qos0(Port) ->
    S = open(Port),
    P = open(Port),
    {ok, _, _} = emqtt:subscribe(S, <<"quic/qos0">>, 0),
    ok = emqtt:publish(P, <<"quic/qos0">>, <<"zero">>, 0),
    #{qos := 0, payload := <<"zero">>} = expect(<<"quic/qos0">>),
    stop(S),
    stop(P).

%% QoS 1 down and up: the publisher waits for PUBACK, the subscriber's delivery
%% has `qos => 1` and is acknowledged by the client's own stack.
case_qos1(Port) ->
    S = open(Port),
    P = open(Port),
    {ok, _, _} = emqtt:subscribe(S, <<"quic/qos1">>, 1),
    {ok, _} = emqtt:publish(P, <<"quic/qos1">>, <<"one">>, 1),
    #{qos := 1, payload := <<"one">>} = expect(<<"quic/qos1">>),
    stop(S),
    stop(P).

%% QoS 2 both ways: PUBREC/PUBREL/PUBCOMP on the same stream, then a single
%% delivery (duplicates would show up as a second message and fail the flush
%% check at the end of the case).
case_qos2(Port) ->
    S = open(Port),
    P = open(Port),
    {ok, _, _} = emqtt:subscribe(S, <<"quic/qos2">>, 2),
    {ok, _} = emqtt:publish(P, <<"quic/qos2">>, <<"two">>, 2),
    #{qos := 2, payload := <<"two">>} = expect(<<"quic/qos2">>),
    ok = no_more_messages(<<"quic/qos2">>, 500),
    stop(S),
    stop(P).

%% Retained state is broker state, not connection state: the publisher is gone
%% by the time the subscriber asks for the topic, and the delivery still carries
%% `retain => true`.
case_retain(Port) ->
    P = open(Port),
    {ok, _} = emqtt:publish(P, <<"quic/retain">>, <<"kept">>,
                            [{qos, 1}, {retain, true}]),
    stop(P),
    S = open(Port),
    {ok, _, _} = emqtt:subscribe(S, <<"quic/retain">>, 1),
    #{retain := true, payload := <<"kept">>} = expect(<<"quic/retain">>),
    stop(S).

%% Will: the client goes away **without** a DISCONNECT (its process is stopped,
%% which closes the QUIC stream and connection at the transport level). Closing
%% a stream is not a clean MQTT shutdown, so the broker has to publish the Will.
case_will(Port) ->
    S = open(Port),
    {ok, _, _} = emqtt:subscribe(S, <<"quic/will">>, 1),
    W = open(Port, [{will_topic, <<"quic/will">>},
                    {will_payload, <<"gone">>},
                    {will_qos, 1}]),
    ok = gen_statem:stop(W),
    #{payload := <<"gone">>} = expect(<<"quic/will">>),
    stop(S).

%% MQTT 5.0 over the same single stream: properties survive the round trip.
%%
%% Client ids are explicit here: with `proto_ver => v5' and no client id,
%% `emqtt' deliberately sends an empty one (5.0 allows the server to assign an
%% id), and this broker refuses that -- `client_identifier_not_valid'. Making
%% the id explicit keeps the case about properties, which is what it is testing.
case_mqtt5(Port) ->
    S = open(Port, [{clientid, <<"compat-v5-sub">>},
                    {proto_ver, v5}, {clean_start, true}]),
    P = open(Port, [{clientid, <<"compat-v5-pub">>},
                    {proto_ver, v5}, {clean_start, true}]),
    {ok, _, _} = emqtt:subscribe(S, <<"quic/v5">>, [{qos, 1}]),
    Props = #{'User-Property' => [{<<"source">>, <<"compat">>}]},
    {ok, _} = emqtt:publish(P, <<"quic/v5">>, Props, <<"five">>, [{qos, 1}]),
    #{properties := Received, payload := <<"five">>} = expect(<<"quic/v5">>),
    [{<<"source">>, <<"compat">>}] = maps:get('User-Property', Received, []),
    stop(S),
    stop(P).

%% A persistent session survives the connection. The second connection
%% subscribes to nothing: the message arrives only if the broker kept the first
%% connection's subscription (clean_start = false + the same client id).
case_reconnect(Port) ->
    Cid = <<"compat-reconnect">>,
    C1 = open(Port, [{clientid, Cid}, {clean_start, false}]),
    {ok, _, _} = emqtt:subscribe(C1, <<"quic/resume">>, 1),
    stop(C1),
    %% Give the broker a beat to detach the first session before the second
    %% connection with the same client id arrives.
    timer:sleep(300),
    C2 = open(Port, [{clientid, Cid}, {clean_start, false}]),
    P = open(Port),
    {ok, _} = emqtt:publish(P, <<"quic/resume">>, <<"after">>, 1),
    #{payload := <<"after">>} = expect(<<"quic/resume">>),
    stop(C2),
    stop(P).

%%%_* Client ------------------------------------------------------------------

open(Port) ->
    open(Port, []).

open(Port, Extra) ->
    Opts = [{host, ?HOST},
            {port, Port},
            %% `emqtt_quic' reads the connection and stream options from this
            %% pair. Both are **proplists** (`emqtt_quic:from_sockopts/1' does
            %% `DefConnOpts ++ UserConnOpts'), and the defaults it then adds are
            %% the ones the CN draft requires for a client (ALPN `mqtt',
            %% `verify_none', stream `active => 1').
            {quic_opts, {[], []}} | Extra],
    {ok, Client} = emqtt:start_link(Opts),
    case emqtt:quic_connect(Client) of
        {ok, _Properties} -> Client;
        {error, Reason} -> erlang:error({quic_connect, Reason})
    end.

%% Stop the client without sending a DISCONNECT. `emqtt' closes the QUIC
%% connection in `terminate/2', which is exactly the "connection died" case the
%% broker has to survive (and, for a Will client, act on).
stop(Client) ->
    _ = (catch gen_statem:stop(Client)),
    ok.

%%%_* Assertions --------------------------------------------------------------

expect(Topic) ->
    receive
        {publish, #{topic := T} = Msg} when T =:= Topic ->
            Msg;
        {publish, #{topic := Other}} ->
            erlang:error({unexpected_topic, Other, Topic})
    after ?TIMEOUT ->
            erlang:error({no_message, Topic})
    end.

no_more_messages(Topic, Wait) ->
    receive
        {publish, #{topic := T}} when T =:= Topic ->
            erlang:error({duplicate_delivery, Topic})
    after Wait ->
            ok
    end.

%% Let in-flight deliveries land, then discard them. Called between cases: the
%% broker is asynchronous by nature, and a case that ends the instant its last
%% assertion passes would otherwise leak a delivery into the next one.
settle() ->
    timer:sleep(250),
    flush().

flush() ->
    receive
        {publish, _} -> flush();
        {disconnected, _, _} -> flush()
    after 0 ->
            ok
    end.
