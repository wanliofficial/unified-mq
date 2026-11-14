%% S10-D D.2 driver: the runtime "no persistence" proof.
%%
%% One VM boots the real kernel *and* the production WSS signaling listener
%% *and* the production STUN/TURN server, then:
%%
%%   1. snapshot the three durable facts (`s10_no_persist:snapshot/0');
%%   2. drive a full 1:1 WebRTC call with the real third-party client
%%      (`webrtc_evidence.py' -- aiortc: offer/answer/ICE/data channel);
%%   3. snapshot again and require `unchanged';
%%   4. **negative control A**: append one ordinary message and require the diff
%%      to report a moved offset and moved byte count. A snapshot that cannot
%%      move proves nothing, so the guard has to be shown to have teeth;
%%   5. **negative control B**: create a real subscription, ack one record and
%%      wait one cursor tick, then require a `.cursors' file to appear. Same
%%      reason: the cursor detector must be able to see one.
%%
%% Steps 4 and 5 are not part of the "no persistence" claim -- they are the
%% control group that makes the claim falsifiable.
%%
%% Entry point: `erl -noshell ... -s s10_no_persist_run main'. Reads
%% `S10_DIR' (where the python scripts live) and `S10_NOPERSIST_OUT' (JSONL
%% evidence path) from the environment.
-module(s10_no_persist_run).

-include_lib("broker_core/include/broker_message.hrl").

-export([main/0]).

main() ->
    logger:remove_handler(default),
    ok = logger:add_handler(default, logger_std_h,
                            #{config => #{type => standard_error}}),
    #{wss_port := WssPort, stun_port := StunPort, ice_ip := IceIp} =
        s10_node:boot(),
    io:format("S10-NODE-READY wss=~p stun=~p ice=~s room=e2e~n",
              [WssPort, StunPort, inet:ntoa(IceIp)]),

    %% Let the kernel settle and let one cursor tick pass before the baseline,
    %% so a baseline taken mid-tick cannot masquerade as a signaling write.
    timer:sleep(500),
    %% Seed exactly one ordinary message and let retention settle. This is what
    %% gives the offset/byte invariants teeth on their own: the baseline is a
    %% non-empty log (high > 1, bytes > 0), so "unchanged" is an observation
    %% about a live writer instead of a comparison of two zeroes.
    Seed = append_one(<<"s10.d2.seed">>, <<"seed">>),
    timer:sleep(300),
    _ = wal_gc_run(),
    Before = s10_no_persist:snapshot(),
    io:format("S10-SNAPSHOT before ~s~n", [s10_no_persist:format(Before)]),

    %% --- 1:1 call through the real relay, real client ----------------------
    Evidence = run_python(WssPort, StunPort, IceIp),
    After = s10_no_persist:snapshot(),
    io:format("S10-SNAPSHOT after ~s~n", [s10_no_persist:format(After)]),

    Signaling = s10_no_persist:diff(Before, After),
    Appended =
        case append_one(<<"s10.d2.control">>, <<"control">>) of
            {ok, Offset} ->
                timer:sleep(200),
                _ = wal_gc_run(),
                {Offset, s10_no_persist:diff(After, s10_no_persist:snapshot())};
            {error, Reason} ->
                {error, Reason, unchanged}
        end,
    Cursors = cursor_control(),

    ok = write_evidence(Evidence, Seed, Before, After, Signaling, Appended, Cursors),
    Verdict = verdict(Evidence, Signaling, Appended, Cursors),
    io:format("S10-NOPERSIST-VERDICT ~s~n", [Verdict]),
    halt(case Verdict of "PASS" -> 0; _ -> 1 end).

%%%_* Negative control A: one append must move the snapshot ------------------

append_one(Destination, Payload) ->
    Msg = #broker_message{message_id = <<"s10-d2-control">>,
                          tenant_id = <<"t1">>,
                          destination = Destination,
                          partition_id = 0,
                          payload = Payload,
                          payload_encoding = raw,
                          headers = #{}, proto_meta = #{},
                          persistence = durable,
                          reliability = fsync,
                          created_at = erlang:system_time(millisecond)},
    case broker_queue:append(0, Msg, fsync) of
        {ok, Offset} -> {ok, Offset};
        {error, Reason} -> {error, Reason}
    end.

%% The periodic retention sweep is on a 60 s timer; run it explicitly so the
%% control is not racing it and a byte-count change cannot be attributed to GC.
wal_gc_run() ->
    try wal_gc:run() catch _:_ -> ok end.

%%%_* Negative control B: a real subscription must produce a `.cursors' ------

cursor_control() ->
    Before = s10_no_persist:snapshot(),
    EmptyBefore = maps:get(cursors, Before),
    ClientId = <<"s10-d2-control">>,
    Filter = <<"s10/d2/control">>,
    case broker_subscription:subscribe(ClientId, Filter, 1) of
        {ok, SubRef} ->
            %% Ack the cursor's own anchor, not a fixed offset: a subscribe
            %% anchors at the partition's live tail, so a hard-coded 1 would be
            %% behind the cursor and change nothing (advance_plain ignores it),
            %% which would make this control pass or fail for the wrong reason.
            Acked = case broker_subscription:cursor(SubRef, 0) of
                        {ok, Anchor} ->
                            ok = broker_subscription:ack(SubRef, 0, Anchor,
                                                         ClientId),
                            Anchor;
                        {error, _} = Err ->
                            Err
                    end,
            timer:sleep(400),
            Files = maps:get(cursors, s10_no_persist:snapshot()),
            _ = broker_subscription:unsubscribe(ClientId, Filter),
            #{sub_ref => SubRef,
              acked => Acked,
              files_before => EmptyBefore,
              files_after => Files,
              ok => Files =/= [] andalso Files =/= EmptyBefore};
        {error, Reason} ->
            #{sub_ref => undefined, files_before => EmptyBefore,
              files_after => [], ok => false, error => Reason}
    end.

%%%_* Python driver ---------------------------------------------------------

run_python(WssPort, StunPort, IceIp) ->
    Dir = os:getenv("S10_DIR", "."),
    Libs = os:getenv("S10_LIBS", "/tmp/opencode/s10-libs"),
    Out = evidence_dir() ++ "/webrtc_signaling_d2.jsonl",
    _ = file:delete(Out),
    Cmd = lists:flatten(
            ["cd ", shell_quote(Dir), " && PYTHONPATH=", shell_quote(Libs),
             " /usr/bin/python3 webrtc_evidence.py",
             " --host 127.0.0.1",
             " --stun-host ", inet:ntoa(IceIp),
             " --wss-port ", integer_to_list(WssPort),
             " --stun-port ", integer_to_list(StunPort),
             " --out ", shell_quote(Out),
             " > ", shell_quote(evidence_dir() ++ "/d2-python.out"), " 2>&1"]),
    Output = os:cmd(Cmd),
    _ = Output,
    read_jsonl(Out).

evidence_dir() ->
    filename:dirname(os:getenv("S10_NOPERSIST_OUT",
                              "/tmp/opencode/s10-evidence/no_persistence.jsonl")).

shell_quote(S) ->
    "'" ++ S ++ "'".

read_jsonl(Path) ->
    case file:read_file(Path) of
        {ok, Bin} -> [Line || Line <- binary:split(Bin, <<"\n">>, [global]),
                              Line =/= <<>>];
        {error, Reason} -> [#{<<"missing">> => iolist_to_binary(io_lib:format("~p", [Reason]))}]
    end.

%%%_* Verdict + evidence ---------------------------------------------------

verdict(Evidence, Signaling, Appended, Cursors) ->
    CallOk = python_says_pass(Evidence),
    NoPersist = Signaling =:= unchanged,
    ControlA = control_a_ok(Appended),
    ControlB = maps:get(ok, Cursors, false),
    case CallOk andalso NoPersist andalso ControlA andalso ControlB of
        true -> "PASS";
        false -> "FAIL"
    end.

%% The python script prints `S10-EVIDENCE-VERDICT PASS' on stdout, which we
%% redirected; the JSONL always ends with its own verdict line.
python_says_pass(Evidence) ->
    lists:any(fun(Line) ->
                      binary:match(Line, <<"\"event\": \"verdict\"">>) =/= nomatch
                          andalso binary:match(Line, <<"\"ok\": true">>) =/= nomatch
              end, Evidence).

control_a_ok({_Offset, {changed, Moved}}) ->
    Keys = [K || {K, _, _} <- Moved],
    lists:member(offsets, Keys) andalso lists:member(wal_bytes, Keys);
control_a_ok(_) ->
    false.

write_evidence(Evidence, Seed, Before, After, Signaling, Appended, Cursors) ->
    Path = os:getenv("S10_NOPERSIST_OUT",
                     "/tmp/opencode/s10-evidence/no_persistence.jsonl"),
    ok = filelib:ensure_dir(Path),
    Lines =
        [json(#{event => <<"seed">>,
                offset => elem_or_null(Seed)}),
         json(#{event => <<"snapshot">>, phase => <<"before">>,
                snapshot => s10_no_persist:to_json(Before)}),
         json(#{event => <<"call">>,
                python_events => [jsonl_object(E) || E <- Evidence]}),
         json(#{event => <<"snapshot">>, phase => <<"after">>,
                snapshot => s10_no_persist:to_json(After)}),
         json(#{event => <<"no_persistence">>,
                diff => diff_json(Signaling)}),
         json(#{event => <<"control_append">>,
                offset => append_offset(Appended),
                diff => diff_json(append_diff(Appended))}),
         json(#{event => <<"control_cursor">>,
                sub_ref => subref_json(Cursors),
                files_before => s10_no_persist:to_json(
                                  maps:get(files_before, Cursors, [])),
                files_after => s10_no_persist:to_json(
                                 maps:get(files_after, Cursors, [])),
                ok => maps:get(ok, Cursors, false)}),
         json(#{event => <<"verdict">>,
                verdict => iolist_to_binary(verdict(Evidence, Signaling,
                                                    Appended, Cursors)),
                call_ok => python_says_pass(Evidence),
                no_persistence => Signaling =:= unchanged,
                control_append => control_a_ok(Appended),
                control_cursor => maps:get(ok, Cursors, false)})],
    ok = file:write_file(Path, [[L, $\n] || L <- Lines]),
    io:format("S10-NOPERSIST-EVIDENCE ~s~n", [Path]),
    ok.

elem_or_null({ok, Offset}) -> Offset;
elem_or_null(_) -> undefined.

append_offset({Offset, _Diff}) when is_integer(Offset) -> Offset;
append_offset(_) -> undefined.

append_diff({_Offset, Diff}) -> Diff;
append_diff(_) -> unchanged.

json(Term) -> unicode:characters_to_binary(json:encode(Term)).

jsonl_object(Line) ->
    try json:decode(Line) of
        Map -> Map
    catch _:_ -> #{<<"raw">> => Line}
    end.

diff_json(unchanged) -> #{changed => false, moved => []};
diff_json({changed, Moved}) ->
    #{changed => true,
      moved => [#{fact => atom_to_binary(K, utf8),
                  before => B, after_value => A}
                || {K, B, A} <- Moved]}.

subref_json(undefined) -> null;
subref_json(#{sub_ref := SubRef}) ->
    {Kind, Key, Filter} = SubRef,
    #{kind => atom_to_binary(Kind, utf8), key => Key, filter => Filter}.
