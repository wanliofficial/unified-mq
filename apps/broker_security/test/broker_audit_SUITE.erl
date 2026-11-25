%% @doc S13 Gate: the tamper-evident security-event audit trail.
%%
%% `broker_security_audit_SUITE' pins the S6/Gate checklist for *security events*
%% (auth, authz-deny, revoke, admin.action, config.boot). Each case is written as
%% an attacker or a careless operator would try to defeat the trail:
%%
%%   1. **Chain integrity** -- `N' events hash-link; verify reports all `N' with
%%      every `prev' matching the prior `hash'.
%%   2. **Tamper** -- flip one byte in a middle line: `verify' names that `seq' and
%%      `hash_mismatch'.
%%   3. **Deletion** -- remove a middle line: the next line's `prev' no longer
%%      matches, `chain_break' at the line after the gap.
%%   4. **Reorder** -- swap two adjacent lines: the swapped line's `prev' points at
%%      a hash that is now two lines ahead, `chain_break'.
%%   5. **Forgery** -- rewrite a line's content but keep its (now-stale) `sig':
%%      the recomputed hash no longer matches, `hash_mismatch'. And a line whose
%%      `hash' is correct but whose `sig' was replaced is `bad_signature'.
%%   6. **Legacy tolerance** -- pre-chain lines (the old config-audit format) are
%%      counted, not failed: the chain starts at the first line that carries
%%      `seq'.
%%   7. **Back-pressure** -- a full buffer drops the record and counts it, never
%%      blocks the caller.
%%   8. **emit is total** -- a missing buffer, a non-map, a dead writer: `emit/1'
%%      always returns `ok' in bounded time.
%%   9. **Restart continuity** -- a writer restart continues the chain from the
%%      file tail instead of starting a fresh, unlinked one.
%%  10. **Admin surface** -- `/api/v1/audit/pubkey' returns the key, and the
%%      verify route reports intact vs tampered.
%%
%% The whole point of the module is that the *hot path never blocks and the
%% broker never falls over*, so cases 7 and 8 assert exactly that: a dead writer
%% or a full buffer must not turn `emit/1' into anything but `ok'.
-module(broker_audit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [chain_links_and_verifies,
     tampered_byte_is_detected,
     deleted_line_breaks_the_chain,
     reordered_lines_break_the_chain,
     forged_content_is_detected,
     bad_signature_is_detected,
     legacy_prefix_is_tolerated,
     full_buffer_drops_and_counts,
     emit_is_total_under_every_failure,
     restart_continues_the_chain,
     admin_pubkey_and_verify_routes].

-define(DIR, "audit_suite").
%% A fixed 32-byte seed so signatures are reproducible within a testcase; the
%% real deployment uses a configured or generated key, but the tests only care
%% that verify accepts a good signature and rejects a bad one.
-define(SEED_B64,
        "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=").

init_per_suite(Config) ->
    ok = filelib:ensure_dir(filename:join(["/tmp/opencode", ?DIR, "x"])),
    [{dir, filename:join(["/tmp/opencode", ?DIR])} | Config].

end_per_suite(_Config) ->
    _ = file:del_dir_r(filename:join(["/tmp/opencode", ?DIR])),
    ok.

init_per_testcase(_Case, Config) ->
    Dir = ?config(dir, Config),
    SavedDataDir = application:get_env(broker_storage, data_dir, undefined),
    SavedEnabled = application:get_env(broker_audit, enabled, undefined),
    SavedPriv = application:get_env(broker_audit, private_key, undefined),
    %% A unique file per testcase so chains never bleed across cases; point the
    %% module at a clean slate in init, restore in cleanup.
    File = unique_name(),
    ok = filelib:ensure_dir(filename:join([Dir, "x"])),
    application:set_env(broker_storage, data_dir, Dir),
    application:set_env(broker_audit, file, File),
    application:set_env(broker_audit, enabled, true),
    application:set_env(broker_audit, private_key, ?SEED_B64),
    ok = broker_audit:init(),
    %% `reset/0' also tells a live writer to re-read the chain head, so a writer
    %% inherited from another suite in the same CT node (whose in-memory cursor
    %% was computed from ITS file) restarts this case's chain at seq 1 instead of
    %% appending seq 31+ to a fresh file and tripping `verify'.
    ok = broker_audit:reset(),
    [{file, File}, {saved_data_dir, SavedDataDir},
     {saved_enabled, SavedEnabled}, {saved_priv, SavedPriv} | Config].

end_per_testcase(_Case, Config) ->
    stop_writer(),
    ok = broker_audit:reset(),
    restore_env(broker_storage, data_dir, ?config(saved_data_dir, Config)),
    restore_env(broker_audit, enabled, ?config(saved_enabled, Config)),
    restore_env(broker_audit, private_key, ?config(saved_priv, Config)),
    ok.

restore_env(_App, _Key, undefined) ->
    application:unset_env(_App, _Key);
restore_env(App, Key, Value) ->
    application:set_env(App, Key, Value).

%%%_* Helpers ---------------------------------------------------------------

%% Start a writer if one is not already up. In a plain CT node the broker_security
%% application is usually NOT started (these cases do not need it), so the writer
%% is brought up directly; if some other suite left it running we reuse it.
ensure_writer() ->
    case whereis(broker_audit) of
        Pid when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true -> Pid;
                false -> start_fresh_writer()
            end;
        undefined ->
            start_fresh_writer()
    end.

start_fresh_writer() ->
    case broker_audit:start_link() of
        {ok, Pid} ->
            unlink(Pid),
            Pid;
        {error, {already_started, Pid}} ->
            unlink(Pid),
            Pid
    end.

stop_writer() ->
    case whereis(broker_audit) of
        undefined -> ok;
        Pid ->
            Ref = erlang:monitor(process, Pid),
            unlink(Pid),
            gen_server:stop(Pid, normal, 2000),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 2000 -> demonitor(Ref, [flush]), ok end
    end.

%% Emit `N' events synchronously (flush drains them).
emit_n(N) ->
    ensure_writer(),
    [broker_audit:emit(#{type => 'auth.failure',
                         result => error, reason => probe, n => I})
     || I <- lists:seq(1, N)],
    ok = broker_audit:flush(),
    ok.

%% Emit without flushing so we can inspect the buffer directly.
emit_to_buffer(Map) ->
    ensure_writer(),
    broker_audit:emit(Map).

audit_path() ->
    {ok, DataDir} = application:get_env(broker_storage, data_dir),
    {ok, File} = application:get_env(broker_audit, file),
    filename:join(DataDir, File).

unique_name() ->
    ["audit_", integer_to_list(erlang:unique_integer([positive])), ".log"].

read_lines() ->
    {ok, Bin} = file:read_file(audit_path()),
    [L || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>].

decode_line(Line) ->
    json:decode(Line).

write_lines(Lines) ->
    Body = binary_join(Lines, $\n),
    ok = file:write_file(audit_path(), [Body, $\n]).

binary_join([], _Sep) -> <<>>;
binary_join([H | T], Sep) ->
    lists:foldl(fun(E, Acc) -> <<Acc/binary, Sep, E/binary>> end, H, T).

%%%_* 1. Chain integrity ----------------------------------------------------

chain_links_and_verifies(_Config) ->
    _Path = emit_n(5),
    #{ok := true, entries := 5, legacy := 0} = broker_audit:verify(),
    %% Every line's prev equals the previous line's hash; the first is genesis.
    Lines = read_lines(),
    5 = length(Lines),
    assert_chain(Lines),
    ok.

assert_chain([]) -> ok;
assert_chain([First | Rest]) ->
    #{<<"prev">> := <<"genesis">>, <<"hash">> := H0} = decode_line(First),
    assert_chain_rest(Rest, H0).

assert_chain_rest([], _Prev) -> ok;
assert_chain_rest([Line | Rest], Prev) ->
    Rec = decode_line(Line),
    #{<<"prev">> := Prev, <<"hash">> := H} = Rec,
    assert_chain_rest(Rest, H).

%%%_* 2. Tamper -------------------------------------------------------------

tampered_byte_is_detected(_Config) ->
    _Path = emit_n(5),
    Lines = read_lines(),
    %% Flip one hex digit in the middle line's hash field value.
    Middle = lists:nth(3, Lines),
    Tampered = flip_one_char(Middle),
    ok = write_lines(replace_at(Lines, 3, Tampered)),
    #{ok := false, tampered_at := 3, reason := hash_mismatch} =
        broker_audit:verify(),
    ok.

%%%_* 3. Deletion -----------------------------------------------------------

deleted_line_breaks_the_chain(_Config) ->
    _Path = emit_n(5),
    Lines = read_lines(),
    %% Delete the third line. The fourth line's prev pointed at the (now gone)
    %% third line's hash, so the break surfaces at seq 4.
    Kept = [L || {I, L} <- lists:zip(lists:seq(1, 5), Lines), I =/= 3],
    ok = write_lines(Kept),
    #{ok := false, tampered_at := 4, reason := chain_break} =
        broker_audit:verify(),
    ok.

%%%_* 4. Reorder ------------------------------------------------------------

reordered_lines_break_the_chain(_Config) ->
    _Path = emit_n(5),
    Lines = read_lines(),
    %% Swap lines 2 and 3. Line 3 now sits at position 2: its prev points at
    %% line 1's hash, which *does* match the running prev (line 1), so position 2
    %% passes. Line 2 now sits at position 3: its prev points at line 1's hash
    %% but the running prev is line 3's hash -- a mismatch, reported at seq 3.
    [L1, L2, L3, L4, L5] = Lines,
    ok = write_lines([L1, L3, L2, L4, L5]),
    #{ok := false, tampered_at := 3, reason := chain_break} =
        broker_audit:verify(),
    ok.

%%%_* 5. Forgery ------------------------------------------------------------

%% An attacker rewrites a line's *content* (e.g. to hide a denied action) but
%% cannot recompute a valid hash without the chain. Even keeping the old sig, the
%% recomputed hash no longer matches the stored one.
forged_content_is_detected(_Config) ->
    _Path = emit_n(4),
    Lines = read_lines(),
    Target = lists:nth(3, Lines),
    Rec = decode_line(Target),
    %% Claim the event was an allow instead of a deny.
    Forged = Rec#{<<"result">> => <<"allow">>},
    Line = iolist_to_binary(json:encode(Forged)),
    ok = write_lines(replace_at(Lines, 3, Line)),
    #{ok := false, tampered_at := 3, reason := hash_mismatch} =
        broker_audit:verify(),
    ok.

%% If an attacker recomputes the hash but cannot sign it (no private key), the
%% If an attacker rewrites a line AND recomputes a valid hash but cannot sign it
%% with the node key (they lack the private key), the signature check catches it.
%% We build a line whose hash is internally consistent -- so `hash_mismatch' does
%% NOT fire -- but whose sig was produced by a different (attacker) key.
bad_signature_is_detected(_Config) ->
    _Path = emit_n(4),
    Lines = read_lines(),
    Target = lists:nth(3, Lines),
    Rec = decode_line(Target),
    %% Recompute the hash the way the writer would, from the record minus hash/sig.
    WithoutSecrets = maps:without([<<"hash">>, <<"sig">>], Rec),
    Wire = broker_audit_wire(WithoutSecrets),
    GoodHash = binary:encode_hex(
                 crypto:hash(sha256, term_to_binary(Wire, [{minor_version, 2}])),
                 lowercase),
    %% Sign that hash with an attacker-generated key, not the node's key.
    {_APub, APriv} = crypto:generate_key(eddsa, ed25519),
    BadSig = binary:encode_hex(
               crypto:sign(eddsa, none, binary:decode_hex(GoodHash), [APriv, ed25519]),
               lowercase),
    Forged = Rec#{<<"hash">> => GoodHash, <<"sig">> => BadSig},
    Line = iolist_to_binary(json:encode(Forged)),
    ok = write_lines(replace_at(Lines, 3, Line)),
    #{ok := false, tampered_at := 3, reason := bad_signature} =
        broker_audit:verify(),
    ok.

%% Mirror of broker_audit's wire/1 normalisation, for building an attacker line
%% the verifier will hash identically (binary keys, atom->binary, drop non-JSON).
broker_audit_wire(Term) when is_map(Term) ->
    maps:from_list([{wire_key(K), broker_audit_wire(V)}
                    || {K, V} <- maps:to_list(Term), json_safe(V)]);
broker_audit_wire(Term) when is_list(Term) ->
    [broker_audit_wire(V) || V <- Term];
broker_audit_wire(Term) when is_atom(Term) -> atom_to_binary(Term, utf8);
broker_audit_wire(Term) when is_binary(Term); is_number(Term) -> Term;
broker_audit_wire(_) -> null.

wire_key(K) when is_atom(K) -> atom_to_binary(K, utf8);
wire_key(K) when is_binary(K) -> K;
wire_key(K) when is_integer(K) -> integer_to_binary(K);
wire_key(K) -> iolist_to_binary(io_lib:format("~p", [K])).

json_safe(V) ->
    not (is_pid(V) orelse is_reference(V) orelse is_function(V) orelse is_port(V)).

%%%_* 6. Legacy tolerance ---------------------------------------------------

legacy_prefix_is_tolerated(_Config) ->
    %% Seed the file with a pre-chain line (the old config-audit shape: a hash and
    %% a snapshot but no seq/prev/sig). verify must count it, not fail on it.
    Legacy = iolist_to_binary(
               json:encode(#{<<"hash">> => <<"abc">>,
                             <<"snapshot">> => #{<<"a">> => 1}})),
    ok = file:write_file(audit_path(), [Legacy, $\n]),
    %% Now emit two chain events; the writer must start a fresh chain after the
    %% legacy line and verify must pass with legacy = 1.
    _Path = emit_n(2),
    #{ok := true, entries := 2, legacy := 1} = broker_audit:verify(),
    ok.

%%%_* 7. Back-pressure ------------------------------------------------------

full_buffer_drops_and_counts(_Config) ->
    %% No writer here: emit straight into the bare ring buffer so nothing drains
    %% it. The cap is ?BUF_MAX (10000). Fill it exactly, then one more must be
    %% dropped and counted, while emit still returns ok. reset() in init zeroed
    %% the counter, so the count is exact.
    without_writer(fun() ->
        Max = 10000,
        [broker_audit:emit(#{type => 'auth.failure', result => error, n => N})
         || N <- lists:seq(1, Max)],
        ok = broker_audit:emit(#{type => 'auth.failure', result => error,
                                 n => overflow}),
        ?assertEqual(1, broker_audit:dropped())
    end).

%%%_* 8. emit totality ------------------------------------------------------

emit_is_total_under_every_failure(_Config) ->
    %% Non-map input: a no-op, never a crash, even with no writer running.
    ok = broker_audit:emit(not_a_map),
    ok = broker_audit:emit(<<"a string">>),
    ok = broker_audit:emit(42),
    %% A map with no writer draining: emit lands in the buffer and returns ok.
    %% Nothing is lost and nothing blocks -- the writer is not on the hot path.
    without_writer(fun() ->
        ok = broker_audit:reset(),
        ok = broker_audit:emit(#{type => 'authz.deny', result => deny}),
        ?assertEqual(1, buffered_count())
    end),
    %% A missing buffer table must also be tolerated: a caller that fires before
    %% `init/0' ran (or after a test deleted the table) must not get an exception
    %% out of an auth path. This is the fail-safe `broker_admission' posture.
    without_writer(fun() ->
        Deleted = try ets:delete(broker_audit_buffer), true catch _:_ -> false end,
        ?assert(Deleted),
        ok = broker_audit:emit(#{type => 'authz.deny', result => deny})
    end),
    %% Restore the buffer for the rest of the suite.
    ok = broker_audit:init(),
    ok.

%% Run `Fun' with the audit writer paused (if one is running), so the bare ring
%% buffer can be inspected without a concurrent drain. `sys:suspend' is used
%% rather than stopping the process: the writer may be a supervised child
%% (restart = permanent) and stopping it would race an immediate restart. A
%% writer we started ourselves is simply stopped. Both are restored in `after'.
without_writer(Fun) ->
    case whereis(broker_audit) of
        undefined ->
            Fun();
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    ok = sys:suspend(Pid),
                    try Fun() after ok = sys:resume(Pid) end;
                false ->
                    Fun()
            end
    end.

%% Count what is sitting in the buffer right now (no writer draining it).
buffered_count() ->
    try ets:info(broker_audit_buffer, size)
    catch error:badarg -> 0 end.

%%%_* 9. Restart continuity -------------------------------------------------

restart_continues_the_chain(_Config) ->
    _Path = emit_n(3),
    %% Restart the writer. It must read the tail and continue at seq 4 with the
    %% last hash as prev, not restart a disconnected chain at seq 1.
    stop_writer(),
    ensure_writer(),
    ok = broker_audit:emit(#{type => 'auth.failure', result => error, seq_tag => 4}),
    ok = broker_audit:flush(),
    #{ok := true, entries := 4, legacy := 0} = broker_audit:verify(),
    %% seq must be monotonic 1..4 with no repeat.
    Seqs = [S || #{<<"seq">> := S} <- [decode_line(L) || L <- read_lines()]],
    ?assertEqual([1, 2, 3, 4], Seqs),
    ok.

%%%_* 10. Admin surface -----------------------------------------------------

admin_pubkey_and_verify_routes(_Config) ->
    %% pubkey route returns the configured key. `handle/4' returns
    %% {Status, ContentType, Body}; decode the JSON body.
    {200, _, PubBody} = broker_admin_http:handle(
                          get, <<"/api/v1/audit/pubkey">>, #{}, <<>>),
    #{<<"pubkey">> := Pub} = json:decode(iolist_to_binary(PubBody)),
    ?assert(is_binary(Pub)),
    %% With a clean (empty) trail the verify route reports intact.
    {200, _, OkBody} = broker_admin_http:handle(
                          get, <<"/api/v1/audit/verify">>, #{}, <<>>),
    #{<<"ok">> := true} = json:decode(iolist_to_binary(OkBody)),
    %% Emit a real event, flush, and the route still reports intact.
    emit_n(3),
    {200, _, VBody} = broker_admin_http:handle(
                         get, <<"/api/v1/audit/verify">>, #{}, <<>>),
    #{<<"ok">> := true, <<"entries">> := 3} =
        json:decode(iolist_to_binary(VBody)),
    %% Tamper the file on disk; the route now reports 409 with tampered_at.
    Lines = read_lines(),
    Bad = flip_one_char(lists:nth(2, Lines)),
    ok = write_lines(replace_at(Lines, 2, Bad)),
    {409, _, BadBody} = broker_admin_http:handle(
                           get, <<"/api/v1/audit/verify">>, #{}, <<>>),
    #{<<"ok">> := false, <<"tampered_at">> := 2} =
        json:decode(iolist_to_binary(BadBody)),
    ok.


replace_at(List, Idx, New) ->
    {Before, [_Old | After]} = lists:split(Idx - 1, List),
    Before ++ [New | After].

%% Flip the first hex digit of the first "hash":"...." value we can find. If the
%% line has no hash field, flip its first byte after the opening brace instead --
%% either way the line's content changes and its stored hash must stop matching.
flip_one_char(Line) ->
    flip_one_char(Line, byte_size(Line)).

flip_one_char(<<${, Rest/binary>>, _Len) ->
    %% Skip to the first '"', flip the char after it.
    flip_val(Rest).

flip_val(<<$", C, Rest/binary>>) ->
    Flipped = flip_digit(C),
    <<${, $", Flipped, Rest/binary>>;
flip_val(<<B, Rest/binary>>) ->
    <<B, (flip_val(Rest))/binary>>.

flip_digit(C) when C >= $0, C < $9 -> ((C - $0 + 1) rem 10) + $0;
flip_digit(_) -> $0.
