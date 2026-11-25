%% @doc Tamper-evident security-event audit trail (S13).
%%
%% `broker_config_audit' writes one boot-time config snapshot with a per-line
%% SHA-256 that is *not* linked to anything; edit, delete or reorder a line and
%% the digest still matches. This module closes that gap for *security events*:
%% every record is appended to a hash chain (each `hash' covers the previous
%% hash) and signed with Ed25519, so a writer who can rewrite the file but does
%% not hold the private key cannot forge a consistent chain.
%%
%% == Never on the hot path ==
%%
%% Authentication and ACL decisions run on every connection and every message
%% (eight protocol apps call `broker_security:authorize/2'). Audit must never
%% block there. So `emit/1' only appends to a bounded ETS ring buffer in the
%% caller's process -- O(1), no message round-trip, no gen_server call -- and a
%% separate writer process (`broker_audit' gen_server) drains the buffer
%% asynchronously, computes the chain and writes the file. This mirrors
%% `broker_metrics' (writes are `counters:add/3', not calls) and
%% `broker_admission' (the hot path stays local).
%%
%% Two consequences follow, both deliberate:
%%
%%   * **Audit never takes the broker down.** If the writer dies or the disk is
%%     full, the application still boots and traffic still flows; the only
%%     effect is that records are dropped and counted (`dropped/0'). An audit
%%     record is evidence, not a gate -- the same posture `broker_config_audit'
%%     already takes.
%%   * **The buffer is bounded.** When it is full, `emit/1' drops the record and
%%     bumps the drop counter instead of applying back-pressure. A security
%%     event storm (credential stuffing) must not be able to stall the broker by
%%     filling an unbounded queue.
%%
%% == Record shape (one JSON line) ==
%%
%% ```
%% {"seq":N,"at":Ms,"type":"authz.deny","principal":"alice","tenant":"t1",
%%  "action":"publish","destination":"t2/x","result":"deny","reason":"acl",
%%  "prev":"<hex sha256 of previous line's hash>",
%%  "hash":"<hex sha256 of canonical(record-without-hash/sig)>",
%%  "sig":"<hex ed25519 signature over hash>"}
%% '''
%%
%% `prev' links the chain; `hash' covers every field including `prev'; `sig'
%% authenticates `hash' with the node's auditor key. `verify/0,1' walks the file
%% line by line checking both the chain and every signature.
%%
%% == Key handling ==
%%
%% The auditor key is a 32-byte Ed25519 seed, base64, read from
%% `broker_audit.private_key' (env) or the file at `broker_audit.private_key_file'.
%% When neither is set, a fresh key is generated at boot and a warning logged:
%% the chain and signatures are still internally consistent, but a restart
%% starts a new chain under a new key (offline verification must then switch
%% public keys at the boundary). Production deployments should pin the key.
%% The public half is exposed as `pubkey/0', served by the admin API at
%% `GET /api/v1/audit/pubkey', so an offline verifier needs only the public key.
%%
%% == Boundary: tail truncation ==
%%
%% A hash chain detects edits, deletions and reorders *strictly before* the end
%% of the file, because the next line's `prev' no longer matches. It cannot, by
%% itself, detect truncation of the *trailing* lines: dropping the last N lines
%% leaves a shorter chain that still verifies. Closing that needs an external
%% anchor (ship each tail hash to a separate store, an append-only sink, or a
%% trusted timestamp) -- out of scope for this module, same boundary as the
%% still-open external-KMS item in `docs/security.md' §12. `entries' lets a
%% monitor alert on an unexpected drop, which is the honest mitigation we ship.
-module(broker_audit).

%% Hot-path emit + the query/verify surface.
-export([init/0, reset/0, start_link/0]).
-export([emit/1, verify/0, verify/1, verify/2, pubkey/0, dropped/0, path/0, flush/0]).
%% gen_server callbacks (dispatched only at runtime).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(PT, {?MODULE, state}).
-define(DROP_KEY, {?MODULE, drop_ref}).
-define(TAB, broker_audit_buffer).
-define(BUF_MAX, 10000).
-define(DRAIN_MS, 50).
-define(DEFAULT_FILE, "audit.log").

%%%_* Public API ------------------------------------------------------------

%% @doc Create the ring buffer and drop counter. Called by `broker_security_app'
%% before the supervisor, so a front end that authenticates during boot always
%% finds a buffer to write into (mirrors `broker_admission:init/0' and
%% `broker_revocation:init/0'). Idempotent.
-spec init() -> ok.
init() ->
    case ets:whereis(?TAB) of
        undefined ->
            _ = ets:new(?TAB, [named_table, public, ordered_set,
                               {write_concurrency, true},
                               {read_concurrency, true}]),
            ok;
        _Tid ->
            ok
    end,
    _ = drop_ref(),
    ok.

-doc "Drop the ring buffer contents and reset the drop counter (tests only).".
%% Also tells a live writer to re-read the chain head, so a test that points the
%% module at a fresh file does not inherit a cursor computed from the previous
%% file (which would append seq N+1 to a one-line chain and look like tampering).
-spec reset() -> ok.
reset() ->
    _ = try ets:delete_all_objects(?TAB) catch error:badarg -> ok end,
    _ = try atomics:put(drop_ref(), 1, 0) catch _:_ -> ok end,
    _ = try gen_server:call(?MODULE, reset_chain, 5000) catch _:_ -> ok end,
    ok.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Append one security event. Total and non-blocking: a malformed event, a
%% missing buffer, or a full buffer all return `ok' -- audit must never make an
%% authentication or authorization decision fail. The writer fills in
%% `seq'/`prev'/`hash'/`sig' when it drains.
%%
%% Non-map events are ignored (the caller may pass a richer shape; only maps are
%% recorded, so a bad call is a no-op rather than a crash).
-spec emit(map()) -> ok.
emit(Event) when is_map(Event) ->
    case enabled() of
        false ->
            ok;
        true ->
            try
                case ets:info(?TAB, size) of
                    N when is_integer(N), N >= ?BUF_MAX ->
                        bump_drop();
                    _ ->
                        Key = erlang:unique_integer([monotonic, positive]),
                        _ = ets:insert(?TAB, {Key, Event}),
                        ok
                end
            catch
                error:badarg ->
                    %% Buffer not created (module not started): drop silently,
                    %% same as `broker_admission' tolerating a missing table.
                    ok
            end
    end;
emit(_Other) ->
    ok.

%% @doc Verify the trail at the configured path, checking every signature
%% against *this node's* public key.
-spec verify() -> map().
verify() ->
    verify(path(), pubkey()).

%% @doc Verify a specific audit file against this node's public key.
-spec verify(file:filename_all()) -> map().
verify(Path) ->
    verify(Path, pubkey()).

%% @doc Verify a specific audit file against an explicit base64 public key.
%% Passing a key is how an offline verifier checks a copied log without trusting
%% (or even running) the node that wrote it. Returns
%% `#{ok => true, entries => N, legacy => L}` or
%% `#{ok => false, tampered_at => Seq, reason => Reason}`.
%% `legacy' counts pre-chain lines (present before this module existed) that are
%% skipped rather than failed.
-spec verify(file:filename_all(), binary() | undefined) -> map().
verify(Path, Pub) ->
    case file:read_file(Path) of
        {error, enoent} ->
            #{ok => true, entries => 0, legacy => 0};
        {error, Reason} ->
            #{ok => false, reason => Reason};
        {ok, Bin} ->
            Lines = [L || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>],
            try
                verify_lines(Lines, undefined, Pub, 0, 0)
            catch
                throw:{tampered, Seq, Reason} ->
                    #{ok => false, tampered_at => Seq, reason => Reason}
            end
    end.

%% @doc The node's auditor public key, base64. `undefined' if the module has no
%% key (not started). Used by the offline verifier and the admin endpoint.
-spec pubkey() -> binary() | undefined.
pubkey() ->
    case persistent_term:get(?PT, undefined) of
        #{pubkey := Pub} -> Pub;
        _ -> undefined
    end.

%% @doc How many events were dropped because the buffer was full (since boot).
-spec dropped() -> non_neg_integer().
dropped() ->
    try atomics:get(drop_ref(), 1)
    catch _:_ -> 0 end.

%% @doc The audit file path.
-spec path() -> file:filename_all().
path() ->
    DataDir = application:get_env(broker_storage, data_dir, "data"),
    filename:join(DataDir, application:get_env(broker_audit, file, ?DEFAULT_FILE)).

%% @doc Synchronously drain the buffer to disk and return. The periodic writer
%% already drains every ?DRAIN_MS; this is the explicit handle for a graceful
%% shutdown, an operator action, or a test that must not poll for the tick. It is
%% a `gen_server:call', so it also waits for the writer to be alive -- if it is
%% not (crashed, never started) the call exits, which is the one condition under
%% which flushing is impossible. Returns `ok' whether or not there was anything
%% to write.
-spec flush() -> ok.
flush() ->
    gen_server:call(?MODULE, flush, 15000).

%%%_* gen_server ------------------------------------------------------------

init([]) ->
    {Priv, Pub, Source} = load_key(),
    persistent_term:put(?PT, #{priv => Priv, pubkey => base64:encode(Pub),
                               source => Source}),
    {Seq, Prev} = chain_state(),
    _ = erlang:send_after(?DRAIN_MS, self(), drain),
    {ok, #{seq => Seq, prev => Prev}}.

handle_call(flush, _From, St) ->
    {Reply, St1} = drain_once(St),
    {reply, Reply, St1};
handle_call(reset_chain, _From, _St) ->
    %% Test hook: drop any in-memory cursor and re-read it from the configured
    %% file, so a fresh per-testcase file starts the chain at seq 1.
    {Seq, Prev} = chain_state(),
    {reply, ok, #{seq => Seq, prev => Prev}};
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(drain, St) ->
    {_Reply, St1} = drain_once(St),
    _ = erlang:send_after(?DRAIN_MS, self(), drain),
    {noreply, St1};
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, St) ->
    %% Best effort: flush what is buffered so a clean shutdown does not lose
    %% events silently.
    {_Reply, _St1} = drain_once(St),
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Draining --------------------------------------------------------------

%% Take everything currently buffered, sort by insertion key (the unique_integer
%% gives a total order matching emit order), and append each to the chain. The
%% {seq, prev} cursor lives in the gen_server state and is threaded forward, so a
%% drain never re-reads the whole file -- only the writer touches the chain head.
drain_once(St) ->
    case enabled() of
        false -> {ok, St};
        true ->
            Rows = try ets:tab2list(?TAB) catch error:badarg -> [] end,
            case Rows of
                [] -> {ok, St};
                _ ->
                    _ = try ets:delete_all_objects(?TAB) catch error:badarg -> ok end,
                    Sorted = lists:keysort(1, Rows),
                    Seq = maps:get(seq, St),
                    Prev = maps:get(prev, St),
                    {LastSeq, LastHash} = append_all(Sorted, Seq, Prev),
                    {ok, St#{seq => LastSeq, prev => LastHash}}
            end
    end.

%% Fold the buffered events into the file, threading {seq, prev} forward and
%% returning the cursor after the last line written (or the input cursor when
%% there was nothing to write).
append_all([], Seq, Prev) ->
    {Seq, Prev};
append_all([{_Key, Event} | Rest], Seq, Prev) ->
    Signed = sign_record(Event, Seq, Prev),
    _ = append_line(Signed),
    append_all(Rest, Seq + 1, maps:get(hash, Signed)).

%% Fill in seq/at/defaults, then hash the canonical record and sign the hash.
sign_record(Event, Seq, Prev) ->
    At = maps:get(at, Event, erlang:system_time(millisecond)),
    Base = Event#{seq => Seq, at => At, prev => Prev},
    Hash = record_hash(Base),
    Sig = sign(Hash),
    Base#{hash => Hash, sig => Sig}.

%% `hash' = sha256 over a deterministic encoding of every field except `hash'
%% and `sig'. We normalise to a binary-keyed wire map with only JSON-safe values,
%% then `term_to_binary' it: two records that differ only in map iteration order
%% hash identically, and the same record always hashes the same across runs and
%% nodes. Crucially this uses the *same* `wire/1' the encoder uses, so the value
%% a verifier recovers by decoding the JSON line recomputes to the same hash.
record_hash(Base) ->
    Wire = wire(maps:without([hash, sig], Base)),
    binary:encode_hex(crypto:hash(sha256, term_to_binary(Wire, [{minor_version, 2}])),
                      lowercase).

append_line(Record) ->
    Path = path(),
    try
        _ = filelib:ensure_dir(Path),
        Line = [iolist_to_binary(json:encode(wire(Record))), $\n],
        case file:open(Path, [append, raw, binary]) of
            {ok, Fd} ->
                Result = file:write(Fd, Line),
                _ = file:close(Fd),
                Result;
            {error, _} = Err ->
                Err
        end
    catch
        _:_ -> {error, audit_write_failed}
    end.

%%%_* Chain state -----------------------------------------------------------

%% The next {seq, prev}: read the tail of the file so a restart continues the
%% chain instead of starting a fresh, unlinked one. If the file is absent or its
%% last line is unparseable we start at seq 1 with prev "genesis"; that is a
%% visible boundary, not a silent reset.
chain_state() ->
    case file:read_file(path()) of
        {ok, Bin} ->
            Lines = [L || L <- binary:split(Bin, <<"\n">>, [global]), L =/= <<>>],
            case last_chain_line(lists:reverse(Lines)) of
                {Seq, Hash} -> {Seq + 1, Hash};
                none -> {1, <<"genesis">>}
            end;
        {error, _} ->
            {1, <<"genesis">>}
    end.

%% Find the most recent line that carries seq+hash (skipping any legacy
%% config-audit-style lines that never had them). We reverse so "most recent"
%% means the last line in the file, not the first -- a restart must resume from
%% the tail, otherwise a second run would restart the chain at seq 1 with
%% `prev = genesis' and the file would verify as broken.
last_chain_line([]) ->
    none;
last_chain_line([Line | Rest]) ->
    try json:decode(Line) of
        #{<<"seq">> := Seq, <<"hash">> := Hash}
          when is_integer(Seq), is_binary(Hash) ->
            {Seq, Hash};
        _ ->
            last_chain_line(Rest)
    catch
        _:_ -> last_chain_line(Rest)
    end.

%%%_* Verification ----------------------------------------------------------

verify_lines([], _Prev, _Pub, Entries, Legacy) ->
    #{ok => true, entries => Entries, legacy => Legacy};
verify_lines([Line | Rest], Prev, Pub, Entries, Legacy) ->
    try json:decode(Line) of
        #{<<"seq">> := Seq, <<"hash">> := Hash, <<"sig">> := Sig} = Decoded
          when is_integer(Seq), is_binary(Hash), is_binary(Sig) ->
            ExpectedPrev = maps:get(<<"prev">>, Decoded, <<"genesis">>),
            case Prev of
                undefined -> ok;              %% first chain line: prev is anchor
                _ when ExpectedPrev =:= Prev -> ok;
                _ -> throw({tampered, Seq, chain_break})
            end,
            ReHash = recompute_hash(Decoded),
            case ReHash =:= Hash of
                false -> throw({tampered, Seq, hash_mismatch});
                true -> ok
            end,
            case Pub =:= undefined orelse verify_sig(Hash, Sig, Pub) of
                false -> throw({tampered, Seq, bad_signature});
                true -> ok
            end,
            verify_lines(Rest, Hash, Pub, Entries + 1, Legacy);
        _ ->
            %% A line without seq/hash/sig is a legacy (pre-chain) record: skip
            %% it and count it, do not fail -- the operator may have upgraded a
            %% node whose audit log predates this module.
            verify_lines(Rest, Prev, Pub, Entries, Legacy + 1)
    catch
        throw:{tampered, _, _} = T -> throw(T);
        _:_ -> verify_lines(Rest, Prev, Pub, Entries, Legacy + 1)
    end.

%% Recompute a line's hash from its decoded fields (drop hash/sig), exactly as
%% the writer computed it. The decoded map already has binary keys and JSON
%% scalars, so `wire/1' is idempotent on it.
recompute_hash(Decoded) ->
    WithoutSecrets = maps:without([<<"hash">>, <<"sig">>], Decoded),
    Wire = wire(WithoutSecrets),
    binary:encode_hex(crypto:hash(sha256, term_to_binary(Wire, [{minor_version, 2}])),
                      lowercase).

%%%_* Keys / crypto ---------------------------------------------------------

load_key() ->
    case configured_seed() of
        {ok, Seed} ->
            {Pub, Priv} = crypto:generate_key(eddsa, ed25519, Seed),
            {Priv, Pub, configured};
        undefined ->
            {Pub, Priv} = crypto:generate_key(eddsa, ed25519),
            logger:warning("broker_audit: no key configured, generated an "
                           "ephemeral Ed25519 key; the chain will not verify "
                           "across restarts. Set broker_audit.private_key."),
            {Priv, Pub, ephemeral}
    end.

configured_seed() ->
    case application:get_env(broker_audit, private_key) of
        {ok, B64} when is_binary(B64) -> decode_seed(B64);
        {ok, B64} when is_list(B64) -> decode_seed(iolist_to_binary(B64));
        {ok, _} -> undefined;
        undefined ->
            case application:get_env(broker_audit, private_key_file) of
                {ok, File} -> read_seed_file(File);
                undefined -> undefined
            end
    end.

decode_seed(B64) ->
    try
        Seed = base64:decode(B64),
        case byte_size(Seed) of
            32 -> {ok, Seed};
            _ -> undefined
        end
    catch _:_ -> undefined end.

read_seed_file(File) ->
    case file:read_file(File) of
        {ok, Bin} ->
            case decode_seed(string:trim(Bin)) of
                {ok, Seed} -> {ok, Seed};
                undefined -> undefined
            end;
        {error, _} ->
            undefined
    end.

sign(Hash) ->
    case persistent_term:get(?PT, undefined) of
        #{priv := Priv} ->
            bin_to_hex(crypto:sign(eddsa, none, hex_to_bin(Hash), [Priv, ed25519]));
        _ ->
            <<>>
    end.

verify_sig(Hash, SigHex, PubB64) ->
    try
        Pub = base64:decode(PubB64),
        crypto:verify(eddsa, none, hex_to_bin(Hash), hex_to_bin(SigHex), [Pub, ed25519])
    catch _:_ ->
        false
    end.

bin_to_hex(Bin) -> binary:encode_hex(Bin, lowercase).
hex_to_bin(Hex) -> binary:decode_hex(Hex).

%%%_* Canonical wire form ----------------------------------------------------

%% The single normaliser used for BOTH the hash and the on-disk encoding, so
%% the verifier -- which recovers a record by decoding the JSON line -- recomputes
%% exactly the hash the writer computed. It maps every record to a binary-keyed
%% map whose values are only JSON-native scalars (binary/number/boolean/null)
%% and lists/maps thereof, and it is idempotent on `json:decode' output.
%%
%% The one subtlety: atoms must become binaries *before* encoding, because
%% `json:encode' renders an atom as a string that decodes back to a binary. If
%% we hashed an atom value but wrote its decoded binary form, the two sides
%% would diverge. So `wire(undefined) =:= <<"undefined">>' and, symmetrically,
%% the verifier's `wire(<<"undefined">>) =:= <<"undefined">>'. Same for keys.
wire(Term) when is_map(Term) ->
    maps:from_list([{wire_key(K), wire(V)} || {K, V} <- maps:to_list(Term),
                                              json_safe(V)]);
wire(Term) when is_list(Term) ->
    [wire(V) || V <- Term];
wire(Term) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
wire(Term) when is_binary(Term) ->
    Term;
wire(Term) when is_number(Term) ->
    Term;
wire(_) ->
    null.

wire_key(K) when is_atom(K) -> atom_to_binary(K, utf8);
wire_key(K) when is_binary(K) -> K;
wire_key(K) when is_integer(K) -> integer_to_binary(K);
wire_key(K) -> iolist_to_binary(io_lib:format("~p", [K])).

json_safe(V) ->
    not (is_pid(V) orelse is_reference(V) orelse is_function(V) orelse is_port(V)).

%%%_* Small helpers ---------------------------------------------------------

enabled() ->
    application:get_env(broker_audit, enabled, true) =:= true.

drop_ref() ->
    case persistent_term:get(?DROP_KEY, undefined) of
        undefined ->
            Ref = atomics:new(1, [{signed, false}]),
            persistent_term:put(?DROP_KEY, Ref),
            Ref;
        Ref ->
            Ref
    end.

bump_drop() ->
    try atomics:add(drop_ref(), 1, 1) catch _:_ -> ok end.
