%% @doc The only place this repository calls `quicer'.
%%
%% `broker_proto_mqtt_quic' lives under `apps/', so the **default** profile
%% compiles it too, and in that profile `quicer' is not a dependency at all.
%% Two rules follow, and this module exists to enforce both:
%%
%%   * nothing in the app `-include`s a quicer header or names a quicer record,
%%     so the default build never needs the NIF to be present or even fetched;
%%   * every call goes through `apply/3' here, guarded by `ensure/0', so a
%%     deployment that forgot to add `quicer' gets `{error, quicer_not_available}'
%%     -- an actionable message -- instead of an `undef' crash inside a
%%     supervisor's `init/1'.
%%
%% `quicer' is listed in `optional_applications', not `applications', which is
%% what lets `application:ensure_all_started(broker_proto_mqtt_quic)' succeed on
%% a node that has no QUIC support: the app starts, its listener refuses to, and
%% the rest of the broker is unaffected.
-module(mqtt_quic_nif).

-export([available/0, ensure/0, listen/2, accept/3, handshake/2,
         controlling_process/2, sockname/1, send/2, setopt/3,
         close_stream/1, close_connection/1, close_listener/1]).

-define(MOD, quicer).

%% @doc Whether the NIF backend is loadable at all. Used by the listener to
%% decide between "start" and a clear refusal, and by the suites to decide which
%% groups exist in this profile.
-spec available() -> boolean().
available() ->
    case code:ensure_loaded(?MOD) of
        {module, ?MOD} -> true;
        _ -> false
    end.

-spec ensure() -> ok | {error, quicer_not_available}.
ensure() ->
    case available() of
        true -> ok;
        false -> {error, quicer_not_available}
    end.

-spec listen(inet:port_number(), map()) -> {ok, term()} | {error, term()}.
listen(Port, Opts) ->
    call(listen, [Port, Opts]).

-spec accept(term(), map(), timeout()) -> {ok, term()} | {error, term()}.
accept(Listener, Opts, Timeout) ->
    call(accept, [Listener, Opts, Timeout]).

%% @doc Server side **must** complete the TLS handshake explicitly. Without this
%% call the client's `connect' hangs forever: `quicer:accept' only reports the
%% transport-level connection.
-spec handshake(term(), timeout()) -> {ok, term()} | {error, term()}.
handshake(Conn, Timeout) ->
    call(handshake, [Conn, Timeout]).

-spec controlling_process(term(), pid()) -> ok | {error, term()}.
controlling_process(Handle, Pid) ->
    call(controlling_process, [Handle, Pid]).

%% @doc `{ok, {Ip, Port}}'. For a listener handle this is the only way to learn
%% the port actually bound when the config asked for `port => 0' -- quicer binds
%% inside the NIF, so the port never shows up in `inet:i/0'.
-spec sockname(term()) -> {ok, {inet:ip_address(), inet:port_number()}}
                    | {error, term()}.
sockname(Handle) ->
    call(sockname, [Handle]).

%% @doc `quicer:send/2' answers `{ok, BytesWritten}', which is not the `ok' the
%% transport behaviour promises; normalise it here so no caller has to know.
-spec send(term(), iodata()) -> ok | {error, term()}.
send(Stream, Bin) ->
    case call(send, [Stream, Bin]) of
        {ok, _Bytes} -> ok;
        ok -> ok;
        {error, _} = E -> E
    end.

-spec setopt(term(), atom(), term()) -> ok | {error, term()}.
setopt(Handle, Name, Value) ->
    call(setopt, [Handle, Name, Value]).

-spec close_stream(term()) -> ok | {error, term()}.
close_stream(Stream) ->
    call(close_stream, [Stream]).

-spec close_connection(term()) -> ok | {error, term()}.
close_connection(Conn) ->
    call(close_connection, [Conn]).

-spec close_listener(term()) -> ok | {error, term()}.
close_listener(Listener) ->
    call(close_listener, [Listener]).

call(Fun, Args) ->
    case ensure() of
        ok ->
            try
                apply(?MOD, Fun, Args)
            catch
                Class:Reason -> {error, {Class, Reason}}
            end;
        {error, _} = E ->
            E
    end.
