%% @doc ADR-028 §D7 guard: the signaling face must never touch the message
%% kernel.
%%
%% This is a **source-level** test on purpose. `rebar3 xref' in this repo only
%% covers undefined/deprecated/recursive calls and unused exports; it is not a
%% call-graph query, so it cannot prove the absence of an edge. Reading the
%% sources and asserting that no kernel call/record appears is the check the
%% ADR actually specifies.
%%
%% If a future change genuinely needs a kernel dependency, this suite fails
%% and the change has to go back to ADR-028 (D6: another ADR, with the kernel
%% change scope spelled out), not silently through.
-module(webrtc_boundary_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [no_broker_queue_calls,
     no_broker_router_calls,
     no_broker_message_record,
     no_broker_core_include,
     no_subscription_or_cursor_state,
     app_dependency_ledger_is_clean].

%%%_* Source scanning -------------------------------------------------------

init_per_suite(Config) ->
    {ok, Src} = source_dir(broker_proto_webrtc),
    [{webrtc_src, Src} | Config].

end_per_suite(_Config) ->
    ok.

%% Locate the app's own source tree. CT runs from
%% `_build/test/lib/<app>/src', which is a symlink back to the real checkout;
%% `filelib:wildcard/1' does not follow a symlinked *directory*, so resolve
%% the link first.
source_dir(App) ->
    case code:lib_dir(App) of
        {error, Reason} ->
            {error, {app_not_found, App, Reason}};
        Lib ->
            Src = real_path(filename:join(Lib, "src")),
            case filelib:is_dir(Src) of
                true -> {ok, Src};
                false -> {error, {src_not_found, Src}}
            end
    end.

%% `_build/test/lib/<app>/src' is a symlink to the checkout; read the link so
%% the wildcard below sees a real directory.
real_path(Path) ->
    case file:read_link_all(Path) of
        {ok, Target} ->
            case filename:pathtype(Target) of
                absolute -> Target;
                _ -> filename:absname(Target, filename:dirname(Path))
            end;
        _ ->
            Path
    end.

%% Every .erl in the app's src/ (there is no other place code can live).
sources(Config) ->
    Dir = ?config(webrtc_src, Config),
    Files = filelib:wildcard(filename:join(Dir, "*.erl")),
    ?assert(length(Files) >= 4),
    [{F, strip_comments(read(F))} || F <- Files].

read(File) ->
    {ok, Bin} = file:read_file(File),
    Bin.

%% Code only: a comment that *names* `broker_queue:append/3' (like the ADR
%% quote in this app's headers) is documentation, not a call. A guard test
%% that fails on prose would be turned off within a week, so the scanner
%% removes `%' to end-of-line outside string literals before matching.
strip_comments(Bin) ->
    iolist_to_binary(strip(Bin, false, [])).

strip(<<>>, _InString, Acc) ->
    lists:reverse(Acc);
strip(<<$%, Rest/binary>>, false, Acc) ->
    %% Drop to the end of the line, keep the newline for line numbering.
    strip(drop_line(Rest), false, Acc);
strip(<<$", Rest/binary>>, InString, Acc) ->
    strip(Rest, not InString, [$" | Acc]);
strip(<<$\\, C, Rest/binary>>, true, Acc) ->
    strip(Rest, true, [C, $\\ | Acc]);
strip(<<C, Rest/binary>>, InString, Acc) ->
    strip(Rest, InString, [C | Acc]).

drop_line(<<>>) -> <<>>;
drop_line(<<$\n, Rest/binary>>) -> <<$\n, Rest/binary>>;
drop_line(<<_, Rest/binary>>) -> drop_line(Rest).

%%%_* Cases ----------------------------------------------------------------

no_broker_queue_calls(Config) ->
    assert_absent(Config, <<"broker_queue:">>,
                  <<"ADR-028 D1: signaling must not append to any queue">>).

no_broker_router_calls(Config) ->
    assert_absent(Config, <<"broker_router:">>,
                  <<"ADR-028 D1: signaling must not route through the kernel">>).

no_broker_message_record(Config) ->
    assert_absent(Config, <<"#broker_message{">>,
                  <<"ADR-028 D1: signaling must not produce a kernel message">>).

no_broker_core_include(Config) ->
    assert_absent(Config, <<"broker_core/include">>,
                  <<"ADR-028 D7: no broker_core header may be included">>).

%% SubRef/cursor/retention state is the third data plane ADR-028 forbids.
%% `broker_queue', `broker_router' and the record are the sharp signals; this
%% one catches a reviewer importing the vocabulary without the call.
no_subscription_or_cursor_state(Config) ->
    assert_absent(Config, <<"broker_subscription:">>,
                  <<"ADR-028 D1: no subscription index in the signaling face">>),
    assert_absent(Config, <<".cursors">>,
                  <<"ADR-028 D1: no cursor state in the signaling face">>),
    assert_absent(Config, <<"broker_queue:append">>,
                  <<"ADR-028 D2: append/3 has no transient bypass">>).

%% The dependency ledger is the other half of the guard (ADR-028 D8): the .app
%% `applications' list must not name an app that would drag the kernel in.
app_dependency_ledger_is_clean(_Config) ->
    %% The compiled `.app' lives in the code path's ebin; `code:lib_dir/1'
    %% resolves it regardless of whether the suite ran from the checkout or
    %% from `_build/test`.
    AppFile = filename:join(code:lib_dir(broker_proto_webrtc),
                            "ebin/broker_proto_webrtc.app"),
    {ok, [{application, broker_proto_webrtc, Props}]} = file:consult(AppFile),
    Apps = proplists:get_value(applications, Props, []),
    ?assertNot(lists:member(broker_queue, Apps)),
    ?assertNot(lists:member(broker_router, Apps)),
    ?assertNot(lists:member(broker_subscription, Apps)),
    ?assertNot(lists:member(broker_event, Apps)),
    %% The ones it does need must be there, or the app would start in an
    %% environment where authentication silently does nothing.
    ?assert(lists:member(broker_security, Apps)),
    ?assert(lists:member(broker_ws, Apps)),
    ?assert(lists:member(ranch, Apps)).

%%%_* Assertion ------------------------------------------------------------

assert_absent(Config, Needle, Why) ->
    Hits = [{F, line_of(Content, Needle)}
            || {F, Content} <- sources(Config),
               binary:match(Content, Needle) =/= nomatch],
    case Hits of
        [] ->
            ok;
        _ ->
            ct:fail({forbidden_token, Needle, Why, Hits})
    end.

line_of(Content, Needle) ->
    {Pos, _Len} = binary:match(Content, Needle),
    Lines = binary:split(binary:part(Content, 0, Pos), <<"\n">>, [global]),
    length(Lines).
