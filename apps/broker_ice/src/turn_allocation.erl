%% @doc One TURN allocation (RFC 5766 §5-§11), one process, one relay socket.
%%
%% An allocation exists only because a client sent a successful Allocate on a
%% given 5-tuple. It owns:
%%
%%   * its relay UDP socket, bound in the configured 49152-65535 range;
%%   * the long-term credential key (re-derived on every authenticated request,
%%     so a password rotation takes effect on Refresh -- R-I3);
%%   * the permission set (`CreatePermission', peer IP only for UDP) and the
%%     channel bindings (`ChannelBind', channel number <-> peer 5-tuple);
%%   * its lifetime timer. Refresh(0) or expiry ends it; the process is a
%%     `temporary' child, so no supervisor puts it back.
%%
%% Requests are forwarded here by `broker_ice_udp' after it has matched the
%% client 5-tuple. Responses are sent straight out of the listener's socket
%% (UDP sockets are writable from any process), which keeps the listener from
%% being a reply bottleneck. Nothing in this module logs a password or a key;
%% the only identity it ever names is the username.
-module(turn_allocation).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/1, request/4, send_indication/2, channel_data/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(PERMISSION_LIFETIME_S, 300).
-define(CHANNEL_MIN, 16#4000).
-define(CHANNEL_MAX, 16#7FFF).

-record(st, {client :: {inet:ip_address(), inet:port_number()},
             main_socket :: gen_udp:socket(),
             relay_socket :: gen_udp:socket(),
             relay_port :: inet:port_number(),
             username :: binary(),
             realm :: binary(),
             secret :: binary(),
             key :: <<_:128>>,
             %% The configured ceiling on a lifetime (R-X5); `lifetime_s' below
             %% is what the *current* allocation was granted, which a Refresh
             %% may lower but never raise above this.
             max_lifetime_s :: pos_integer(),
             lifetime_s :: pos_integer(),
             max_permissions :: pos_integer(),
             max_channels :: pos_integer(),
             permissions = #{} :: #{inet:ip_address() => integer()},
             channels = #{} :: #{non_neg_integer() =>
                                     {inet:ip_address(), inet:port_number()}},
             by_peer = #{} :: #{{inet:ip_address(), inet:port_number()} =>
                                    non_neg_integer()},
             timer :: reference() | undefined,
             token :: reference() | undefined}).

%%%_* API -------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

request(Pid, Method, Msg, Wire) ->
    gen_server:cast(Pid, {request, Method, Msg, Wire}).

send_indication(Pid, Msg) ->
    gen_server:cast(Pid, {send_indication, Msg}).

channel_data(Pid, Channel, Data) ->
    gen_server:cast(Pid, {channel_data, Channel, Data}).

%%%_* Lifecycle -------------------------------------------------------------

init(Opts) ->
    Port = maps:get(relay_port, Opts),
    case gen_udp:open(Port, [binary, {active, once}, {reuseaddr, true}]) of
        {ok, Socket} ->
            St = #st{client = maps:get(client, Opts),
                     main_socket = maps:get(main_socket, Opts),
                     relay_socket = Socket,
                     relay_port = Port,
                     username = maps:get(username, Opts),
                     realm = maps:get(realm, Opts),
                     secret = maps:get(secret, Opts),
                     key = maps:get(key, Opts),
                     max_lifetime_s = maps:get(lifetime_s, Opts),
                     lifetime_s = maps:get(lifetime_s, Opts),
                     max_permissions = maps:get(max_permissions, Opts),
                     max_channels = maps:get(max_channels, Opts)},
            %% D.4: one line per allocation, never the key or password. The
            %% client 5-tuple and relay port are already visible via
            %% broker_ice_udp:allocations/0; this just puts them on the timeline.
            ?LOG_INFO("turn_allocation: allocated client=~s relay_port=~b username=~s",
                      [client_str(St#st.client), Port, St#st.username]),
            {ok, schedule(St)};
        {error, Reason} ->
            {stop, Reason}
    end.

terminate(Reason, #st{relay_socket = Socket} = St) ->
    _ = gen_udp:close(Socket),
    ?LOG_INFO("turn_allocation: released client=~s relay_port=~b reason=~p",
              [client_str(St#st.client), St#st.relay_port, coarse(Reason)]),
    ok.

%% Only coarse termination causes are logged; the client 5-tuple is already in
%% the line, so this is about "why did it end", not "who".
coarse(normal) -> normal;
coarse({shutdown, _}) -> shutdown;
coarse(_) -> abnormal.

client_str({Ip, Port}) ->
    IpStr = inet:ntoa(Ip),
    iolist_to_binary([IpStr, $:, integer_to_list(Port)]).

handle_call(_Request, _From, St) ->
    {reply, {error, unknown_call}, St}.

%% The handlers answer either a new state or `{stop, Reason, State}' -- a
%% Refresh(0) ends the allocation -- so the cast wrapper has to pass that
%% through rather than wrapping it in `{noreply, ...}', which would leave a
%% zombie allocation holding a relay port until its timer fired.
handle_cast({request, Method, Msg, Wire}, St) ->
    reply(handle_request(Method, Msg, Wire, St));
handle_cast({send_indication, Msg}, St) ->
    reply(do_send_indication(Msg, St));
handle_cast({channel_data, Channel, Data}, St) ->
    reply(do_channel_data(Channel, Data, St));
handle_cast(_Other, St) ->
    {noreply, St}.

reply({stop, Reason, St}) -> {stop, Reason, St};
reply(St) -> {noreply, St}.

handle_info({udp, Socket, Ip, Port, Data}, #st{relay_socket = Socket} = St) ->
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, peer_data(Ip, Port, Data, St)};
handle_info({expire, Token}, #st{token = Token} = St) ->
    {stop, normal, St};
handle_info({expire, _OldToken}, St) ->
    {noreply, St};
handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Authenticated requests ------------------------------------------------

handle_request(Method, Msg, Wire, St) ->
    Cfg = #{realm => St#st.realm, secret => St#st.secret,
            action => action(Method), destination => <<"$ice/turn">>},
    case turn_auth:authenticate(Msg, Wire, Cfg) of
        {ok, #{principal := User, key := Key}} when User =:= St#st.username ->
            dispatch(Method, Msg, St#st{key = Key});
        {ok, _Other} ->
            %% A different account presenting a valid credential for somebody
            %% else's 5-tuple is a mismatch, not a permission problem.
            reply_error(441, Msg, St);
        {error, {Code, _Reason}} ->
            reply_error(Code, Msg, St)
    end.

action(refresh) -> turn_refresh;
action(create_permission) -> turn_permission;
action(channel_bind) -> turn_channel.

dispatch(refresh, Msg, St) ->
    refresh(Msg, St);
dispatch(create_permission, Msg, St) ->
    create_permission(Msg, St);
dispatch(channel_bind, Msg, St) ->
    channel_bind(Msg, St).

%%%_* Refresh ---------------------------------------------------------------

refresh(#{transaction_id := Tid, attributes := Attrs} = Msg, St) ->
    Requested = case stun_codec:attribute(
                       Attrs, stun_codec:attribute_type(lifetime)) of
                    undefined -> St#st.max_lifetime_s;
                    Value -> stun_codec:lifetime(Value)
                end,
    case Requested of
        0 ->
            _ = send(St, respond(refresh, success, Tid,
                                 [{stun_codec:attribute_type(lifetime), <<0:32>>}],
                                 St#st.key)),
            {stop, normal, St};
        Seconds when is_integer(Seconds), Seconds > 0 ->
            %% RFC 5766 §7.2: the server grants at most its own ceiling; a
            %% client may ask for less but never more.
            Lifetime = min(Seconds, St#st.max_lifetime_s),
            St1 = schedule(St#st{lifetime_s = Lifetime}),
            _ = send(St1, respond(refresh, success, Tid,
                                  [{stun_codec:attribute_type(lifetime),
                                    <<Lifetime:32>>}],
                                  St1#st.key)),
            St1;
        _ ->
            %% A LIFETIME attribute that is not a 32-bit integer is malformed,
            %% not a request to keep the current value.
            reply_error(400, Msg, St)
    end.

schedule(#st{timer = Old, lifetime_s = Lifetime} = St) ->
    _ = case Old of
            undefined -> ok;
            _ -> erlang:cancel_timer(Old)
        end,
    Token = make_ref(),
    Timer = erlang:send_after(Lifetime * 1000, self(), {expire, Token}),
    St#st{timer = Timer, token = Token}.

%%%_* CreatePermission ------------------------------------------------------

create_permission(#{transaction_id := Tid, attributes := Attrs} = Msg, St) ->
    Peers = peer_addresses(Attrs),
    case Peers of
        [] ->
            reply_error(400, Msg, St);
        _ ->
            Expiry = now_ms() + ?PERMISSION_LIFETIME_S * 1000,
            %% Permissions are per peer IP (RFC 5766 §9): any port of that
            %% address is allowed, which is what a browser behind NAT needs.
            Ips = lists:usort([Ip || {Ip, _Port} <- Peers]),
            Existing = maps:keys(St#st.permissions),
            New = [Ip || Ip <- Ips, not lists:member(Ip, Existing)],
            case length(Existing) + length(New) =< St#st.max_permissions of
                true ->
                    Perms = lists:foldl(fun(Ip, Acc) -> maps:put(Ip, Expiry, Acc)
                                        end, St#st.permissions, Ips),
                    _ = send(St, respond(create_permission, success, Tid, [],
                                         St#st.key)),
                    St#st{permissions = Perms};
                false ->
                    reply_error(508, Msg, St)
            end
    end.

peer_addresses(Attrs) ->
    Values = stun_codec:attributes(
               Attrs, stun_codec:attribute_type(xor_peer_address)),
    lists:filtermap(fun(V) ->
                            case stun_codec:decode_xor_address(V) of
                                {ok, Addr} -> {true, Addr};
                                {error, _} -> false
                            end
                    end, Values).

%%%_* ChannelBind -----------------------------------------------------------

channel_bind(#{transaction_id := Tid, attributes := Attrs} = Msg, St) ->
    Channel = channel_of(Attrs),
    Peer = case peer_addresses(Attrs) of
               [P | _] -> P;
               [] -> undefined
           end,
    if
        Channel =:= undefined; Peer =:= undefined ->
            reply_error(400, Msg, St);
        true ->
            channel_bind_1(Channel, Peer, Tid, Msg, St)
    end.

channel_bind_1(Channel, {Ip, _Port} = Peer, Tid, Msg, St) ->
    case {maps:find(Channel, St#st.channels),
          maps:find(Peer, St#st.by_peer)} of
        {{ok, Peer}, _} ->
            _ = send(St, respond(channel_bind, success, Tid, [], St#st.key)),
            St;
        {{ok, _OtherPeer}, _} ->
            %% RFC 5766 §11.2: a channel number is bound to exactly one peer.
            reply_error(400, Msg, St);
        {_, {ok, Existing}} when Existing =/= Channel ->
            reply_error(400, Msg, St);
        _ ->
            case permission_valid(Ip, St) of
                false ->
                    %% A ChannelBind must have a permission in place first;
                    %% otherwise a client could open a relay path by binding.
                    reply_error(403, Msg, St);
                true when map_size(St#st.channels) >= St#st.max_channels ->
                    reply_error(508, Msg, St);
                true ->
                    Channels = maps:put(Channel, Peer, St#st.channels),
                    ByPeer = maps:put(Peer, Channel, St#st.by_peer),
                    _ = send(St, respond(channel_bind, success, Tid, [],
                                         St#st.key)),
                    St#st{channels = Channels, by_peer = ByPeer}
            end
    end.

channel_of(Attrs) ->
    case stun_codec:attribute(Attrs,
                              stun_codec:attribute_type(channel_number)) of
        <<Channel:16, _Reserved:16>> when Channel >= ?CHANNEL_MIN,
                                          Channel =< ?CHANNEL_MAX ->
            Channel;
        _ ->
            undefined
    end.

%%%_* Send indication / ChannelData -----------------------------------------

do_send_indication(#{attributes := Attrs}, St) ->
    case {stun_codec:attribute(Attrs, stun_codec:attribute_type(data)),
          stun_codec:attribute(
            Attrs, stun_codec:attribute_type(xor_peer_address))} of
        {Data, PeerValue} when is_binary(Data), is_binary(PeerValue) ->
            case stun_codec:decode_xor_address(PeerValue) of
                {ok, {Ip, Port}} ->
                    case permission_valid(Ip, St) of
                        true -> forward_to_peer(Ip, Port, Data, St);
                        false -> St
                    end;
                {error, _} ->
                    St
            end;
        _ ->
            St
    end.

do_channel_data(Channel, Data, St) ->
    case maps:find(Channel, St#st.channels) of
        {ok, {Ip, Port}} -> forward_to_peer(Ip, Port, Data, St);
        error -> St
    end.

%% Returns the (unchanged) state: `handle_cast' feeds this return value straight
%% into `reply/1', which treats anything that is not `{stop, _, _}' as the new
%% state. Returning `gen_udp:send''s `ok' here would replace the `#st{}' record
%% with the atom `ok', so the *second* ChannelData on the allocation would crash
%% with `badrecord' -- i.e. a relay that worked exactly once.
forward_to_peer(Ip, Port, Data, #st{relay_socket = Socket} = St) ->
    _ = gen_udp:send(Socket, Ip, Port, Data),
    St.

%%%_* Peer -> client --------------------------------------------------------

peer_data(Ip, Port, Data, St) ->
    case maps:find({Ip, Port}, St#st.by_peer) of
        {ok, Channel} ->
            _ = send(St, <<Channel:16, (byte_size(Data)):16, Data/binary>>),
            St;
        error ->
            case permission_valid(Ip, St) of
                true ->
                    Tid = crypto:strong_rand_bytes(12),
                    Attrs = [{stun_codec:attribute_type(xor_peer_address),
                              stun_codec:xor_address(Ip, Port, Tid)},
                             {stun_codec:attribute_type(data), Data}],
                    Msg = #{method => data, class => indication,
                            transaction_id => Tid, attributes => Attrs},
                    Wire = stun_codec:add_fingerprint(stun_codec:encode(Msg)),
                    _ = send(St, Wire),
                    St;
                false ->
                    St
            end
    end.

%%%_* Permissions -----------------------------------------------------------

permission_valid(Ip, #st{permissions = Perms}) ->
    case maps:find(Ip, Perms) of
        {ok, Expiry} -> Expiry > now_ms();
        error -> false
    end.

now_ms() ->
    erlang:monotonic_time(millisecond).

%%%_* Responses -------------------------------------------------------------

%% `Key' is `undefined' for an error response: a client whose credential we
%% just rejected cannot verify a digest, and RFC 8489 §14.5 says an error
%% response carries MESSAGE-INTEGRITY only when the request authenticated.
respond(Method, Class, Tid, Attrs, undefined) ->
    stun_codec:add_fingerprint(
      stun_codec:encode(#{method => Method, class => Class,
                          transaction_id => Tid, attributes => Attrs}));
respond(Method, Class, Tid, Attrs, Key) ->
    Base = stun_codec:encode(#{method => Method, class => Class,
                               transaction_id => Tid, attributes => Attrs}),
    stun_codec:add_fingerprint(stun_codec:add_integrity(Key, Base)).

%% RFC 8489 §7.3.3: the error response echoes the method of the request that
%% was refused, and never carries MESSAGE-INTEGRITY (the client cannot verify
%% one for a credential we just rejected). The allocation's transaction id is
%% what the client matches on.
reply_error(Code, #{method := Method, transaction_id := Tid}, St) ->
    Attrs = [{stun_codec:attribute_type(error_code),
              stun_codec:error_code(Code div 100, Code rem 100)}],
    _ = send(St, respond(Method, error, Tid, Attrs, undefined)),
    St.

send(#st{main_socket = Socket, client = {Ip, Port}}, Wire) ->
    _ = gen_udp:send(Socket, Ip, Port, Wire),
    ok.
