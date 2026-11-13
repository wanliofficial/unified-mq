%% @doc STUN/TURN UDP front end (ADR-029 §D2): one socket on 3478, plus the
%% allocation index that gives each client 5-tuple its own relay process.
%%
%% This process owns exactly one thing: the listening socket. Relay sockets
%% belong to `turn_allocation' processes, so a busy relay never blocks the
%% shared socket's receive loop. A datagram is classified by its first two
%% bits:
%%
%%   * `00' -- STUN. Binding is answered here; every TURN method except
%%     Allocate is forwarded to the allocation that owns the client 5-tuple.
%%   * `01' -- TURN ChannelData (`<<Channel:16, Length:16, Payload>>'), sent by
%%     a client that has completed ChannelBind. Forwarded on the same way.
%%
%% Allocation ownership is a map + monitor: the allocation process is a
%% `temporary' child of `turn_allocation_sup', and the DOWN message is what
%% frees its entry. A restarted listener (one_for_all) loses the index and
%% therefore every allocation -- clients re-Allocate, which is the honest
%% recovery for a single-node UDP relay (ADR-029 §D5).
%%
%% Every handler below returns the next state; `handle_info/2' is the only
%% place that builds `{noreply, _}'.
-module(broker_ice_udp).

-behaviour(gen_server).

-export([start_link/1, port/0, allocations/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

%% Channel numbers occupy the top half of the 16-bit space (RFC 5766 §11).
-define(CHANNEL_MIN, 16#4000).
-define(CHANNEL_MAX, 16#7FFF).
-define(TURN_TRANSPORT_UDP, 17).
%% The allocator tries at most this many candidate ports per request; a full
%% range answers 508 instead of scanning 16k ports on every failed bind.
-define(PORT_ATTEMPTS, 8).

-record(st, {socket :: gen_udp:socket(),
             turn_ip :: inet:ip_address(),
             realm :: binary(),
             secret :: binary(),
             relay_min :: inet:port_number(),
             relay_max :: inet:port_number(),
             next_port :: inet:port_number(),
             max_allocations :: pos_integer(),
             allocation_lifetime_s :: pos_integer(),
             max_permissions :: pos_integer(),
             max_channels :: pos_integer(),
             %% {ClientIp, ClientPort, udp} => #{pid, relay_port, username}
             allocations = #{} :: map(),
             %% monitor ref => allocation key
             monitors = #{} :: map()}).

%%%_* API -------------------------------------------------------------------

start_link(Cfg) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Cfg, []).

%% @doc The port the kernel actually bound (CT sets `port => 0').
-spec port() -> inet:port_number().
port() ->
    gen_server:call(?MODULE, port).

%% @doc A snapshot for tests and operators: client key, relay port, username.
-spec allocations() -> [map()].
allocations() ->
    gen_server:call(?MODULE, allocations).

%%%_* Lifecycle -------------------------------------------------------------

init(Cfg) ->
    Port = maps:get(port, Cfg),
    {RelayMin, RelayMax} = maps:get(relay_port_range, Cfg),
    Options = [binary, {active, once}, {reuseaddr, true},
               {recbuf, maps:get(recbuf_bytes, Cfg)},
               {sndbuf, maps:get(recbuf_bytes, Cfg)}],
    case gen_udp:open(Port, Options) of
        {ok, Socket} ->
            {ok, RealPort} = inet:port(Socket),
            ?LOG_INFO("broker_ice: STUN/TURN on udp ~p (relay ~p-~p)",
                      [RealPort, RelayMin, RelayMax]),
            {ok, #st{socket = Socket,
                     turn_ip = maps:get(turn_ip, Cfg),
                     realm = maps:get(realm, Cfg),
                     secret = maps:get(nonce_secret, Cfg),
                     relay_min = RelayMin,
                     relay_max = RelayMax,
                     next_port = RelayMin,
                     max_allocations = maps:get(max_allocations, Cfg),
                     allocation_lifetime_s =
                         maps:get(allocation_lifetime_s, Cfg),
                     max_permissions = maps:get(max_permissions, Cfg),
                     max_channels = maps:get(max_channels, Cfg)}};
        {error, Reason} ->
            {stop, {udp_open_failed, Port, Reason}}
    end.

terminate(_Reason, #st{socket = Socket}) ->
    _ = gen_udp:close(Socket),
    ok.

handle_call(port, _From, #st{} = St) ->
    {reply, gen_udp_port(St), St};
handle_call(allocations, _From, #st{allocations = Allocs} = St) ->
    {reply, [#{client => Key, relay_port => maps:get(relay_port, A),
               username => maps:get(username, A)}
             || {Key, A} <- maps:to_list(Allocs)], St};
handle_call(_Other, _From, St) ->
    {reply, {error, unknown_call}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% Re-arm after every datagram: `{active, once}' is the back-pressure knob that
%% keeps a flood from growing this process's mailbox without bound.
handle_info({udp, Socket, Ip, Port, Bin}, #st{socket = Socket} = St) ->
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, handle_datagram(Bin, {Ip, Port}, St)};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, St) ->
    {noreply, forget_allocation(Ref, St)};
handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Datagram dispatch -----------------------------------------------------

handle_datagram(<<Channel:16, Len:16, Rest/binary>>, Peer, St)
  when Channel >= ?CHANNEL_MIN, Channel =< ?CHANNEL_MAX ->
    case Rest of
        <<Data:Len/binary, _Padding/binary>> ->
            route_channel(Channel, Data, Peer, St);
        _ ->
            St
    end;
handle_datagram(Bin, Peer, St) ->
    case stun_codec:decode(Bin) of
        {ok, Msg} -> handle_stun(Msg, Bin, Peer, St);
        {error, _Reason} -> St
    end.

handle_stun(Msg, Wire, Peer, St) ->
    case stun_codec:unknown_required(maps:get(attributes, Msg)) of
        [] ->
            handle_stun_1(Msg, Wire, Peer, St);
        Unknown ->
            Types = << <<T:16>> || T <- Unknown >>,
            reply_error(420, Msg, Peer, St,
                        [{stun_codec:attribute_type(unknown_attributes), Types}])
    end.

handle_stun_1(#{class := request, method := binding} = Msg, _Wire, Peer, St) ->
    reply_binding(Msg, Peer, St);
handle_stun_1(#{class := request, method := allocate} = Msg, Wire, Peer, St) ->
    handle_allocate(Msg, Wire, Peer, St);
handle_stun_1(#{class := request, method := Method} = Msg, Wire, Peer, St)
  when Method =:= refresh; Method =:= create_permission;
       Method =:= channel_bind ->
    forward_request(Method, Msg, Wire, Peer, St);
handle_stun_1(#{class := indication, method := send} = Msg, _Wire, Peer, St) ->
    forward_send(Msg, Peer, St);
handle_stun_1(#{class := request} = Msg, _Wire, Peer, St) ->
    reply_error(400, Msg, Peer, St, []);
handle_stun_1(_Msg, _Wire, _Peer, St) ->
    %% Responses never arrive here (we are not a client) and indications other
    %% than Send are not part of TURN's UDP surface: drop.
    St.

%%%_* STUN Binding ----------------------------------------------------------

%% Binding is deliberately unauthenticated: it is the reflexive-address probe
%% a browser sends before it has any credential, and it reveals nothing but the
%% source address the packet already carried.
reply_binding(#{transaction_id := Tid}, {Ip, Port} = Peer, St) ->
    Mapped = stun_codec:xor_mapped_address(Ip, Port, Tid),
    Attrs = [{stun_codec:attribute_type(xor_mapped_address), Mapped}],
    _ = send(St, Peer, respond(binding, success, Tid, Attrs, undefined)),
    St.

%%%_* Allocate --------------------------------------------------------------

handle_allocate(Msg, Wire, Peer, St) ->
    case requested_transport(Msg) of
        ?TURN_TRANSPORT_UDP ->
            handle_allocate_auth(Msg, Wire, Peer, St);
        _ ->
            reply_error(442, Msg, Peer, St, [])
    end.

requested_transport(#{attributes := Attrs}) ->
    stun_codec:requested_transport(
      stun_codec:attribute(Attrs, stun_codec:attribute_type(requested_transport))).

handle_allocate_auth(Msg, Wire, Peer, St) ->
    Cfg = #{realm => St#st.realm, secret => St#st.secret,
            action => turn_allocate, destination => <<"$ice/turn">>},
    case turn_auth:authenticate(Msg, Wire, Cfg) of
        {ok, #{principal := User, key := Key}} ->
            allocate(Msg, Peer, User, Key, St);
        {error, {Code, _Reason}} ->
            reply_error(Code, Msg, Peer, St, challenge_attrs(Code, St))
    end.

allocate(#{transaction_id := Tid} = Msg, {Ip, Port} = Peer, User, Key, St) ->
    ClientKey = {Ip, Port, udp},
    if
        is_map_key(ClientKey, St#st.allocations) ->
            %% RFC 5766 §6.2: a second Allocate on the same 5-tuple is a
            %% mismatch, not a new allocation (the client must Refresh).
            reply_error(437, Msg, Peer, St, []);
        map_size(St#st.allocations) >= St#st.max_allocations ->
            reply_error(486, Msg, Peer, St, []);
        true ->
            allocate_1(ClientKey, Tid, Msg, Peer, User, Key, St)
    end.

allocate_1(ClientKey, Tid, Msg, {Ip, Port} = Peer, User, Key, St) ->
    case start_allocation(ClientKey, User, Key, Peer, St) of
        {ok, Pid, RelayPort, St1} ->
            Mapped = stun_codec:xor_mapped_address(Ip, Port, Tid),
            RelayAddr = stun_codec:xor_address(St1#st.turn_ip, RelayPort, Tid),
            Lifetime = St1#st.allocation_lifetime_s,
            Attrs = [{stun_codec:attribute_type(xor_relayed_address), RelayAddr},
                     {stun_codec:attribute_type(xor_mapped_address), Mapped},
                     {stun_codec:attribute_type(lifetime),
                      <<Lifetime:32>>}],
            _ = send(St1, Peer, respond(allocate, success, Tid, Attrs, Key)),
            Ref = erlang:monitor(process, Pid),
            Allocs = maps:put(ClientKey,
                              #{pid => Pid, relay_port => RelayPort,
                                username => User}, St1#st.allocations),
            St1#st{allocations = Allocs, next_port = next_port(RelayPort, St1),
                   monitors = maps:put(Ref, ClientKey, St1#st.monitors)};
        {error, Code, _Reason} ->
            reply_error(Code, Msg, Peer, St, [])
    end.

start_allocation(ClientKey, User, Key, Peer, St) ->
    Candidates = candidate_ports(St),
    try_ports(Candidates, 0, ClientKey, User, Key, Peer, St).

try_ports(_Ports, Tried, _ClientKey, _User, _Key, _Peer, _St)
  when Tried >= ?PORT_ATTEMPTS ->
    {error, 508, <<"Insufficient Capacity">>};
try_ports([], _Tried, _ClientKey, _User, _Key, _Peer, _St) ->
    {error, 508, <<"Insufficient Capacity">>};
try_ports([Port | Rest], Tried, ClientKey, User, Key, Peer, St) ->
    Opts = #{relay_port => Port,
             advertised_ip => St#st.turn_ip,
             %% Responses to the client go out of the *listening* socket: the
             %% client's NAT/firewall has already seen that 5-tuple, and a
             %% reply from the relay port would look like a new flow.
             main_socket => St#st.socket,
             client => Peer,
             username => User,
             realm => St#st.realm,
             secret => St#st.secret,
             key => Key,
             lifetime_s => St#st.allocation_lifetime_s,
             max_permissions => St#st.max_permissions,
             max_channels => St#st.max_channels},
    case turn_allocation_sup:start_allocation(Opts) of
        {ok, Pid} ->
            {ok, Pid, Port, St};
        {error, eaddrinuse} ->
            try_ports(Rest, Tried + 1, ClientKey, User, Key, Peer, St);
        {error, {shutdown, {failed_to_start_child, _, eaddrinuse}}} ->
            try_ports(Rest, Tried + 1, ClientKey, User, Key, Peer, St);
        {error, _Reason} ->
            {error, 508, <<"Insufficient Capacity">>}
    end.

%% Round-robin from the last handed-out port so a churning deployment does not
%% allocate the same low port over and over.
candidate_ports(#st{relay_min = Min, relay_max = Max, next_port = Next,
                    allocations = Allocs}) ->
    Used = maps:fold(fun(_K, #{relay_port := P}, Acc) ->
                             sets:add_element(P, Acc)
                     end, sets:new(), Allocs),
    [P || P <- wrap_ports(Next, Min, Max), not sets:is_element(P, Used)].

wrap_ports(Next, Min, Max) when Next > Max; Next < Min ->
    lists:seq(Min, Max);
wrap_ports(Next, Min, Max) ->
    lists:seq(Next, Max) ++ lists:seq(Min, Next - 1).

next_port(Port, #st{relay_min = Min, relay_max = Max}) when Port >= Max ->
    Min;
next_port(Port, _St) ->
    Port + 1.

%%%_* Allocation routing ----------------------------------------------------

forward_request(Method, Msg, Wire, Peer, St) ->
    case find_allocation(peer_key(Peer), St) of
        {ok, Pid} ->
            turn_allocation:request(Pid, Method, Msg, Wire),
            St;
        error ->
            reply_error(437, Msg, Peer, St, [])
    end.

forward_send(Msg, Peer, St) ->
    case find_allocation(peer_key(Peer), St) of
        {ok, Pid} -> turn_allocation:send_indication(Pid, Msg), St;
        error -> St
    end.

route_channel(Channel, Data, Peer, St) ->
    case find_allocation(peer_key(Peer), St) of
        {ok, Pid} -> turn_allocation:channel_data(Pid, Channel, Data), St;
        error -> St
    end.

peer_key({Ip, Port}) -> {Ip, Port, udp}.

find_allocation(Key, #st{allocations = Allocs}) ->
    case maps:find(Key, Allocs) of
        {ok, #{pid := Pid}} -> {ok, Pid};
        error -> error
    end.

forget_allocation(Ref, #st{monitors = Monitors, allocations = Allocs} = St) ->
    case maps:take(Ref, Monitors) of
        {Key, Rest} ->
            St#st{monitors = Rest, allocations = maps:remove(Key, Allocs)};
        error ->
            St
    end.

%%%_* Responses -------------------------------------------------------------

%% A response always ends with FINGERPRINT. `Key' is `undefined' for STUN
%% Binding and for errors (401/438 carry REALM + NONCE instead); every
%% authenticated TURN success carries MESSAGE-INTEGRITY.
respond(Method, Class, Tid, Attrs, Key) ->
    Base = stun_codec:encode(#{method => Method, class => Class,
                               transaction_id => Tid, attributes => Attrs}),
    Signed = case Key of
                 undefined -> Base;
                 _ -> stun_codec:add_integrity(Key, Base)
             end,
    stun_codec:add_fingerprint(Signed).

%% RFC 8489 §7.3.3: an error response carries the *same* method as the request
%% it refuses, so the client can match it against the transaction it started.
%% (Answering a refused Allocate with a Binding error is what makes a naive
%% client retry forever.)
reply_error(Code, #{method := Method, transaction_id := Tid}, Peer, St, Extra) ->
    Attrs = [{stun_codec:attribute_type(error_code),
              stun_codec:error_code(Code div 100, Code rem 100)} | Extra],
    _ = send(St, Peer, respond(Method, error, Tid, Attrs, undefined)),
    St.

challenge_attrs(401, St) ->
    [{stun_codec:attribute_type(realm), St#st.realm},
     {stun_codec:attribute_type(nonce), fresh_nonce(St)}];
challenge_attrs(438, St) ->
    [{stun_codec:attribute_type(realm), St#st.realm},
     {stun_codec:attribute_type(nonce), fresh_nonce(St)}];
challenge_attrs(_Code, _St) ->
    [].

fresh_nonce(#st{secret = Secret}) ->
    turn_auth:nonce(Secret, erlang:system_time(second)).

send(#st{socket = Socket}, {Ip, Port}, Wire) ->
    _ = gen_udp:send(Socket, Ip, Port, Wire),
    ok.

gen_udp_port(#st{socket = Socket}) ->
    {ok, Port} = inet:port(Socket),
    Port.
