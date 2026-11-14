%% @doc A byte-level STUN/TURN client for the `broker_ice' suites.
%%
%% Deliberately hand-rolled on top of `stun_codec' instead of reusing server
%% code: if a suite built its request with the same helper the server uses to
%% answer, a shared mistake would cancel out and the test would prove nothing.
%% Everything here is the shape RFC 5766 §6-§11 describes a client doing:
%%
%%   1. ask unauthenticated (Allocate with no MESSAGE-INTEGRITY);
%%   2. read REALM + NONCE out of the 401 challenge;
%%   3. re-ask with USERNAME / REALM / NONCE / MESSAGE-INTEGRITY;
%%   4. sign *every* later request (Refresh, CreatePermission, ChannelBind) the
%%      same way -- a TURN server rejects one that omits the credential, and a
%%      helper that quietly omitted it would hide that rule from every suite.
%%
%% `allocate/3' therefore returns a context map, and all the later helpers take
%% it. The context keeps the derived key, which is why no test needs the
%% password after the initial exchange.
-module(ice_test_client).

-export([open/0, close/1, port/1,
         binding/2, binding/3,
         allocate/3, allocate_with/4,
         request/5, refresh/4, create_permission/4,
         channel_bind/5, channel_data/4, send_indication/4,
         recv/1, recv/2, recv_data/1, recv_data/2,
         key/3, sign/2, xaddr/2, transaction_id/0]).

-define(TIMEOUT, 3000).
-define(HOST, {127, 0, 0, 1}).

%%%_* Sockets ---------------------------------------------------------------

open() ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    Socket.

close(Socket) ->
    _ = gen_udp:close(Socket),
    ok.

port(Socket) ->
    {ok, Port} = inet:port(Socket),
    Port.

%%%_* Simple transactions ---------------------------------------------------

binding(Socket, ServerPort) ->
    binding(Socket, ServerPort, <<"127.0.0.1">>).

binding(Socket, ServerPort, Host) ->
    Tid = transaction_id(),
    Msg = #{method => binding, class => request, transaction_id => Tid,
            attributes => []},
    send_wire(Socket, {host(Host), ServerPort},
              stun_codec:add_fingerprint(stun_codec:encode(Msg))),
    await(Socket, Tid).

%% @doc The full TURN credential dance. Returns a context (relay address, key,
%% realm, nonce) that the authenticated helpers below need.
allocate(Socket, ServerPort, #{username := User, password := Password}) ->
    allocate_with(Socket, ServerPort,
                  #{username => User, password => Password}, []).

allocate_with(Socket, ServerPort, #{username := User, password := Password},
              ExtraAttrs) ->
    Transport = {stun_codec:attribute_type(requested_transport),
                 <<17, 0, 0, 0>>},
    case transact(Socket, ServerPort, allocate, [Transport | ExtraAttrs],
                  none) of
        {error, 401, Challenge} ->
            Attrs = maps:get(attributes, Challenge),
            Realm = stun_codec:attribute(Attrs, stun_codec:attribute_type(realm)),
            Nonce = stun_codec:attribute(Attrs, stun_codec:attribute_type(nonce)),
            Key = key(User, Realm, Password),
            Ctx = #{username => User, realm => Realm, nonce => Nonce,
                    key => Key},
            case transact(Socket, ServerPort, allocate,
                          [Transport | ExtraAttrs], Ctx) of
                {ok, Msg} -> {ok, allocation(Msg, Ctx)};
                Other -> Other
            end;
        Other ->
            Other
    end.

allocation(Msg, Ctx) ->
    Attrs = maps:get(attributes, Msg),
    {ok, {RelayIp, RelayPort}} = stun_codec:decode_xor_address(
                                   attribute(Attrs, xor_relayed_address)),
    {ok, {MappedIp, MappedPort}} = stun_codec:decode_xor_address(
                                      attribute(Attrs, xor_mapped_address)),
    Ctx#{relay_ip => RelayIp, relay_port => RelayPort,
         mapped_ip => MappedIp, mapped_port => MappedPort,
         lifetime => stun_codec:lifetime(attribute(Attrs, lifetime))}.

refresh(Socket, ServerPort, Ctx, Lifetime) ->
    Attrs = [{stun_codec:attribute_type(lifetime), <<Lifetime:32>>}],
    transact(Socket, ServerPort, refresh, Attrs, Ctx).

create_permission(Socket, ServerPort, Ctx, Peers) ->
    Tid = maps:get(tid, Ctx, transaction_id()),
    Attrs = [{stun_codec:attribute_type(xor_peer_address), xaddr(Peer, Tid)}
             || Peer <- Peers],
    transact(Socket, ServerPort, create_permission, Attrs, Ctx, Tid).

channel_bind(Socket, ServerPort, Ctx, Channel, Peer) ->
    Tid = transaction_id(),
    Attrs = [{stun_codec:attribute_type(channel_number),
              stun_codec:channel_number(Channel)},
             {stun_codec:attribute_type(xor_peer_address), xaddr(Peer, Tid)}],
    transact(Socket, ServerPort, channel_bind, Attrs, Ctx, Tid).

%% ChannelData is not a STUN message: it is a 4-byte header plus the payload,
%% and it carries no credential -- the channel binding is the credential.
channel_data(Socket, ServerPort, Channel, Data) ->
    gen_udp:send(Socket, ?HOST, ServerPort,
                 <<Channel:16, (byte_size(Data)):16, Data/binary>>).

%% A Send indication is never authenticated (RFC 5766 §10): it has no response,
%% so there would be nobody to challenge.
send_indication(Socket, ServerPort, Peer, Data) ->
    Tid = transaction_id(),
    Attrs = [{stun_codec:attribute_type(xor_peer_address), xaddr(Peer, Tid)},
             {stun_codec:attribute_type(data), Data}],
    Msg = #{method => send, class => indication, transaction_id => Tid,
            attributes => Attrs},
    gen_udp:send(Socket, ?HOST, ServerPort,
                 stun_codec:add_fingerprint(stun_codec:encode(Msg))).

%%%_* Receiving -------------------------------------------------------------

recv(Socket) ->
    recv(Socket, ?TIMEOUT).

recv(Socket, Timeout) ->
    case gen_udp:recv(Socket, 0, Timeout) of
        {ok, {_Ip, _Port, Bin}} -> {ok, Bin};
        {error, timeout} -> timeout
    end.

%% @doc Wait for one relayed payload: either ChannelData or a Data indication.
recv_data(Socket) ->
    recv_data(Socket, ?TIMEOUT).

recv_data(Socket, Timeout) ->
    case recv(Socket, Timeout) of
        timeout ->
            timeout;
        {ok, <<Channel:16, Len:16, Rest/binary>>} when Channel >= 16#4000 ->
            case Rest of
                <<Data:Len/binary, _/binary>> ->
                    {ok, {channel_data, Channel, Data}};
                _ ->
                    {error, truncated_channel_data}
            end;
        {ok, Bin} ->
            case stun_codec:decode(Bin) of
                {ok, #{method := data, class := indication,
                       attributes := Attrs}} ->
                    {ok, {data_indication,
                          stun_codec:attribute(
                            Attrs, stun_codec:attribute_type(data))}};
                {ok, Other} ->
                    {ok, {stun, Other}};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%%%_* Wire helpers ----------------------------------------------------------

%% @doc One authenticated (or, with `none', unauthenticated) request with a
%% caller-built attribute list. Suites use this when they need to send
%% something the typed helpers would reject client-side -- a channel number
%% outside 0x4000-0x7FFF, say -- so the server's refusal is what is tested.
request(Socket, ServerPort, Method, Attrs, Ctx) ->
    transact(Socket, ServerPort, Method, Attrs, Ctx).

transact(Socket, ServerPort, Method, Attrs, Ctx) ->
    transact(Socket, ServerPort, Method, Attrs, Ctx, transaction_id()).

%% One request/response exchange, matched on the transaction id. An error class
%% comes back as `{error, Code, Msg}' so a suite asserts on the code without
%% decoding ERROR-CODE itself.
transact(Socket, ServerPort, Method, Attrs, Ctx, Tid) ->
    %% An authenticated request repeats USERNAME / REALM / NONCE and closes
    %% with MESSAGE-INTEGRITY; an unauthenticated one carries neither. Building
    %% the attribute list here (rather than splicing bytes afterwards) keeps the
    %% message the digest covers identical to the message on the wire.
    {AllAttrs, Sign} = with_credentials(Attrs, Ctx),
    Msg = #{method => Method, class => request, transaction_id => Tid,
            attributes => AllAttrs},
    send_wire(Socket, {?HOST, ServerPort},
              sign(Sign, stun_codec:encode(Msg))),
    await(Socket, Tid).

with_credentials(Attrs, none) ->
    {Attrs, none};
with_credentials(Attrs, #{username := User, realm := Realm, nonce := Nonce,
                          key := Key}) ->
    {Attrs ++ [{stun_codec:attribute_type(username), User},
               {stun_codec:attribute_type(realm), Realm},
               {stun_codec:attribute_type(nonce), Nonce}],
     {key, Key}}.

await(Socket, Tid) ->
    case recv(Socket) of
        timeout ->
            timeout;
        {ok, Bin} ->
            case stun_codec:decode(Bin) of
                {ok, #{transaction_id := Tid} = Msg} ->
                    classify(Msg);
                _ ->
                    %% A stale reply from an earlier case: keep waiting rather
                    %% than failing on it.
                    await(Socket, Tid)
            end
    end.

classify(#{class := success} = Msg) ->
    {ok, Msg};
classify(#{class := error, attributes := Attrs} = Msg) ->
    {ok, Code, _Reason} = stun_codec:decode_error_code(
                            stun_codec:attribute(
                              Attrs, stun_codec:attribute_type(error_code))),
    {error, Code, Msg};
classify(Msg) ->
    {unexpected, Msg}.

%% `none' -- no credential: FINGERPRINT closes the message.
%% `{key, Key}' -- MESSAGE-INTEGRITY after the attributes, then FINGERPRINT,
%% which is the order RFC 8489 §14.5/§14.7 requires.
sign(none, Wire) ->
    stun_codec:add_fingerprint(Wire);
sign({key, Key}, Wire) ->
    stun_codec:add_fingerprint(stun_codec:add_integrity(Key, Wire)).

send_wire(Socket, {Ip, Port}, Wire) ->
    gen_udp:send(Socket, Ip, Port, Wire).

attribute(Attrs, Name) ->
    stun_codec:attribute(Attrs, stun_codec:attribute_type(Name)).

%% RFC 5766 §4.
key(Username, Realm, Password) ->
    crypto:hash(md5, [Username, <<":">>, Realm, <<":">>, Password]).

xaddr({Ip, Port}, Tid) ->
    stun_codec:xor_address(Ip, Port, Tid).

transaction_id() ->
    crypto:strong_rand_bytes(12).

host(Bin) ->
    {ok, Ip} = inet:parse_address(binary_to_list(Bin)),
    Ip.
