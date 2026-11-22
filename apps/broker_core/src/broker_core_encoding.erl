%%% @doc Pure encoding utilities shared across protocol adapters.
%%%
%%% `broker_core' is otherwise just the frozen message model and public types,
%%% but these are stateless helpers that multiple adapters need identically.
%%% Adding them here avoids copy-paste drift in security-sensitive wire formats
%%% (A2A task IDs, MCP correlation IDs, etc).
-module(broker_core_encoding).

-export([base64url/1]).

%% @doc Base64url encode (RFC 4648 §5): `+' -> `-', `/' -> `_', strip `='.
%% No padding added back; decode must accept both padded and unpadded.
-spec base64url(binary()) -> binary().
base64url(Bin) ->
    Encoded = base64:encode(Bin),
    iolist_to_binary([url_char(B) || <<B/integer>> <= Encoded, B =/= $=]).

url_char($+) -> $-;
url_char($/) -> $_;
url_char(B) -> B.
