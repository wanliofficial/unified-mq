%% Frozen by ADR-009 §9.1 (2026-10-21). Changing any field or field type is a
%% breaking change for already-written WAL segments and every protocol adapter:
%% it requires an ADR revision, not a silent edit here.

-type partition_id() :: non_neg_integer().
-type offset()       :: non_neg_integer().
-type principal()    :: binary().
-type sync_mode()    :: os_buffer | fsync | replicated_commit.

%% Slots that exist but are NOT active in S1 (ADR-009 §9.3):
%%   headers -> always #{}   event -> always undefined
%%   expires_at -> always undefined for BOTH persistence classes. Never derive a
%%     placeholder TTL here: it would land in an immutable WAL segment and let
%%     S2's TTL drop already-acknowledged messages.
%%   replicated_commit -> in no S1 backend's capability/1.
-record(broker_message, {
    message_id        :: binary(),
    tenant_id         :: binary(),
    destination       :: binary(),
    partition_id      :: partition_id(),
    payload           :: binary(),
    payload_encoding  :: raw | json | binary | text,
    headers           :: map(),
    proto_meta        :: map(),
    persistence       :: transient | durable,
    reliability       :: sync_mode(),
    created_at        :: integer(),
    expires_at        :: integer() | undefined,
    event             :: undefined | map()
}).
