%% @doc Storage behavior: one append-only log per partition.
%% Frozen by ADR-011 §11.1 (2026-10-21).
%%
%% create/2 and recover/2 are deliberately separate: recover/2 must never
%% auto-create, otherwise a mistyped partition id or a lost mount shows up as a
%% healthy empty partition, which is silent data loss.
-module(broker_storage).

-include_lib("broker_core/include/broker_message.hrl").

-export_type([handle/0]).

-type handle() :: term().

-callback create(PartitionId :: partition_id(), Config :: map()) ->
    {ok, handle()} | {error, {already_exists, partition_id()}} | {error, term()}.

%% CommittedOffset is the global ack watermark (continuous prefix);
%% HighOffset is where the next append goes.
-callback recover(PartitionId :: partition_id(), Config :: map()) ->
    {ok, handle(), CommittedOffset :: offset(), HighOffset :: offset()}
      | {error, {not_found, partition_id()}} | {error, term()}.

-callback close(handle()) -> ok.

-callback append(handle(), [#broker_message{}], sync_mode()) ->
    {ok, [offset()]} | {error, {unsupported_sync_mode, sync_mode()}} | {error, term()}.

-callback read_from(handle(), FromOffset :: offset(), Limit :: pos_integer()) ->
    {ok, [{offset(), #broker_message{}}], NextOffset :: offset()} | {error, term()}.

%% Storage owns only the global committed watermark; per-subscription cursors
%% belong to broker_subscription. May regress under crash (duplicates allowed),
%% must never run ahead (would let truncate/2 discard unacked data).
-callback advance_committed(handle(), Offset :: offset()) -> ok | {error, term()}.

%% Logical unreadable boundary; never deletes files in S1.
-callback truncate(handle(), BeforeOffset :: offset()) -> ok | {error, term()}.

%% Instance level, so one backend can expose different capabilities on
%% different disks or durability settings.
-callback capability(handle()) -> [sync_mode()].
