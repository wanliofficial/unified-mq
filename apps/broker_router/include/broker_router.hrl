%% Frozen by ADR-010 §10.1 (2026-10-21). The record shape is the published
%% contract; callers (protocol adapters, broker_queue) construct it, so it lives
%% in a header rather than only inside broker_router.erl -- a record defined in
%% a module file cannot be used by any other module.
%%
%% Include broker_core/include/broker_message.hrl BEFORE this file: the field
%% annotations below reuse the ADR-009 types principal()/sync_mode().

-record(route_ctx, {
    tenant_id   :: binary(),
    publisher   :: principal() | undefined,   % ADR-009
    reliability :: sync_mode()                % ADR-007 / ADR-009
}).
-type route_ctx() :: #route_ctx{}.
