%% @doc S2-G: the pure backpressure decisions (design §4.7, D5).
%%
%% The frozen decision is "observable + bounded", not "fair": over a threshold
%% the broker either *pauses* the consumer's pull round or *rejects* the
%% producer's publish, and counts it. Keeping the arithmetic here -- instead of
%% inline in the connection FSM -- is what makes the boundary testable without
%% a socket, a storage writer or a running metrics app.
%%
%% Two rules are easy to get wrong and are pinned by the suite:
%%
%%  * `0' disables a mailbox limit, but an in-flight limit of `0' still means
%%    "no deliveries" (it is the window size, not a switch). The FSM has always
%%    read it that way, and changing it here would silently start delivering.
%%  * the consumer checks the mailbox FIRST: a process whose inbox is already
%%    over the limit is not helped by one more page, no matter how empty its
%%    in-flight window looks.
-module(mqtt_backpressure).

-export([consumer/3, producer/2]).

-export_type([verdict/0]).

-type verdict() :: ok
                 | {pause, mailbox | inflight}
                 | {reject, mailbox}.

%% @doc Should this pull round be skipped? `MailboxLen' is the connection's own
%% message queue length, `Inflight' the number of deliveries it has not yet had
%% acknowledged. `MailboxLimit = 0' disables the mailbox check.
-spec consumer(non_neg_integer(), non_neg_integer(),
               #{mailbox := non_neg_integer(), inflight := non_neg_integer()}) ->
          ok | {pause, mailbox | inflight}.
consumer(MailboxLen, Inflight, #{mailbox := MaxMailbox, inflight := MaxInflight})
  when is_integer(MailboxLen), MailboxLen >= 0,
       is_integer(Inflight), Inflight >= 0,
       is_integer(MaxMailbox), MaxMailbox >= 0,
       is_integer(MaxInflight), MaxInflight >= 0 ->
    if
        MaxMailbox > 0, MailboxLen >= MaxMailbox ->
            {pause, mailbox};
        Inflight >= MaxInflight ->
            {pause, inflight};
        true ->
            ok
    end.

%% @doc Should this inbound PUBLISH be refused? MQTT 3.1.1 has no quota frame,
%% so "refused" means "close without an ack" at the call site.
-spec producer(non_neg_integer(), non_neg_integer()) -> ok | {reject, mailbox}.
producer(_MailboxLen, 0) ->
    ok;
producer(MailboxLen, MaxMailbox)
  when is_integer(MailboxLen), MailboxLen >= 0,
       is_integer(MaxMailbox), MaxMailbox > 0 ->
    case MailboxLen >= MaxMailbox of
        true -> {reject, mailbox};
        false -> ok
    end.
