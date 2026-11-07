%% @doc CloudEvents 1.0 decoded view (README §3.1) -- the interface slot T11
%% reserves for S3's codec, not a structure S1 ever fills.
%%
%% Two facts S3 must reconcile, recorded here so it does not start from a
%% wrong assumption:
%%
%%   * `#broker_message{}.event' is frozen by ADR-009 §9.1 as
%%     `undefined | map()'. This record is therefore NOT what goes into that
%%     slot as-is: S3 either maps this record into that map or files an
%%     ADR-009 amendment. Deciding that is S3 work, deliberately not pre-empted
%%     here.
%%   * `created_at' (broker receive time) and `time' (event occurrence time)
%%     are different clocks and stay separate (README §4.1, ADR-005).
%%
%% Field names and defaults mirror README §3.1 exactly; the only additions are
%% type annotations. `data' / `data_base64' are mutually exclusive per the
%% spec -- S1 does not enforce that because S1 never constructs one.
-record(cloud_event, {
    specversion     = <<"1.0">> :: binary(),
    id                          :: binary(),
    source                      :: binary(),
    type                        :: binary(),
    subject         = undefined :: binary() | undefined,
    time            = undefined :: binary() | undefined,   % RFC3339 string
    datacontenttype = undefined :: binary() | undefined,
    dataschema      = undefined :: binary() | undefined,
    extensions      = #{}       :: #{binary() => term()},
    data            = undefined :: term(),
    data_base64     = undefined :: binary() | undefined
}).
