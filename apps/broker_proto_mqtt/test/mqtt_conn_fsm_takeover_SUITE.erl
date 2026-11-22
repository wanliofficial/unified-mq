%% @doc conn_fsm takeover 判定的可测纯逻辑（Plan 2 T5 review）。
%% claim/epoch 路径很薄（依赖 ra），这里测真正被复用、纯的判定谓词 takeover_target/2。
%% 端到端 nodedown→claim→续传由后续 dist-CT 覆盖。
-module(mqtt_conn_fsm_takeover_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [matches_own_owner_death,
          ignores_other_node_death,
          ignores_when_no_owner].

init_per_testcase(_C, C) -> C.
end_per_testcase(_C, _Cfg) -> ok.

%% 记录的 owner 正是死亡节点 → 本连接该发起 claim。
matches_own_owner_death(_C) ->
    ?assert(mqtt_conn_fsm:takeover_target(owner@host, owner@host)),
    ok.

%% 死亡的是别的节点 → 与本连接无关。
ignores_other_node_death(_C) ->
    ?assertNot(mqtt_conn_fsm:takeover_target(owner@host, other@host)),
    ok.

%% 尚未记录 owner（开关关 / 本机 owner）→ 不发起 claim。
ignores_when_no_owner(_C) ->
    ?assertNot(mqtt_conn_fsm:takeover_target(undefined, some@host)),
    ok.
