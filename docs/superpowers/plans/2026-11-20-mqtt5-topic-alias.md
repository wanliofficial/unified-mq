# MQTT 5.0 Topic Alias 实现计划（MQ-04）

> 配套 spec：`docs/superpowers/specs/2026-11-20-mqtt5-topic-alias-design.md`
> 全程 TDD：每个 Task 先写失败测试、跑红、再实现、跑绿、提交。

## Task 0：建隔离工作区 + feature 分支

- [ ] **Step 1**：确认当前在 `main`、工作树干净、`main` 与 `origin/main` 同步（`git status -sb`）。
- [ ] **Step 2**：用平台工具建 worktree `mqtt5-topic-alias`（off main），移动会话进去，
      建并切到 `mqtt5-topic-alias` 分支。
- [ ] **Step 3**：确认 `docs/superpowers/specs/2026-11-20-mqtt5-topic-alias-design.md` 在该分支可见。

## Task 1：CONNACK 宣布 topic_alias_maximum

**Files**：`src/mqtt_conn_fsm.erl`（`#st{}` 字段、`handle_connect`、`connack_bin/3`）、`test/mqtt_conn_SUITE.erl`

- [ ] **Step 1（红）**：`all()` 加 `v5_connack_announces_topic_alias_maximum`；写用例：应用 env
      `mqtt_topic_alias_maximum => 10`（`application:set_env/3`，用例末恢复），v5 CONNECT，读 CONNACK
      字节断言含 `topic_alias_maximum => 10`；再设 0（或不设）断言 CONNACK 不含该属性。
- [ ] **Step 2（红）**：跑该套件确认失败。
- [ ] **Step 3（绿）**：
  - `#st{}` 加 `topic_alias_max = 0 :: non_neg_integer()`（在 `proto_level` 附近）。
  - `handle_connect/2` 的 `D0a = D0#st{...}` 加 `topic_alias_max = opt(mqtt_topic_alias_maximum, 0, D0)`。
  - `connack_bin(SP, RC, D)`：`D#st.topic_alias_max > 0` 且 v5 时给 props 加
    `#{topic_alias_maximum => D#st.topic_alias_max}`，否则 `#{}``。注意 `connack_bin/3` 当前不知
    v5——用 `vsn(D)`（`vsn/1` 已存在）或把 props 拼好后由 `enc/2` 按 `vsn(D)` 处理；确认
    `mqtt_frame:encode(#connack{props=...}, 5)` 会输出该属性（codec 已就绪）。
  - 注意 `connack_bin/3` 被 `refuse/2`（`:2575`）等多处调用——refuse 路径也会带 maximum，符合
    §3.2.2.3（CONNACK 始终携带本属性），可接受；若想最小面可只在成功 CONNACK 带，二选一，
    实现时保持一致并注释。
- [ ] **Step 4（绿）**：跑套件确认绿，且 `v5_topic_alias_zero_still_rejects`（本 Task 可一并加：
      未配置时入站带别名仍 0x94——逻辑在 Task 2，但零回归断言可先放这里，Task 2 实现后转绿）。
- [ ] **Step 5**：`git commit`。

## Task 2：入站别名解析

**Files**：`src/mqtt_conn_fsm.erl`（`handle_publish/2` v5 分支）

- [ ] **Step 1（红）**：`all()` 加 `v5_publish_with_topic_name_and_alias_registers_it`、
      `v5_publish_with_empty_topic_and_known_alias`、`v5_publish_with_empty_topic_and_unknown_alias_is_protocol_error`、
      `v5_publish_with_empty_topic_and_no_alias_is_protocol_error`、`v5_publish_alias_survives_reconnect`。
      - 用例 A：订阅方订阅真实 topic；发布方发 PUBLISH 带真实 topic + `topic_alias=1`，
        订阅方用真实 topic 收到（别名登记不影响投递）。
      - 用例 B：先发 topic+alias=1 登记，再发空 topic `<<>>`+`topic_alias=1`，订阅方用真实 topic 收到。
      - 用例 C：发空 topic + `topic_alias=99`（未登记）→ 连接以 0x94 断开（断言 publish 后连接关闭/
        `active` 不再，参考套件里既有 protocol_error 用例的断连断言手法）。
      - 用例 D：空 topic + 无别名 → 0x91 断开。
      - 用例 E：登记 alias 1→topic，`gen_server:stop` 发布方、clean=0 重连，再发空 topic+alias=1
        仍能还原到原 topic（验证持久化，依赖 Task 4 的 blob 持久化；若 Task 4 未做则本用例在
        Task 2 先跳过 `[skip]`，Task 4 后启用）。
- [ ] **Step 2（红）**：跑套件确认失败。
- [ ] **Step 3（绿）**：改写 `handle_publish/2` v5 分支（`:740-757`）为 spec §3.3 的解析：
      ```erlang
      handle_publish(#publish{topic = Topic, props = Props}, #st{proto_level = 5} = D) ->
          Alias = maps:get(topic_alias, Props, undefined),
          Max = D#st.topic_alias_max,
          case {byte_size(Topic), Alias, Max} of
              {0, _, 0} when Max =:= 0 ->
                  %% 未宣布别名却用了别名：维持现状
                  protocol_error(16#94, "...", D);
              {0, undefined, _} ->
                  protocol_error(16#91, "empty topic without alias", D);
              {0, _, _} ->
                  case maps:find(Alias, D#st.alias_in) of
                      {ok, RealTopic} ->
                          handle_publish_in(QoS, RealTopic, WirePid, Payload, Retain,
                                            maps:remove(topic_alias, Props), D);
                      error ->
                          protocol_error(16#94, "unknown topic alias", D)
                  end;
              {_, _, 0} when Alias =/= undefined ->
                  protocol_error(16#94, "...", D);
              {_, A, _} when A =/= undefined ->
                  %% topic 名非空 + 别名：登记/覆盖，用真实 topic 派发
                  D1 = D#st{alias_in = maps:put(A, Topic, D#st.alias_in)},
                  handle_publish_in(QoS, Topic, WirePid, Payload, Retain,
                                    maps:remove(topic_alias, Props), D1)
          end;
      ```
      （QoS/WirePid/Payload/Retain 从原记录解构；`#st{}` 需加 `alias_in`/`alias_out`/
      `next_alias_out`，见 Step 4。）
- [ ] **Step 4（绿）**：`#st{}` 加 `alias_in = #{}`、`alias_out = #{}`、`next_alias_out = 1`；
      跑套件 A/B/C/D 绿。
- [ ] **Step 5**：`git commit`。

## Task 3：出站别名生成

**Files**：`src/mqtt_conn_fsm.erl`（`publish_bin/6`）

- [ ] **Step 1（红）**：`all()` 加 `v5_outbound_publish_uses_allocated_alias`：配置
      `topic_alias_maximum => 5`，订阅方订阅真实 topic，发布方发真实 topic（不带别名），订阅方收到的
      PUBLISH 属性含 `topic_alias`（首次分配 1）。用直读字节或 `paho` 风格裸帧确认属性存在。
- [ ] **Step 2（红）**：跑套件确认失败。
- [ ] **Step 3（绿）**：`publish_bin/6`（`:2304`）在 `vsn(D)=:=5` 且 `topic_alias_max>0` 时调
      `maybe_alias(Topic, Props, D)`：`alias_out` 命中→加 `topic_alias`；未命中且
      `next_alias_out =< max`→分配+登记+加；否则原样。注意 will 路径若也走 `publish_bin` 则同样受益
      （will topic 通常只出现一次，多不命中，无副作用）。
- [ ] **Step 4（绿）**：跑套件绿。
- [ ] **Step 5**：`git commit`。

## Task 4：别名表持久化

**Files**：`src/mqtt_conn_fsm.erl`（`blob/1`、`restore_with/6`）

- [ ] **Step 1**：启用 Task 2 Step 1 的用例 E（去掉 `[skip]`）。
- [ ] **Step 2（红）**：跑用例 E 确认失败（当前 blob 不存别名，重连后 alias_in 空）。
- [ ] **Step 3（绿）**：`blob/1` 加 `aliases => {maps:to_list(alias_in), maps:to_list(alias_out), next_alias_out}`
      （仅 v5 且 `topic_alias_max>0` 时写入，否则不带该键或写空——与 `subs_blob` 同样门控）；
      `restore_with/6` 读回 `aliases` 重建三字段（缺省 `#{}`/`#{}`/`1`）。vsn 注释更新为 4。
- [ ] **Step 4（绿）**：跑用例 E 绿。
- [ ] **Step 5**：`git commit`。

## Task 5：配置 + 全量验证 + 文档

**Files**：`config/sys.config.src`、`docs/protocol-compatibility.md`、`docs/compatibility/unifiedmq-mqtt-family-gap.md`

- [ ] **Step 1**：`config/sys.config.src` 的 `broker_proto_mqtt` env 块加
      `{mqtt_topic_alias_maximum, 0}`，注释「0 = 关闭 Topic Alias（MQTT 5 可选特性，默认关闭以零回归）」。
- [ ] **Step 2**：dialyzer + xref（0 warning / 无新增 unused-export）。
- [ ] **Step 3**：`rm -rf _build/test/extras` 后全量 `rebar3 ct`，确认 1579 基线 +8 = 1587、零回归。
- [ ] **Step 4**：`protocol-compatibility.md` §4.11 MQTT 5 行 + capability 表：Topic Alias「明确未做」→「已支持」
      （机制：CONNACK 宣布 maximum，入站/出站别名，随会话持久化）；`gap` 文档 MQ-04 行改「已支持」+ 机制。
- [ ] **Step 5**：`git commit`。

## Task 6：merge 回 main + 清理

- [ ] **Step 1**：在 main 上 `git fetch` + 确认与 origin 同步；`git merge --no-ff mqtt5-topic-alias`。
- [ ] **Step 2**：main 上复跑全量 `rebar3 ct`（1587）+ dialyzer，确认合并后无回归。
- [ ] **Step 3**：移会话回主仓，平台 `worktree_remove`（force）清理，删分支。
- [ ] **Step 4**：询问用户是否推送 origin。

---

### 备注 / 坑位

- `handle_publish/2` 现 v5 分支解构了 `QoS/Topic/WirePid/Payload/Retain/Props`；改写时保持这些绑定。
- 别名解析出的真实 topic 进 `handle_publish_in/7`，`topic_alias` 属性**不**进 `#broker_message`
  （内核不见别名，符合 spec §2）。
- `connack_bin/3` 被 refuse 路径共用——决定「所有 CONNACK 带 maximum」还是「仅成功 CONNACK」，
  实现时统一并注释。
- `restore_blob`/`restore_with` 已有 vsn 门控读 `subs`；别名持久化照同款，vsn 升 4，旧 blob 读空表。
- 出站别名用尽即停发（不回收），`next_alias_out` 单调至 `topic_alias_max`。
