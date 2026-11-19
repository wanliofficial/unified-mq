# ADR-033：S12-D 前置——集群成员在线变更——admin 驱动的 `add_member`/`remove_member` 控制器

- **状态**：已采纳（2026-11-19，产品侧确认 G2 终态 =「一个逻辑 Broker」后立项）
- **日期**：2026-11-19
- **关联**：ADR-021（集群控制面、Metadata Raft 与 Fencing——本 ADR 扩展其成员变更面）、ADR-007（显式可靠性三档——成员变更的可用性承诺绑定它）、ADR-010（`broker_router` 行为——不变）、ADR-011（`broker_storage`——新成员建分区复用它）、ADR-019（订阅 / 游标本地——不变）、ADR-032（S12 节点视图真相、drain、再平衡边界——本 ADR 落实其中一条 TODO 并扩展 drain 生命周期）、[`docs/design/s12d-cross-node-delivery-convergence.md`](s12d-cross-node-delivery-convergence.md)（S12-D 跨节点投递，**另一份 ADR**，本 ADR 是其前置而非其实现）
- **事实来源**：`_build/default/lib/ra/src/ra.erl:582-635`（`ra:add_member/2,3`、`ra:remove_member/2,3` 的 spec 与实现）、`apps/broker_cluster/src/broker_cluster_meta.erl:98-122`（`ensure_group/2`、`members/0`、`members_env/0`）、`apps/broker_cluster/src/broker_rebalance.erl:76,108,184`（成员取值点）、`apps/broker_cluster/src/broker_cluster_meta_machine.erl:96-100`（`replicas_of/2`）、ADR-032「S12-C 的一处已知限制：成员集取自启动配置」与「一处实测到的成员变更限制」

## 背景（Context）

ADR-032 已把「成员来自静态启动配置」留成一条已知限制，并实测钉死了它的两个后果：

1. **单节点无法加入运行中的组。** ra 的 `initial_members` 只在首次 bootstrap 时被采纳；一个带新成员表启动的节点，其本地服务器找不到与自己匹配的已有集群，`broker_cluster_meta:init` 回 `{error, {metadata_cluster_not_formed, cluster_not_formed}}`（实测见 ADR-032「一处实测到的成员变更限制」）。
2. **扩容只能靠全组重启。** 全组同时重启（含新成员）实测可行，但有启动顺序竞态（`name_not_registered`），且每次成员变更都是一次有停机风险的运维动作。

而 **S12-D（跨节点投递 → G2 的「一个逻辑 Broker」）已经产品侧确认在范围内**（2026-11-19）。跨节点投递要成立，集群必须能**在不停机的前提下改变成员**——否则每次扩缩容都要全组重启，逻辑 Broker 无从谈起。所以成员在线变更是 S12-D 的**前置能力**，本 ADR 先冻结它的边界与形状，S12-D 本体另立 ADR。

关键事实：**本仓不缺库能力。** vendored `ra` 已提供 `ra:add_member/2,3` 与 `ra:remove_member/2,3`（`ra.erl:582-635`）：

- `add_member` 往 leader 日志提交一条 join 命令，**在命令追加到日志后即返回**（`after_log_append`），leader 随后开始向新服务器复制。新节点**不必在加入时已运行**，但预期在合理时间内上线；一个迟迟不上线的被公告成员会**抬高 quorum 节点数、影响集群可用性**（`ra.erl:574-577`）。
- `remove_member` 提交一条成员变更命令；被移除的服务器在变更复制后会**拒绝一切命令**（`ra.erl:608-609`）。

`broker_cluster_meta` **0 处调用它们**（`grep` 全仓确认：唯一 `remove_member` 在 `broker_proto_kafka` 的消费者组逻辑，与 ra 无关）。当前 `broker_cluster_meta:start_link/0` 只走 `ra:start_or_restart_cluster/5` 的 bootstrap/attach 路径（`broker_cluster_meta.erl:111`）。所以这是一处**接线缺口**，不是共识层缺陷。

## 决策

### D1. 成员在线变更 = 接线既有 ra 能力，不新增共识、不新增 app、不新增端口

采纳 `ra:add_member/2,3` / `ra:remove_member/2,3` 作为唯一成员变更入口，由 `broker_cluster_meta` 暴露两个新 API（形状见 D2）。**不**自行实现 gossip / 成员对账 / 视图合并——那是 ra 的职责，重复它等于引入第二套成员真相，违反 roadmap §14 硬边界①与 ADR-021。净新增运行时依赖 0、新 app 0、新监听端口 0。

### D2. 控制器形状：admin 驱动、显式、同步语义；成员变更 API 落在管理面（8090），不在数据面

- **API 入口（建议名，实现期定稿）**：`broker_cluster_meta:join(Node)` / `leave(Node)`，**admin 进程调用**（经 `broker_admin` 在 8090 的 `POST /api/v1/cluster/members` 一类管理端点），**不是**数据面前端、不是自动触发。
- **同步返回**：`join` 在 join 命令被 leader 日志接受后返回 `{ok, Node}` 或 `{error, {already_member | cluster_change_not_performed, _}}`（ra 的返回值原样透出 + 归类），**不**保证新节点已上线、已建分区、已追平——那是 D5 的异步收尾。
- **admin 驱动而非事件驱动**（同 ADR-032 D4「没有 watchdog / cron / 节点事件订阅」）：节点 up/down 的**自动**反应（如故障节点自动剔除、新节点自动纳管）**不在本 ADR**。故障节点的恢复走「带原成员表重启」（ADR-032 已修的崩溃恢复路径）。本 ADR 只做运维**显式**发起的扩缩容。

### D3. 再平衡器的成员取值点从「启动配置」改为「实时 ra 成员」——落实 ADR-032 的 TODO

`broker_rebalance:evaluate/0` 与 `step/0` 的成员来源（`broker_rebalance.erl:76,108` 调 `broker_cluster_meta:members/0`）改为读 **ra 的实时成员视图**：`ra:members({local, {broker_cluster_meta, node()}})`（`ra.erl:1102-1103`，`{local, _}` 形态**不跨节点、选举窗口里超时只是返回已知视图**而非失败）。这直接消解 ADR-032「S12-C 的一处已知限制：成员集取自启动配置」那条 TODO：

- 新加入的成员不再被当孤儿回收、已移除的不再被计入均衡目标。
- **诚实失败模式**：`{local, _}` 视图查询在极端情况下仍可能超时；触发者此时回 `{error, no_quorum}`（复用既有写路径答复），**不**引入第二种失败模式（ADR-032 该 TODO 的原约束）。
- 决策函数仍保留 `evaluate/1` / `step/1` 的**显式成员参数**（单节点可纯测、不维护第二份成员集，R-LB3 不变）；只是 `evaluate/0` / `step/0` 默认源换了。

### D4. 可用性语义：绑定 ADR-007 三档；加入未完成即计入 quorum 的风险必须显式

ra 文档（`ra.erl:574-577`）明确：一个被公告但迟迟不上线的成员会**抬高 quorum、拖累可用性**。本 ADR 的处置：

- `join` 的成功只代表**元数据变更被接受**，不代表新成员可用。新成员真正可用要满足 D5，期间集群的可用性按 ra 的实际 quorum 走（多数派仍可用，但若新成员卡在中间态，写可用性下降）——这与 ADR-007「失去多数派一律拒绝且不 ACK」一致，不额外承诺。
- `leave(Node)` 必须是**干净的 `ra:remove_member`**（先 drain、确认无在途，再移除），**不是** kill 进程。kill 一个成员等于让它变成「缺席成员」，仍计入 quorum 直到被显式 `remove_member`——这与 ADR-032 推荐的节点下线流程（先 drain → 再停进程）一致，本 ADR 把它接到 ra 的真实成员变更上。
- 成员变更命令本身是 ra 多数派提交，失去多数派时 `join`/`leave` 返回 `{error, no_quorum}`（经 `broker_cluster_meta` 归类），视图不变——不存在「本地以为移了但集群没提交」。

### D5. 数据面就绪是元数据变更之后的一步；`/readyz` 与 `/api/v1/node` 如实反映「未就绪」

加入一个成员要**两步**，二者分离：

1. **元数据成员变更**（D2 的 `ra:add_member`）——集群开始向新节点复制。
2. **数据面就绪**：新节点上**每个配置分区**都要 `broker_queue:create_partition(P)`（`broker_queue.erl:62`；需先 `broker_storage` 起来）才能承载读写。

在 D2 完成、D5 未完成之间，新节点在集群里是「成员但未就绪」。**这正好复用 S12-A 的 readiness 契约**：`/readyz` 只在其 app 已 started **且** 必要分区就绪后才回 200；`/api/v1/node` 的 `ready` 字段如实为 false。外部 LB 因此**不会**把新连接路由到一个还不能服务的成员（ADR-032 D3 的「外部 LB 消费节点视图」在这里闭合）。运维流程的口径是：`join` 后轮询 `/api/v1/node` 直到 `ready` true，再放流量。

### D6. 不做什么（边界）

- **不做跨节点投递 / S12-D 本体**。本 ADR 是 S12-D 的**前置**（成员可在线变更），不是 S12-D。S12-D（订阅者节点索引 + 跨节点扇出 + 全局游标，撞 ADR-010/019 冻结边界）另立 ADR，见 [`s12d-cross-node-delivery-convergence.md`](s12d-cross-node-delivery-convergence.md)。
- **不做自动扩缩容 / 自动故障剔除**（D2）。
- **不改分区数 G、不改 `phash2` 编号**（ADR-008 / ADR-032 D5）。成员变更只改 `(owner, epoch, replicas)` 的**成员集**，不改分区数与路由函数。
- **不引入一致性哈希 / `chash` / ring / 第二套共识**（roadmap 硬边界①、ADR-021）。
- **不改变 `broker_security` 语义、`broker_metrics` 冻结键集**；成员变更的可观测若需计数，复用既有 `cluster_*` 键或走日志，不新增冻结键。

### D7. 与 drain / 再平衡 / 崩溃恢复的衔接

- **下线流程**（把 ADR-032 §operations 5.3 的推荐流程接到真实成员变更）：① `POST /api/v1/drain` 摘流 → ② 等 `/api/v1/node` 的 `connections.active` 归零 → ③ 确认 `broker_rebalance:status()` 的 `orphaned` 为空 → ④ 在存活的节点 `broker_rebalance:step` 把其分区 owner 换走（或 `leave` 走 D4 的干净移除）→ ⑤ `ra:remove_member` / 停进程。`broker_rebalance` 在成员实时化（D3）后能正确识别「谁还在组里」。
- **崩溃恢复不变**：单节点崩溃后带**原成员表**重启仍走 ADR-032 已修的 `ensure_group/2` 路径；本 ADR 不改这条，只在其之上补「新节点在线加入」。

## 后果（Consequences）

- **正向**：扩缩容从「全组重启 + 启动竞态」变为「admin 一条命令 + 轮询就绪」；S12-D 的前置能力就位；ADR-032 那条 TODO 关闭；`broker_rebalance` 不再对动态成员误判孤儿 / 越界。
- **代价 / 风险**：ra 的「被公告成员抬高 quorum」是真实可用性风险，D4 的口径（join 成功 ≠ 可用、未完成期间可用性下降）必须写进运维文档，否则运维会误以为 `join` 返回即扩好了。成员视图读 `ra:members({local,_})` 是本地查询，可能比 leader 视图旧，触发者据此决策时可能落后一个成员变更——这是可接受的（下一轮 step 会追上），但要如实说明。
- **依赖顺序**：本 ADR（成员在线变更）应先于 S12-D（跨节点投递）实现；S12-D 还依赖本 ADR 的成员可在线变更，但不依赖本 ADR 的再平衡成员取值改动。
- **冻结文本不改**：ADR-010/019/021 的相关边界**不在本 ADR 修订**——成员变更只在既有 ra 组上加命令，不触碰 router 的倒排索引、不触碰消费者本地 ETS、不新增第二套成员真相。

## 验证（Validation）

本 ADR 只冻结形状，**不开端口、不写实现代码**。实现期必须落 CT（扩 `broker_cluster_dist_SUITE` 或新增套件），至少含：

1. **在线加入**：3 节点组成立后，第 4 个节点经控制器 `join`（**不重启存活节点**）→ 存活节点的 `ra:members` 立即含新节点；新节点建分区、追平后 `/api/v1/node` 的 `ready` 翻 true；存活对端全程未被触碰、组仍可提交（对比 ADR-032 实测的「单节点无法加入」为反证基线）。
2. **再平衡识别新成员**：`join` 后 `broker_rebalance:status()` 的 `members` 含新节点（D3 生效），不把它当孤儿、不向已移除节点搬分区。
3. **干净移除**：`leave`（先 drain）后该节点从 `ra:members` 消失、其分区被存活成员接管（`broker_rebalance` 回收或运维 step），组仍提交。
4. **失去多数派**：`join`/`leave` 在失去多数派时回 `{error, no_quorum}`、视图不变。
5. **反证**：把成员取值点改回启动配置 → 用例 2 立即红（新成员不在 `members` 里被当孤儿）。

## 落地偏差记录

- **2026-11-19 立项**：本 ADR 因产品侧确认 G2 终态 =「一个逻辑 Broker」（S12-D 在范围内）而启动；S12-D 本体（跨节点投递）另见 [`s12d-cross-node-delivery-convergence.md`](s12d-cross-node-delivery-convergence.md)，本 ADR 只交付其前置能力「成员在线变更」。实现尚未开工，落地偏差将在实现提交后追加。
- **2026-11-19 实现落地（S12-D 前置）**：接线在 `broker_cluster_meta`（`join/1,2`、`leave/1,2`、`with_server/1`、`add_member/2`、`remove_member/2`、`live_members/1`），`broker_cluster` 透出同名委托（`join/1,2`、`leave/1,2`），`broker_rebalance` 的 `evaluate/0`、`step/0`、`status/0` 改读 `broker_cluster_meta:members/0`（新增 `members_view/0` 把 `{error,_}` 折成空视图，D3 的「诚实失败模式」），8090 落 `POST /api/v1/cluster/members`。CT：`broker_cluster_dist_SUITE` 扩到 10 例（新增 4 例覆盖验证 1~4，验证 5 以反证并入验证 2 用例），`broker_admin_SUITE` 扩到 23 例（3 例钉住 8090 端点的分发、入参校验与未启动守卫）。未新增 app / 运行时依赖 / 端口，未改 `broker_security` 语义，未扩 `broker_metrics` 冻结键集（D1 成立）。

以下为实现期实测到、与 ADR 正文不一致的地方（正文不改，以本记录为准）：

- **偏差 1：`join` 是「leader 记录 + 新节点自起 server」两步，`ra:restart_server` 用不了。** 实测 `ra:add_member/3` 只在 leader 侧把新 `ra_server_id()` 写进已提交配置；新节点必须自己 `ra:start_server(default, broker_cluster_meta, {broker_cluster_meta, New}, {module, broker_cluster_meta_machine, #{}}, [{broker_cluster_meta, Seed}])` 以既有成员为 seed 起 follower、追平日志（ra README「Dynamically Changing Cluster Membership」的第二步）。对一个从未启动过 server 的新节点调 `ra:restart_server/3` 回 `{error, name_not_registered}`。故 D2 的「控制器只调 `ra:add_member`」只覆盖元数据侧，**第二步（`start_server`）仍是引导/运维职责**，与 D5「join 成功 ≠ 就绪」一致。
- **偏差 2：返回形状不是 `{ok, Node}`，且「单 voter 加入」实测返回 `ok` 而非错误。** 实现返回 `ok` / `{ok, already_member}` / `{ok, not_member}` / `{error, Reason}`：幂等空操作（已是成员 / 本就不是成员）按成功处理，因为期望终态已成立；ra 的 `{error, already_member}`、`{error, not_member}` 不再作为错误透出。更重要的实测：ra 3.2.0 的 `after_log_append` 在 leader 追加即返回，**单节点组就是自己的多数派**，所以在「其余成员已停」的 lone-voter 场景里 `join/1` 合法地回 `ok`（ADR 正文预期的是 `{error, cluster_change_not_performed}`）。因此验证 4 的断言口径改为**看持久事实**而非看返回值：新节点**没有**进入已提交成员（`ra_directory:list_registered(default)` 不含它）、后续写仍回 `{error, no_quorum}`。
- **偏差 3：追平验证走 cache 黑盒。** ADR 验证 1 要求「新节点建分区、追平后 `/api/v1/node` 的 `ready` 翻 true」。`ra_server_proc:state_query/2` 在 3.2.0 不存在（只有 `/3`），查 ra 内部状态会越界。改为：join 后在 leader `assign` 一个新分区，轮询新节点 `broker_cluster_cache:owner(1) =:= {ok, N1}` —— 该 cache 由 ra 的 apply loop 写入，能读到即证明新节点已应用 join 之后的日志（追平）。数据面后半段（`broker_queue:create_partition/1`、`/readyz` 翻转）属 `broker_queue` + `broker_storage` 的既有套件，本 ADR 的 CT 不重复钉。
- **偏差 4：`members/0` 的形状与一个已修的实现 bug。** `broker_cluster_meta:members/0` 现在回**节点原子列表**（从 `ra:members({local, {?SERVER, Node}})` 的 `ra_server_id()` 里抽第二元素），不是 `{?SERVER, Node}` 元组列表；且当本节点不在已提交成员（多数派丢失）时回 `{error, no_quorum}`，复用既有写路径的失败形状，不引第二种。首版 `join`/`leave` 误用 `lists:member({?SERVER, Node}, Members)` 判成员，恒为 false，导致 `leave` 永不调 `ra:remove_member`（`clean_leave_removes_the_node_from_the_group` 抓到的 `{badmatch, {ok, not_member}}`）。已改为 `lists:member(Node, Members)`，用例钉住。
- **偏差 5：8090 端点是惰性守卫，不加 app 依赖边。** handler 用 `started(broker_cluster)` 判（同既有 `cluster/0` 的 `try broker_cluster:snapshot()` 模式），`broker_admin` 的 `.app.src` 不新增 `broker_cluster` 依赖，故「未启动」是 503 `cluster_not_running` 而不是启动失败。node 名用 `binary_to_existing_atom/2` 解析（不铸新原子，未知名回 400 `unknown_node`，非字符串回 400 `invalid_node`）。错误码映射：`ok`/`{ok, already_member}`/`{ok, not_member}` → 200；`{error, no_quorum}` → 503；其他 `{error, Reason}` → 503 `cluster_membership_failed`；请求体问题 → 400（`invalid_json`/`body_required`/`body_must_have_action_and_node`/`invalid_action`）。
- **偏差 6：验证 5 以反证形式并入验证 2。** `online_join_is_visible_to_the_rebalance_view` 先断言 `false = lists:member(New, Members)`（`New` 永不在启动配置里），join 后再断言 `status().members` 含 `New` 且 `orphaned` 不含它——若成员取值点退回启动配置，这条立即红。不再单列用例。
- **`leave` 的职责边界**：`leave` 只从组的已提交配置里移除该节点（`ra:remove_member/3`，由 leader 提交）；被移除节点的本地进程不归本 ADR 管，D7 步骤⑤的「停进程」仍是运维动作。
