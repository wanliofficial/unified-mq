# S0.5 Spike Report: CloudEvents Validation Overhead

**结论：扩展属性数量必须设上限。1024 个扩展属性导致单条消息 8 ms 处理时间，是明确的 CPU 拒绝服务向量。建议默认上限 32 个。**

- **对应 Roadmap**：`docs/roadmap.md` §6 S0.5
- **验证对象**：README 3.1/3.2/3.3 事件模型、README 10.1 资源限制
- **环境**：OTP 28 (erts-16.2)，16 核，jsx 3.1.0
- **代码**：`spikes/s0_5_cloudevents/`

---

## 1. 实测数据

迭代 3,000 次（高扩展数场景 300 次），data payload 64 B：

| 扩展属性数 | JSON 字节 | decode | validate | encode | 合计 |
|---|---|---|---|---|---|
| 0 | 248 | — | — | — | **53.2 µs** |
| 4 | 320 | — | — | — | **88.5 µs** |
| 16 | 543 | — | — | — | **181.2 µs** |
| 64 | 1,455 | — | — | — | **527.6 µs** |
| 256 | 5,417 | 514.6 µs | 619.1 µs | 747.9 µs | **1,881.7 µs** |
| 1024 | 21,595 | 2,220.6 µs | 2,515.4 µs | 3,257.3 µs | **7,993.3 µs** |

（低扩展数场景未拆分 decode/validate/encode，见 `s0_5_codec:bench/3` 可重新运行。）

### 关键观察

1. **成本随扩展属性数线性增长**，无明显拐点。
2. **1024 扩展 = 8 ms/条**。单核每秒仅能处理约 125 条此类消息。
3. 校验本身（validate）与解码（decode）成本相当，说明属性遍历是主要开销，而非 JSON 解析。
4. 即使 0 扩展属性，53 µs 的基线开销也需要注意——这是 `validate_if_present` 策略下每条消息的固定成本。

---

## 2. 对 README 10.1 的建议

README 10.1 已要求"限制事件属性数量、扩展属性长度和嵌套 JSON 深度"，但未给出数值。基于本实测建议：

| 限制项 | 建议默认值 | 依据 |
|---|---|---|
| 扩展属性数量 | **32** | 64 个时已达 528 µs/条，接近 0.5 ms 预算 |
| 扩展属性名长度 | 64 字符 | CloudEvents 规范建议 ≤ 20，64 宽松 |
| 扩展属性值长度 | 8 KB | 防止用单个扩展撑爆内存 |
| 嵌套 JSON 深度 | 32 层 | 防止解析器栈溢出 |
| 单事件总字节 | 1 MB | 与 MQTT 最大包大小对齐 |

**扩展属性上限应可按租户/目的地配置**（与 README 3.4 的策略开关一致），可信内部生产者可以放宽。

### 资源保护含义

若不限制扩展属性数量，恶意生产者可用 21 KB 消息（每条 8 ms 处理）达到：
- 单核 **~125 msg/s** 的上限
- 1000 msg/s 的洪泛需要 **8 核** 全部耗尽在校验上

这与 README 10.1 防范"高基数/大属性导致资源耗尽"的要求直接相关。

---

## 3. 测试覆盖

`spikes/s0_5_cloudevents/test/s0_5_tests.erl`，12 个 EUnit 用例全部通过：

| 类别 | 用例 |
|---|---|
| 核心属性 | 合法事件、缺失必需属性、必需属性类型错误 |
| data/data_base64 | 互斥冲突检测（README 3.1 明确要求） |
| time 格式 | 合法 RFC3339、非法格式、非字符串类型 |
| 扩展属性名 | 大写拒绝、小写接受（CloudEvents 1.0 要求小写字母数字） |
| 编解码往返 | Structured Mode encode→decode 一致 |
| 非法输入 | 非 JSON、JSON 非对象 |

`data` 与 `data_base64` 互斥检测是本组测试中最重要的一条——README 3.1 特别强调不能任意混用。

---

## 4. 过程中的错误（记录以免重犯）

| 错误 | 现象 | 根因 |
|---|---|---|
| `us/1` 定义在函数体内 | syntax error | Erlang 不允许函数体内定义局部函数 |
| `per_us/1` 引用外部变量 | variable 'Iterations' is unbound | 需显式传参 |
| 多段二进制守卫 | variable 'YYYY' is unbound | `<<"YYYY-",...>>` 把整体绑定为一个变量，不能做算术守卫 |
| `lists:all(fun(<<C>>)...)` | function_clause | `binary_to_list/1` 返回整数列表，不是字节序列 |
| 扩展名校验未排除 `data` | function_clause | `data` 值是 map，但被当作扩展名检查 |

---

## 5. 对后续阶段的影响

### 对 S3（CloudEvents 层）

- 扩展属性上限是**必做项**，不是优化项
- `validate_if_present` 策略下，即使不使用 CloudEvents 的消息也要付出探测开销；需要明确"探测"的成本边界（是否只看 Content-Type）
- 校验失败必须走明确的拒绝/隔离路径（README 3.4 末段），本 spike 的 `{error, {invalid, Reason}}` 结构可直接映射

### 对 S6（生产化）

- 上限值需要暴露为管理 API 配置项，支持按租户调整
- 指标需记录校验失败原因分布，但**不得**把 `source`/`id` 作为标签（README 10.1 防高基数）

### 待办

- [ ] 嵌套深度对解析栈的影响（需构造深层 JSON）
- [ ] 1 MB 大消息的编解码成本
- [ ] Binary Mode 相比 Structured Mode 的开销差异（Binary Mode 不需解析 `data`，应更快）

---

## 6. 复现方式

```bash
export PATH=/home/wanli/Development/otp-28.3.1/bin:$PATH
cd spikes/s0_5_cloudevents

rebar3 get-deps
rebar3 eunit
rebar3 compile

# 全量扫描（高扩展数场景较慢，建议用小 iters）
erl -pa _build/default/lib/s0_5_cloudevents/ebin \
    -pa _build/default/lib/jsx/ebin -noshell \
    -eval 's0_5_main:main(["iters","300","out","RESULTS.txt"]), halt(0).'

# 单点测量
erl -pa _build/default/lib/s0_5_cloudevents/ebin \
    -pa _build/default/lib/jsx/ebin -noshell \
    -eval 'R = s0_5_codec:bench(3000, 64, 64), io:format("~p~n",[R]), halt(0).'
```