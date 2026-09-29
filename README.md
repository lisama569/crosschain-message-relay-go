# 本地跨链消息中继（Go）

用本地内存账本模拟源链与目标链，实现一条**可解释、结论一致**的跨链消息中继流程：
消息在源链达到要求的确认深度后才允许中继，链重组会撤销/拒绝回滚消息，
目标链对重复投递、乱序和并发竞争做幂等保护，并支持按中继器的速率与总额度限制。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `internal/types` | 共享领域类型：`Message`、`Result` 与全部可区分状态 `Status` |
| `internal/ledger` | 本地账本：`SourceLedger`（出块/重组/确认深度）与 `DestinationLedger`（余额、有序幂等收件箱、竞争认领） |
| `internal/relay` | `Relayer`：编排确认规则、重组判定、竞争协调、速率与额度限制 |
| `main.go` | 可运行的端到端演示 |

## 中继判定流程（`Relayer.Submit`）

每次投递只产生一个明确结论，绝不静默放行：

1. **消息合法性**：`id` 非空、`seq` 从 1 开始，否则 `invalid`。
2. **源链确认与重组检查**（`SourceLedger.ConfirmationDepth`）：
   - 消息所在区块已不在当前规范链、但历史上出现过 → `reorg_rolled_back`，明确拒绝；
   - 消息从未存在于任何区块 → `invalid`；
   - 区块仍在链上但 `depth = tip - blockHeight + 1 < RequiredDepth` → `pending_confirmations`，
     返回实际深度与要求深度，待后续区块补齐后重新提交即可执行；
   - 深度达标后才继续。
3. **目标链单赢家认领**（`DestinationLedger.TryAcquire`）：
   - 已落账 → `duplicate`（重放）；
   - 正被其他中继器执行 → `race_lost`（并发竞争失败）；
   - 否则当前中继器拿到唯一 claim。
4. **速率与额度**（滚动 1 秒窗口 + 生命周期总额度）：
   - 窗口内执行数达到 `PerSecond` → `rate_limited`；
   - 累计执行数达到 `TotalQuota` → `quota_exceeded`；
   - 被拒绝时立即释放 claim 与预留名额，**不计费、不改动任何余额**。
5. **有序提交**（`DestinationLedger.Commit`，全程互斥锁）：
   - 序列号小于期望值 → `seq_out_of_order`（回退）；
   - 大于期望值 → `seq_out_of_order`（空缺/gap）；
   - 恰好等于期望值才记账：增加余额、记录已投递 id、期望序号 +1。
   - 提交失败（如序号问题）同样释放 claim 与限额预留，不会造成状态污染。

### 结果状态一览

| 状态 | 含义 |
| --- | --- |
| `executed` | 已成功落账目标链 |
| `pending_confirmations` | 源链确认深度不足（返回实际/要求深度） |
| `reorg_rolled_back` | 所在区块被链重组移除，拒绝中继 |
| `duplicate` | 消息 id 已执行（重放保护） |
| `seq_out_of_order` | 序列号回退或空缺（严格有序，不允许覆盖/跳号） |
| `race_lost` | 并发竞争中其他中继器正在执行同一消息 |
| `rate_limited` | 超过每秒速率限制 |
| `quota_exceeded` | 超过生命周期执行额度 |
| `invalid` | 消息非法或不属于任何源链区块 |

## 关键一致性保证

- **单赢家落账**：目标链所有变更在同一把互斥锁内完成；claim 阶段区分
  "正在竞争（`race_lost`）"与"早已落账（`duplicate`）"，并发下只有一个中继器成功。
- **幂等与重放保护**：已投递消息 id 集合 + 每个目标收件箱严格递增序号；
  重复 id、序号回退/空缺都被显式拒绝，已确认余额不受任何失败提交影响。
- **重组安全**：源链重组后按新规范链重建消息索引；被回滚消息再提交一律拒绝，
  已经在深度达标后落账的消息不受影响（本模拟不处理"执行后又被深度重组"的长程重组）。
- **限额不误伤**：速率/额度采用"先预留、成功才计费，失败即取消"的方式，
  被限流的提交不会消耗额度，也不会改动余额与序号。

## 验证方法

```bash
# 全部单测（含竞态检测）
go test -race ./...

# 查看带消息标识 / 确认深度 / 执行结果的详细日志
go test ./internal/relay/ -v
go test ./internal/ledger/ -v

# 端到端演示：浅深度拒绝 → 达标执行 → 重放 → 重组拒绝 → 限流
go run .
```

测试覆盖：

- 链重组（`TestSourceReorgRebuildsMessageIndex` / `TestReorgRollback`）：回滚消息被拒绝、
  仍在规范链上的消息重放时命中幂等保护；
- 重复投递与乱序（`TestDestinationOrderedDelivery` / `TestReplayAndOutOfOrder`）：
  重复 id、序号空缺与回退均被区分拒绝；
- 并发竞争（`TestDestinationConcurrentClaimSingleWinner` /
  `TestConcurrentRelayersSingleWinner`）：12 个中继器竞争同一消息，恰好 1 个执行、
  11 个得到 `race_lost`，落账后再投递为 `duplicate`；
- 限额（`TestRateAndQuotaLimits`）：`rate_limited` 窗口过后可补执行，
  `quota_exceeded` 不影响已确认余额。

测试日志统一打印 `relayer / message / height / depth / result / detail`，
失败原因可直接区分。
