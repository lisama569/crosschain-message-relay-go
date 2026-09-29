# 本地跨链消息中继（Go）

用本地账本模拟源链与目标链，实现一条可解释、状态一致的跨链消息中继流程。
覆盖**确认深度、链重组、重放保护、乱序/序列号校验、多中继器并发竞争、执行限额**六类问题。

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `relay/types.go` | 消息、状态、可区分失败原因码 `FailReason` 与 `RelayResult` |
| `relay/source.go` | 源链账本：区块追加、确认深度计算、链重组回滚 |
| `relay/target.go` | 目标链账本：余额、幂等执行记录、严格递增序列号、竞争原子落账 |
| `relay/service.go` | 中继编排：确认/重组校验、副本比对、限额、调用目标链执行 |
| `relay/*_test.go` | 重组、重复投递、乱序、并发竞争、限额等场景单测 |
| `cmd/demo/main.go` | 端到端演示 |

## 确认规则

- 消息随区块上链，确认深度 = `链头高度 - 消息所在高度 + 1`。
- 只有 `confirmations >= RequiredConfirmations` 的消息才允许中继；
  未达深度返回 `status=PENDING, reason=UNCONFIRMED`，**绝不悄悄放行**，且余额不变。
- 链重组通过 `SourceChain.ReorgTo(keepHeight)` 模拟：高度大于 `keepHeight`
  的区块被整体丢弃，其中消息进入回滚集合，之后提交一律返回
  `status=REJECTED, reason=REORGED`。
- 源链中根本不存在的 ID 返回 `MESSAGE_NOT_FOUND`，与 `REORGED` 明确区分。
- 已成功落账的消息资金不可逆：即使源侧之后发生重组，重复提交结论仍是
  `ALREADY_RELAYED`（展示已落账事实），不会二次扣账。

## 重放保护机制

目标链 `Execute` 在**同一把互斥锁内**完成全部判定与落账，保证原子性：

1. **消息幂等**：`messageID` 的执行记录是唯一的。
   - 同一中继器重复投递 → `ALREADY_RELAYED`；
   - 不同中继器竞争同一条消息（后来者）→ `RELAY_RACE_LOST`。
2. **序列号严格递增**，下一个期望值记为 `nextSeq`：
   - `seq < nextSeq` 且被**另一条消息**占用 → `SEQUENCE_CONFLICT`；
   - `seq > nextSeq`（前序未执行）→ `SEQUENCE_GAP`，拒绝跳跃执行；
   - 中继器持有**同一消息的陈旧副本**（副本序列号小于源链规范化序列号）
     → `SEQUENCE_ROLLBACK`；其余字段被篡改 → `PAYLOAD_MISMATCH`。
3. 余额不足 → `INSUFFICIENT_FUND`，序列号与余额都不前进。
4. 所有拒绝路径都**先校验后落账**，不会静默覆盖已有状态或余额。

## 并发竞争与执行限额

- `Service.Submit` 整体串行化（内部互斥锁），目标链执行也是锁内原子的，
  因此 N 个中继器并发竞争同一条消息时**恰有一个**成功，其余全部得到
  `RELAY_RACE_LOST`，余额只扣一次、序列号只前进一次。
- 速率限额 `RatePerEpoch` / `Epoch`：每周期最多执行条数，超出返回
  `RATE_LIMITED`；进入下一周期自动恢复。
- 总额度 `TotalQuota`：服务生命周期内最多执行条数，超出返回
  `QUOTA_EXCEEDED`，跨周期不恢复。
- 限额检查在实际落账之前，被拒消息不影响任何已确认消息的余额与状态。

## 失败原因码一览

| 原因码 | 含义 |
| --- | --- |
| `MESSAGE_NOT_FOUND` | 源链无此消息 |
| `UNCONFIRMED` | 确认深度不足 |
| `REORGED` | 所在区块被链重组回滚 |
| `ALREADY_RELAYED` | 同一消息重复投递（自己重放） |
| `RELAY_RACE_LOST` | 并发竞争中已被其他中继器抢先执行 |
| `SEQUENCE_GAP` | 序列号超前，前序消息缺失 |
| `SEQUENCE_CONFLICT` | 序列号已被另一条消息占用 |
| `SEQUENCE_ROLLBACK` | 持有同一消息的过期序列号副本 |
| `PAYLOAD_MISMATCH` | 中继副本与源链规范化内容不一致 |
| `RATE_LIMITED` | 超过每周期速率 |
| `QUOTA_EXCEEDED` | 超过总额度 |
| `INSUFFICIENT_FUND` | 目标账户余额不足 |

## 验证方法

```bash
# 全量单测（含链重组、重复投递、乱序、并发竞争、限额）
go test ./... -v

# 并发安全检测（竞态）
go test ./relay/ -race -v

# 端到端演示
go run ./cmd/demo
```

单测日志统一通过 `logResult` 打印**消息标识、确认深度、执行状态、
失败原因、中标中继器与执行后余额**，例如：

```
[first] message=dup-1 confirmations=1 status=RELAYED reason="" executedBy=relayer-A balance=900
[duplicate-same-relayer] message=dup-1 confirmations=1 status=RELAYED reason="ALREADY_RELAYED" ...
[concurrent-15] message=race-1 confirmations=1 status=RELAYED reason="" executedBy=relayer-15 balance=999900
```
