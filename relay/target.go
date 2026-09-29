package relay

import "sync"

type execution struct {
	msg       Message
	relayerID string
	balance   int64 // 执行后目标账户余额
}

// TargetChain 用本地账本模拟目标链：维护余额、已执行消息与严格递增序列号。
type TargetChain struct {
	mu       sync.Mutex
	balances map[string]int64
	executed map[string]*execution // messageID -> 执行记录
	seqOwner map[uint64]string     // sequence -> 占用该序列号的 messageID
	nextSeq  uint64                // 下一个允许的序列号
}

// NewTargetChain 创建目标链账本。
func NewTargetChain() *TargetChain {
	return &TargetChain{
		balances: make(map[string]int64),
		executed: make(map[string]*execution),
		seqOwner: make(map[uint64]string),
		nextSeq:  1,
	}
}

// Deposit 预存资金（模拟目标链上的流动性池/账户充值）。
func (t *TargetChain) Deposit(account string, amount int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.balances[account] += amount
}

// Balance 查询账户余额。
func (t *TargetChain) Balance(account string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.balances[account]
}

// IsExecuted 判断消息是否已执行。
func (t *TargetChain) IsExecuted(messageID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.executed[messageID]
	return ok
}

// NextExpectedSequence 返回下一个允许的序列号。
func (t *TargetChain) NextExpectedSequence() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextSeq
}

// ExecutedBy 返回执行该消息的中继器标识，未执行返回空串。
func (t *TargetChain) ExecutedBy(messageID string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ex, ok := t.executed[messageID]; ok {
		return ex.relayerID
	}
	return ""
}

// Execute 在目标链执行消息。整个判定与落账在同一把锁内完成，
// 因此多个中继器并发竞争同一条消息时只有一个能成功落账。
//
// 失败原因可区分：
//   - FailAlreadyRelayed   同一 messageID 重复投递
//   - FailSequenceConflict 序列号已被别的消息占用（含回退到旧序列号）
//   - FailSequenceGap      序列号超前（前序空缺）
//   - FailInsufficientFund 余额不足
func (t *TargetChain) Execute(msg Message, relayerID string) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 1) 重放保护：同一条消息只允许落账一次。
	if ex, ok := t.executed[msg.ID]; ok {
		if ex.relayerID == relayerID {
			return ex.balance, NewRelayError(FailAlreadyRelayed,
				"message %s already relayed by %s", msg.ID, ex.relayerID)
		}
		return ex.balance, NewRelayError(FailRaceLost,
			"message %s already relayed by %s, relayer %s lost the race",
			msg.ID, ex.relayerID, relayerID)
	}

	// 2) 序列号必须严格等于下一个期望值：
	//    小于 -> 已被占用（SEQUENCE_CONFLICT，回退场景在服务层已拦截）；
	//    大于 -> 前序空缺（SEQUENCE_GAP）。
	if msg.Sequence < t.nextSeq {
		return t.balances[msg.To], NewRelayError(FailSequenceConflict,
			"sequence %d already used by message %s", msg.Sequence, t.seqOwner[msg.Sequence])
	}
	if msg.Sequence > t.nextSeq {
		return t.balances[msg.To], NewRelayError(FailSequenceGap,
			"sequence %d skips expected %d", msg.Sequence, t.nextSeq)
	}

	// 3) 余额校验：不足时绝不改动账本。
	balance := t.balances[msg.To]
	if balance < msg.Amount {
		return balance, NewRelayError(FailInsufficientFund,
			"account %s balance %d < amount %d", msg.To, balance, msg.Amount)
	}

	// 4) 原子落账：余额、序列号、执行记录一起提交。
	balance -= msg.Amount
	t.balances[msg.To] = balance
	t.seqOwner[msg.Sequence] = msg.ID
	t.executed[msg.ID] = &execution{msg: msg, relayerID: relayerID, balance: balance}
	t.nextSeq = msg.Sequence + 1
	return balance, nil
}
