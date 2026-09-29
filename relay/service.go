package relay

import "sync"

// Service 是跨链中继服务：校验确认深度/重组、重放保护、
// 多中继器竞争与执行限额。所有提交串行化判定，保证并发下结论一致。
type Service struct {
	mu     sync.Mutex
	source *SourceChain
	target *TargetChain
	cfg    RelayConfig

	epochStart int64 // 当前速率周期起点（UnixNano）
	epochUsed  int   // 当前周期已执行条数
	totalUsed  int   // 生命周期已执行条数
}

// NewService 创建中继服务。
func NewService(source *SourceChain, target *TargetChain, cfg RelayConfig) *Service {
	return &Service{
		source: source,
		target: target,
		cfg:    cfg,
	}
}

func (s *Service) allow(nowNano int64) FailReason {
	// 周期滚动。
	if s.cfg.Epoch > 0 && s.epochStart > 0 && nowNano-s.epochStart >= int64(s.cfg.Epoch) {
		s.epochStart = nowNano
		s.epochUsed = 0
	}
	if s.epochStart == 0 {
		s.epochStart = nowNano
	}
	if s.cfg.TotalQuota > 0 && s.totalUsed >= s.cfg.TotalQuota {
		return FailQuotaExceeded
	}
	if s.cfg.RatePerEpoch > 0 && s.epochUsed >= s.cfg.RatePerEpoch {
		return FailRateLimited
	}
	return FailNone
}

// Submit 由某个中继器提交其持有的消息副本进行中继。
// 服务以源链账本中的规范化消息为准：
//   - 序列号小于规范值 -> SEQUENCE_ROLLBACK（持有过期/陈旧副本）；
//   - 其余字段不一致   -> PAYLOAD_MISMATCH；
//
// 任何拒绝都返回带可区分原因码的 RelayResult，且不改动账本。
func (s *Service) Submit(relayerID string, claim Message, nowNano int64) RelayResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.source.GetStatus(claim.ID)
	res := RelayResult{MessageID: claim.ID, Confirmations: st.Confirmations}
	if !st.Exists {
		res.Status = StatusRejected
		if st.Reorged {
			res.Reason = FailReorged
		} else {
			res.Reason = FailNotFound
		}
		return res
	}

	canonical := st.Message
	res.Balance = s.target.Balance(canonical.To)

	// 1) 重放：已在目标链执行 -> 区分自己重复投递还是竞争落败。
	if s.target.IsExecuted(claim.ID) {
		winner := s.target.ExecutedBy(claim.ID)
		res.Status = StatusRelayed
		res.ExecutedBy = winner
		if winner == relayerID {
			res.Reason = FailAlreadyRelayed
		} else {
			res.Reason = FailRaceLost
		}
		return res
	}

	// 2) 中继副本必须与源链规范化内容一致。
	//    只报 ID（零值副本）表示引用源链规范化消息；
	//    一旦携带了序列号/金额/账户，就必须逐字段吻合。
	if claim.Sequence != 0 || claim.Amount != 0 || claim.From != "" || claim.To != "" {
		if claim.Sequence != 0 && claim.Sequence < canonical.Sequence {
			res.Status = StatusRejected
			res.Reason = FailSequenceRollback
			return res
		}
		if claim != canonical {
			res.Status = StatusRejected
			res.Reason = FailPayloadMismatch
			return res
		}
	}

	// 3) 确认深度：未达到要求必须明确拒绝，不得放行。
	if st.Confirmations < s.cfg.RequiredConfirmations {
		res.Status = StatusPending
		res.Reason = FailUnconfirmed
		return res
	}

	// 4) 限额：速率与总额度。被限额拒绝不改动任何余额与状态。
	if reason := s.allow(nowNano); reason != FailNone {
		res.Status = StatusConfirmed
		res.Reason = reason
		return res
	}

	// 5) 目标链原子执行：序列号校验、竞争落账、余额。
	balance, err := s.target.Execute(canonical, relayerID)
	res.Balance = balance
	if err != nil {
		res.Reason = ReasonOf(err)
		if res.Reason == FailRaceLost {
			res.Status = StatusRelayed
			res.ExecutedBy = s.target.ExecutedBy(claim.ID)
		} else {
			res.Status = StatusRejected
		}
		return res
	}

	s.epochUsed++
	s.totalUsed++
	res.Status = StatusRelayed
	res.ExecutedBy = relayerID
	return res
}

// Snapshot 输出某条消息当前的完整状态视图，便于可解释排查。
func (s *Service) Snapshot(messageID string) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.source.GetStatus(messageID)
	snap := Snapshot{
		MessageID:     messageID,
		Confirmations: st.Confirmations,
	}
	switch {
	case st.Reorged:
		snap.SourceStatus = StatusReorged
	case !st.Exists:
		snap.SourceStatus = "UNKNOWN"
	case st.Confirmations >= s.cfg.RequiredConfirmations:
		snap.SourceStatus = StatusConfirmed
	default:
		snap.SourceStatus = StatusPending
	}
	if st.Exists {
		snap.Balance = s.target.Balance(st.Message.To)
	}
	if s.target.IsExecuted(messageID) {
		snap.TargetStatus = StatusRelayed
	} else {
		snap.TargetStatus = "NOT_EXECUTED"
	}
	return snap
}
