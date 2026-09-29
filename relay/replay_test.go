package relay

import "testing"

func confirmedService() (*Service, *SourceChain, *TargetChain) {
	return newTestService(RelayConfig{RequiredConfirmations: 1})
}

// TestDuplicateDelivery 验证同一消息重复投递只执行一次，
// 重复提交得到可区分的 ALREADY_RELAYED，余额不二次扣减。
func TestDuplicateDelivery(t *testing.T) {
	svc, src, dst := confirmedService()
	dst.Deposit("alice", 1000)
	src.AppendBlock([]Message{{ID: "dup-1", From: "bob", To: "alice", Amount: 100, Sequence: 1}})

	r1 := svc.Submit("relayer-A", Message{ID: "dup-1"}, 0)
	logResult(t, "first", r1)
	if r1.Status != StatusRelayed {
		t.Fatalf("first submit should relay, got %s/%s", r1.Status, r1.Reason)
	}

	// 同一中继器再投一次（携带完整副本也不影响结论：以源链为准）。
	r2 := svc.Submit("relayer-A", Message{ID: "dup-1", Sequence: 1, Amount: 100, To: "alice"}, 0)
	logResult(t, "duplicate-same-relayer", r2)
	if r2.Reason != FailAlreadyRelayed {
		t.Fatalf("duplicate by same relayer -> ALREADY_RELAYED, got %s", r2.Reason)
	}
	if dst.Balance("alice") != 900 {
		t.Fatalf("duplicate delivery must not double-charge, balance=%d", dst.Balance("alice"))
	}
	if dst.NextExpectedSequence() != 2 {
		t.Fatalf("sequence must advance once, next=%d", dst.NextExpectedSequence())
	}
}

// TestSequenceRollback 验证中继器持有同一消息的陈旧副本（序列号回退）
// 必须被明确拒绝，不得用旧内容覆盖已有状态。
func TestSequenceRollback(t *testing.T) {
	svc, src, dst := confirmedService()
	dst.Deposit("alice", 1000)
	src.AppendBlock([]Message{{ID: "roll-1", From: "bob", To: "alice", Amount: 10, Sequence: 5}})

	// 陈旧副本：声称序列号 2，而源链规范化序列号是 5。
	stale := Message{ID: "roll-1", From: "bob", To: "alice", Amount: 10, Sequence: 2}
	r := svc.Submit("relayer-A", stale, 0)
	logResult(t, "stale-claim", r)
	if r.Reason != FailSequenceRollback {
		t.Fatalf("stale claim -> SEQUENCE_ROLLBACK, got %s", r.Reason)
	}
	if dst.IsExecuted("roll-1") || dst.Balance("alice") != 1000 {
		t.Fatalf("rejected stale claim must not affect ledger")
	}

	// 其他字段被篡改（金额不一致）-> PAYLOAD_MISMATCH。
	tampered := Message{ID: "roll-1", From: "bob", To: "alice", Amount: 999, Sequence: 5}
	r = svc.Submit("relayer-A", tampered, 0)
	logResult(t, "tampered-claim", r)
	if r.Reason != FailPayloadMismatch {
		t.Fatalf("tampered claim -> PAYLOAD_MISMATCH, got %s", r.Reason)
	}
}

// TestOutOfOrder 验证乱序到达：序列号空缺/冲突都被明确拒绝，
// 已执行消息的余额与状态不被静默覆盖。
func TestOutOfOrder(t *testing.T) {
	svc, src, dst := confirmedService()
	dst.Deposit("alice", 1000)

	src.AppendBlock([]Message{{ID: "seq-2", From: "bob", To: "alice", Amount: 10, Sequence: 2}})
	r := svc.Submit("r", Message{ID: "seq-2"}, 0)
	logResult(t, "gap", r)
	if r.Reason != FailSequenceGap {
		t.Fatalf("missing seq 1 -> SEQUENCE_GAP, got %s", r.Reason)
	}
	if dst.Balance("alice") != 1000 {
		t.Fatalf("rejected out-of-order msg must not change balance")
	}

	// 正常执行 seq 1。
	src.AppendBlock([]Message{{ID: "seq-1", From: "bob", To: "alice", Amount: 10, Sequence: 1}})
	r = svc.Submit("r", Message{ID: "seq-1"}, 0)
	logResult(t, "seq1", r)
	if r.Status != StatusRelayed {
		t.Fatalf("seq-1 should relay, got %s/%s", r.Status, r.Reason)
	}

	// seq 2 现在可以执行。
	r = svc.Submit("r", Message{ID: "seq-2"}, 0)
	logResult(t, "seq2", r)
	if r.Status != StatusRelayed {
		t.Fatalf("seq-2 should relay after seq-1, got %s/%s", r.Status, r.Reason)
	}

	// 冲突：另一条不同的消息也声称序列号 2（已被 seq-2 占用）。
	src.AppendBlock([]Message{{ID: "seq-clash", From: "bob", To: "alice", Amount: 10, Sequence: 2}})
	r = svc.Submit("r", Message{ID: "seq-clash"}, 0)
	logResult(t, "conflict", r)
	if r.Reason != FailSequenceConflict {
		t.Fatalf("occupied sequence -> SEQUENCE_CONFLICT, got %s", r.Reason)
	}

	if dst.Balance("alice") != 980 {
		t.Fatalf("only seq-1 and seq-2 should debit, balance=%d", dst.Balance("alice"))
	}
	if dst.NextExpectedSequence() != 3 {
		t.Fatalf("next expected sequence should be 3, got %d", dst.NextExpectedSequence())
	}
}
