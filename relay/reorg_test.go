package relay

import "testing"

// TestConfirmationDepth 验证未达确认深度必须被明确拒绝（UNCONFIRMED），
// 达到深度后才能中继。
func TestConfirmationDepth(t *testing.T) {
	svc, src, dst := newTestService(RelayConfig{RequiredConfirmations: 3})
	dst.Deposit("alice", 1000)

	src.AppendBlock([]Message{{ID: "msg-1", From: "bob", To: "alice", Amount: 100, Sequence: 1}})

	// 深度 1 < 3：拒绝。
	r := svc.Submit("relayer-A", Message{ID: "msg-1"}, 0)
	logResult(t, "depth=1", r)
	if r.Reason != FailUnconfirmed || r.Status != StatusPending {
		t.Fatalf("want UNCONFIRMED/PENDING, got %s/%s", r.Reason, r.Status)
	}
	if dst.Balance("alice") != 1000 {
		t.Fatalf("unconfirmed submit must not change balance, got %d", dst.Balance("alice"))
	}

	// 补两个块 -> 深度 3：放行。
	src.AppendBlock(nil)
	src.AppendBlock(nil)
	r = svc.Submit("relayer-A", Message{ID: "msg-1"}, 0)
	logResult(t, "depth=3", r)
	if r.Status != StatusRelayed || r.Reason != FailNone {
		t.Fatalf("want RELAYED, got %s/%s", r.Status, r.Reason)
	}
	if dst.Balance("alice") != 900 {
		t.Fatalf("want balance 900, got %d", dst.Balance("alice"))
	}
}

// TestReorgRejects 验证消息所在区块被链重组丢弃后，提交必须被明确拒绝，
// 且不得落账。
func TestReorgRejects(t *testing.T) {
	svc, src, dst := newTestService(RelayConfig{RequiredConfirmations: 1})
	dst.Deposit("alice", 1000)

	src.AppendBlock([]Message{{ID: "msg-r1", From: "bob", To: "alice", Amount: 50, Sequence: 1}})

	// 先在未中继时重组：区块 1 被丢弃。
	removed := src.ReorgTo(0)
	if removed != 1 {
		t.Fatalf("want 1 removed msg, got %d", removed)
	}
	r := svc.Submit("relayer-A", Message{ID: "msg-r1"}, 0)
	logResult(t, "after-reorg", r)
	if r.Reason != FailReorged || r.Status != StatusRejected {
		t.Fatalf("want REORGED/REJECTED, got %s/%s", r.Reason, r.Status)
	}
	if dst.Balance("alice") != 1000 {
		t.Fatalf("reorged msg must not change balance, got %d", dst.Balance("alice"))
	}

	// 不存在的消息给出可区分的 NOT_FOUND。
	r = svc.Submit("relayer-A", Message{ID: "ghost"}, 0)
	logResult(t, "not-found", r)
	if r.Reason != FailNotFound {
		t.Fatalf("want MESSAGE_NOT_FOUND, got %s", r.Reason)
	}
}

// TestReorgAfterConfirm 验证已确认但尚未中继的消息遭遇重组也必须拒绝。
func TestReorgAfterConfirm(t *testing.T) {
	// 要求深度 1：区块一上链即可中继。
	svc, src, dst := newTestService(RelayConfig{RequiredConfirmations: 1})
	dst.Deposit("alice", 1000)

	src.AppendBlock([]Message{{ID: "msg-r2", From: "bob", To: "alice", Amount: 50, Sequence: 1}})
	src.AppendBlock(nil) // 深度 2

	// 重组到高度 1，丢弃高度 2 —— 消息仍在高度 1，深度回到 1 但仍满足要求。
	src.ReorgTo(1)
	r := svc.Submit("relayer-A", Message{ID: "msg-r2"}, 0)
	logResult(t, "reorg-keep-msg", r)
	if r.Status != StatusRelayed {
		t.Fatalf("msg in kept block should relay, got %s/%s", r.Status, r.Reason)
	}

	// 另一条消息在高度 3，重组丢弃高度 3 -> REORGED。
	src.AppendBlock([]Message{{ID: "msg-r3", From: "bob", To: "alice", Amount: 50, Sequence: 2}})
	src.AppendBlock(nil)
	src.ReorgTo(1) // 丢弃高度 2、3
	r = svc.Submit("relayer-A", Message{ID: "msg-r3"}, 0)
	logResult(t, "reorg-dropped", r)
	if r.Reason != FailReorged {
		t.Fatalf("want REORGED, got %s", r.Reason)
	}
	if dst.Balance("alice") != 950 {
		t.Fatalf("only msg-r2 should have executed, balance=%d", dst.Balance("alice"))
	}
}
