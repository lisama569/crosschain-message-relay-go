package relay

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentRelayRace 多个中继器同时竞争同一条已确认消息，
// 只允许一个成功落账，其余得到可区分的 RELAY_RACE_LOST。
func TestConcurrentRelayRace(t *testing.T) {
	svc, src, dst := newTestService(RelayConfig{RequiredConfirmations: 1})
	dst.Deposit("alice", 1_000_000)
	src.AppendBlock([]Message{{ID: "race-1", From: "bob", To: "alice", Amount: 100, Sequence: 1}})

	const n = 16
	var wg sync.WaitGroup
	results := make([]RelayResult, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			relayer := fmt.Sprintf("relayer-%02d", i)
			<-start
			results[i] = svc.Submit(relayer, Message{ID: "race-1"}, 0)
		}(i)
	}
	close(start)
	wg.Wait()

	winners, losers := 0, 0
	for i, r := range results {
		logResult(t, fmt.Sprintf("concurrent-%02d", i), r)
		switch {
		case r.Status == StatusRelayed && r.Reason == FailNone:
			winners++
		case r.Reason == FailRaceLost:
			losers++
		default:
			t.Fatalf("unexpected result for relayer %d: %s/%s", i, r.Status, r.Reason)
		}
	}
	if winners != 1 {
		t.Fatalf("exactly one winner expected, got %d", winners)
	}
	if losers != n-1 {
		t.Fatalf("%d losers expected, got %d", n-1, losers)
	}
	if dst.Balance("alice") != 1_000_000-100 {
		t.Fatalf("exactly one debit expected, balance=%d", dst.Balance("alice"))
	}
	if dst.NextExpectedSequence() != 2 {
		t.Fatalf("sequence must advance exactly once, next=%d", dst.NextExpectedSequence())
	}
}

// TestRateLimit 每周期限额 2：第 3 条在同周期被 RATE_LIMITED 拒绝，
// 下一周期自动恢复；被拒绝的消息不得影响余额。
func TestRateLimit(t *testing.T) {
	cfg := RelayConfig{RequiredConfirmations: 1, RatePerEpoch: 2, Epoch: 1_000_000_000}
	svc, src, dst := newTestService(cfg)
	dst.Deposit("alice", 1_000_000)

	baseNano := int64(10_000_000_000)
	for i := uint64(1); i <= 3; i++ {
		id := fmt.Sprintf("rate-%d", i)
		src.AppendBlock([]Message{{ID: id, From: "bob", To: "alice", Amount: 10, Sequence: i}})
		r := svc.Submit("r", Message{ID: id}, baseNano)
		logResult(t, "epoch-1", r)
		if i <= 2 && r.Status != StatusRelayed {
			t.Fatalf("msg %d should relay within rate, got %s/%s", i, r.Status, r.Reason)
		}
		if i == 3 {
			if r.Reason != FailRateLimited || r.Status != StatusConfirmed {
				t.Fatalf("msg 3 should be RATE_LIMITED, got %s/%s", r.Status, r.Reason)
			}
		}
	}
	if dst.Balance("alice") != 1_000_000-20 {
		t.Fatalf("rate-limited msg must not debit, balance=%d", dst.Balance("alice"))
	}

	// 进入下一周期，第 3 条恢复执行。
	r := svc.Submit("r", Message{ID: "rate-3"}, baseNano+1_000_000_000)
	logResult(t, "epoch-2", r)
	if r.Status != StatusRelayed {
		t.Fatalf("rate limit should reset in new epoch, got %s/%s", r.Status, r.Reason)
	}
	if dst.Balance("alice") != 1_000_000-30 {
		t.Fatalf("balance after recovery=%d", dst.Balance("alice"))
	}
}

// TestTotalQuota 总额度 2：第 3 条永远 QUOTA_EXCEEDED，
// 即使跨周期也不恢复，且余额保持不变。
func TestTotalQuota(t *testing.T) {
	cfg := RelayConfig{
		RequiredConfirmations: 1,
		RatePerEpoch:          10,
		Epoch:                 1_000_000_000,
		TotalQuota:            2,
	}
	svc, src, dst := newTestService(cfg)
	dst.Deposit("alice", 1_000_000)

	t0 := int64(20_000_000_000)
	for i := uint64(1); i <= 3; i++ {
		id := fmt.Sprintf("quota-%d", i)
		src.AppendBlock([]Message{{ID: id, From: "bob", To: "alice", Amount: 10, Sequence: i}})
		r := svc.Submit("r", Message{ID: id}, t0)
		logResult(t, "quota", r)
		if i <= 2 && r.Status != StatusRelayed {
			t.Fatalf("within quota should relay, got %s/%s", r.Status, r.Reason)
		}
		if i == 3 && r.Reason != FailQuotaExceeded {
			t.Fatalf("over quota -> QUOTA_EXCEEDED, got %s/%s", r.Status, r.Reason)
		}
	}
	// 跨周期也不恢复。
	r := svc.Submit("r", Message{ID: "quota-3"}, t0+5_000_000_000)
	logResult(t, "quota-next-epoch", r)
	if r.Reason != FailQuotaExceeded {
		t.Fatalf("total quota must not reset, got %s", r.Reason)
	}
	if dst.Balance("alice") != 1_000_000-20 {
		t.Fatalf("only two debits allowed, balance=%d", dst.Balance("alice"))
	}
}

// TestInsufficientFund 余额不足被明确拒绝且不推进序列号。
func TestInsufficientFund(t *testing.T) {
	svc, src, dst := newTestService(RelayConfig{RequiredConfirmations: 1})
	dst.Deposit("alice", 5)
	src.AppendBlock([]Message{{ID: "poor-1", From: "bob", To: "alice", Amount: 10, Sequence: 1}})
	r := svc.Submit("r", Message{ID: "poor-1"}, 0)
	logResult(t, "insufficient", r)
	if r.Reason != FailInsufficientFund {
		t.Fatalf("want INSUFFICIENT_FUND, got %s", r.Reason)
	}
	if dst.NextExpectedSequence() != 1 {
		t.Fatalf("failed execution must not advance sequence, next=%d", dst.NextExpectedSequence())
	}
	if dst.Balance("alice") != 5 {
		t.Fatalf("balance must remain 5, got %d", dst.Balance("alice"))
	}
}
