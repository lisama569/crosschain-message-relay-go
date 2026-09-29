package relay

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/crosschain-message-relay-go/internal/ledger"
	"github.com/lisama569/crosschain-message-relay-go/internal/types"
)

func m(id string, h, seq uint64, to string, amt int64) types.Message {
	return types.Message{ID: id, SourceBlock: h, Seq: seq, To: to, Amount: amt}
}

// buildChain creates blocks 0..tip with messages placed at msgHeight.
func buildChain(t *testing.T, s *ledger.SourceLedger, tip uint64, msgs map[uint64][]types.Message) {
	t.Helper()
	var parent string
	for h := uint64(0); h <= tip; h++ {
		hash := fmt.Sprintf("g%d", h)
		b := &ledger.Block{Height: h, Hash: hash, Parent: parent, Messages: msgs[h]}
		if err := s.AppendBlock(b); err != nil {
			t.Fatalf("append block %d: %v", h, err)
		}
		parent = hash
	}
}

// logResult prints message id, observed confirmation depth and relay result.
func logResult(t *testing.T, r *Relayer, res types.Result) {
	t.Helper()
	t.Logf("[relay=%s] message=%s height=%d depth=%d result=%s detail=%q",
		r.Name(), res.MessageID, res.Height, res.Depth, res.Status, res.Detail)
}

// TestConfirmationDepthGate verifies shallow messages are refused with a
// distinguishable reason and become executable once deep enough.
func TestConfirmationDepthGate(t *testing.T) {
	src := ledger.NewSourceLedger()
	buildChain(t, src, 2, map[uint64][]types.Message{
		1: {m("m1", 1, 1, "alice", 100)},
	}) // tip=2 -> m1 depth=2
	r := New("r1", src, ledger.NewDestinationLedger(), Limits{RequiredDepth: 3})

	res := r.Submit(m("m1", 1, 1, "alice", 100))
	logResult(t, r, res)
	if res.Status != types.StatusPendingConfirmations || res.Depth != 2 {
		t.Fatalf("want pending_confirmations with depth 2, got %s depth %d", res.Status, res.Depth)
	}
	if got := r.dst.Balance("alice"); got != 0 {
		t.Fatalf("shallow submit must not credit balance, got %d", got)
	}

	if err := src.AppendBlock(&ledger.Block{Height: 3, Hash: "g3", Parent: "g2"}); err != nil {
		t.Fatal(err)
	} // depth=3
	res = r.Submit(m("m1", 1, 1, "alice", 100))
	logResult(t, r, res)
	if res.Status != types.StatusExecuted || res.Depth != 3 {
		t.Fatalf("want executed at depth 3, got %s depth %d", res.Status, res.Depth)
	}
}

// TestReorgRollback refuses a message whose block disappeared in a reorg.
func TestReorgRollback(t *testing.T) {
	src := ledger.NewSourceLedger()
	buildChain(t, src, 2, map[uint64][]types.Message{
		1: {m("m1", 1, 1, "alice", 100)},
		2: {m("m2", 2, 2, "bob", 50)},
	})
	dst := ledger.NewDestinationLedger()
	r := New("r1", src, dst, Limits{RequiredDepth: 1})

	if res := r.Submit(m("m1", 1, 1, "alice", 100)); res.Status != types.StatusExecuted {
		t.Fatalf("m1 execute: %s %s", res.Status, res.Detail)
	} else {
		logResult(t, r, res)
	}

	// Reorg from height 2: new branch tip does not contain m2.
	if _, err := src.Reorg(2, []*ledger.Block{
		{Height: 2, Hash: "p2", Parent: "g1"},
	}); err != nil {
		t.Fatal(err)
	}
	res := r.Submit(m("m2", 2, 2, "bob", 50))
	logResult(t, r, res)
	if res.Status != types.StatusReorgRolledBack {
		t.Fatalf("want reorg_rolled_back, got %s", res.Status)
	}
	if dst.AppliedCount() != 1 || dst.Balance("bob") != 0 {
		t.Fatalf("reorged message must not apply: applied=%d bob=%d", dst.AppliedCount(), dst.Balance("bob"))
	}

	// Resubmitting m1 (still canonical) must hit replay protection, not re-execute.
	res = r.Submit(m("m1", 1, 1, "alice", 100))
	logResult(t, r, res)
	if res.Status != types.StatusDuplicate {
		t.Fatalf("want duplicate on replay, got %s", res.Status)
	}
	if dst.Balance("alice") != 100 {
		t.Fatalf("alice balance = %d, want 100", dst.Balance("alice"))
	}
}

// TestReplayAndOutOfOrder covers duplicate delivery plus seq rollback/gap.
func TestReplayAndOutOfOrder(t *testing.T) {
	src := ledger.NewSourceLedger()
	buildChain(t, src, 4, map[uint64][]types.Message{
		1: {m("m1", 1, 1, "alice", 10)},
		2: {m("m2", 2, 2, "alice", 20)},
		3: {m("m3", 3, 3, "alice", 30)},
		4: {m("m-rollback", 4, 1, "alice", 5)},
	})
	dst := ledger.NewDestinationLedger()
	r := New("r1", src, dst, Limits{RequiredDepth: 1})

	cases := []types.Message{
		m("m1", 1, 1, "alice", 10),        // execute
		m("m1", 1, 1, "alice", 10),        // duplicate replay (same id)
		m("m3", 3, 3, "alice", 30),        // gap: expects seq 2
		m("m2", 2, 2, "alice", 20),        // execute after gap message
		m("m-rollback", 4, 1, "alice", 5), // new id but seq rollback to 1
		m("m3", 3, 3, "alice", 30),        // now executes in order
	}
	want := []types.Status{
		types.StatusExecuted,
		types.StatusDuplicate,
		types.StatusSeqOutOfOrder,
		types.StatusExecuted,
		types.StatusSeqOutOfOrder,
		types.StatusExecuted,
	}
	for i, c := range cases {
		res := r.Submit(c)
		logResult(t, r, res)
		if res.Status != want[i] {
			t.Fatalf("case %d: want %s, got %s (%s)", i, want[i], res.Status, res.Detail)
		}
	}
	if dst.AppliedCount() != 3 || dst.Balance("alice") != 60 {
		t.Fatalf("applied=%d balance=%d, want 3/60", dst.AppliedCount(), dst.Balance("alice"))
	}
}

// TestConcurrentRelayersSingleWinner: many relayers race for one message.
func TestConcurrentRelayersSingleWinner(t *testing.T) {
	src := ledger.NewSourceLedger()
	buildChain(t, src, 1, map[uint64][]types.Message{
		1: {m("m1", 1, 1, "alice", 42)},
	})
	dst := ledger.NewDestinationLedger()

	const n = 12
	relayers := make([]*Relayer, n)
	winnerHolding := make(chan struct{}) // closed once the winner holds the claim
	losersReady := make(chan struct{})
	var losersWG sync.WaitGroup
	losersWG.Add(n - 1)
	proceedWinner := make(chan struct{})

	for i := range relayers {
		relayers[i] = New(fmt.Sprintf("relayer-%02d", i), src, dst, Limits{RequiredDepth: 1})
	}
	// relayer-00 is the designated winner: it holds the claim while all
	// competitors race against it, then commits.
	relayers[0].acquiredHook = func(msgID string) {
		close(winnerHolding)
		losersWG.Wait() // ensure every loser has observed the inflight claim
		<-proceedWinner
	}

	results := make([]types.Result, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	go func() {
		defer wg.Done()
		<-start
		results[0] = relayers[0].Submit(m("m1", 1, 1, "alice", 42))
	}()
	for i := 1; i < n; i++ {
		go func(i int, rl *Relayer) {
			defer wg.Done()
			<-winnerHolding
			results[i] = rl.Submit(m("m1", 1, 1, "alice", 42))
			losersWG.Done()
		}(i, relayers[i])
	}
	close(start)
	go func() {
		losersWG.Wait()
		close(losersReady)
	}()
	<-losersReady
	close(proceedWinner)
	wg.Wait()

	executed, race, dup := 0, 0, 0
	for i, res := range results {
		logResult(t, relayers[i], res)
		switch res.Status {
		case types.StatusExecuted:
			executed++
		case types.StatusRaceLost:
			race++
		case types.StatusDuplicate:
			dup++
		default:
			t.Fatalf("unexpected status: %s", res.Status)
		}
	}
	if executed != 1 || race != n-1 {
		t.Fatalf("want 1 executed and %d race_lost, got executed=%d race=%d duplicate=%d", n-1, executed, race, dup)
	}
	if dst.AppliedCount() != 1 || dst.Balance("alice") != 42 {
		t.Fatalf("applied=%d balance=%d, want 1/42", dst.AppliedCount(), dst.Balance("alice"))
	}

	// After commit, a fresh delivery is a replay duplicate, not a race loss.
	res := relayers[1].Submit(m("m1", 1, 1, "alice", 42))
	logResult(t, relayers[1], res)
	if res.Status != types.StatusDuplicate {
		t.Fatalf("post-commit replay must be duplicate, got %s", res.Status)
	}
}

// TestRateAndQuotaLimits ensures limited submissions are refused distinctly
// and never touch confirmed balances.
func TestRateAndQuotaLimits(t *testing.T) {
	src := ledger.NewSourceLedger()
	msgs := map[uint64][]types.Message{}
	for i := uint64(1); i <= 5; i++ {
		msgs[i] = []types.Message{m(fmt.Sprintf("m%d", i), i, i, "alice", 1)}
	}
	buildChain(t, src, 5, msgs)

	t.Run("rate_limit", func(t *testing.T) {
		dst := ledger.NewDestinationLedger()
		r := New("rate-rl", src, dst, Limits{RequiredDepth: 1, PerSecond: 2})
		res1 := r.Submit(m("m1", 1, 1, "alice", 1))
		res2 := r.Submit(m("m2", 2, 2, "alice", 1))
		res3 := r.Submit(m("m3", 3, 3, "alice", 1))
		for _, res := range []types.Result{res1, res2, res3} {
			logResult(t, r, res)
		}
		if res1.Status != types.StatusExecuted || res2.Status != types.StatusExecuted {
			t.Fatalf("first two should execute: %s %s", res1.Status, res2.Status)
		}
		if res3.Status != types.StatusRateLimited {
			t.Fatalf("third should be rate limited, got %s", res3.Status)
		}
		if dst.Balance("alice") != 2 {
			t.Fatalf("rate-limited submit must not credit balance, got %d", dst.Balance("alice"))
		}
		// The refused claim is released; after the window passes it can execute.
		time.Sleep(1050 * time.Millisecond)
		res := r.Submit(m("m3", 3, 3, "alice", 1))
		logResult(t, r, res)
		if res.Status != types.StatusExecuted {
			t.Fatalf("m3 should execute after window, got %s (%s)", res.Status, res.Detail)
		}
	})

	t.Run("quota_exceeded", func(t *testing.T) {
		dst := ledger.NewDestinationLedger()
		r := New("quota-rl", src, dst, Limits{RequiredDepth: 1, TotalQuota: 2})
		expect := []types.Status{types.StatusExecuted, types.StatusExecuted, types.StatusQuotaExceeded}
		for i, id := range []string{"m1", "m2", "m3"} {
			res := r.Submit(m(id, uint64(i+1), uint64(i+1), "alice", 1))
			logResult(t, r, res)
			if res.Status != expect[i] {
				t.Fatalf("%s: want %s got %s", id, expect[i], res.Status)
			}
		}
		if dst.Balance("alice") != 2 || dst.AppliedCount() != 2 {
			t.Fatalf("confirmed state must be intact: balance=%d applied=%d", dst.Balance("alice"), dst.AppliedCount())
		}
	})
}
