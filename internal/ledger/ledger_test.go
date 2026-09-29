package ledger

import (
	"sync"
	"testing"

	"github.com/lisama569/crosschain-message-relay-go/internal/types"
)

func msg(id string, h, seq uint64, to string, amt int64) types.Message {
	return types.Message{ID: id, SourceBlock: h, Seq: seq, To: to, Amount: amt}
}

func TestSourceReorgRebuildsMessageIndex(t *testing.T) {
	s := NewSourceLedger()
	_ = s.AppendBlock(&Block{Height: 0, Hash: "g0"})
	_ = s.AppendBlock(&Block{Height: 1, Hash: "g1", Parent: "g0", Messages: []types.Message{msg("m1", 1, 1, "alice", 10)}})
	_ = s.AppendBlock(&Block{Height: 2, Hash: "g2", Parent: "g1", Messages: []types.Message{msg("m2", 2, 2, "bob", 20)}})

	if h, d, ok, ever := s.ConfirmationDepth("m1"); !ok || h != 1 || d != 2 || !ever {
		t.Fatalf("m1 before reorg: h=%d d=%d ok=%v ever=%v", h, d, ok, ever)
	}

	// Reorg from height 1: new branch contains m1' but not the old m2.
	removed, err := s.Reorg(1, []*Block{
		{Height: 1, Hash: "p1", Parent: "g0", Messages: []types.Message{msg("m1p", 1, 1, "alice", 11)}},
		{Height: 2, Hash: "p2", Parent: "p1"},
	})
	if err != nil || len(removed) != 2 {
		t.Fatalf("reorg: err=%v removed=%d", err, len(removed))
	}
	if _, _, ok, ever := s.ConfirmationDepth("m2"); ok || !ever {
		t.Fatalf("m2 should be rolled back yet historically observed: ok=%v ever=%v", ok, ever)
	}
	if _, _, ok, _ := s.ConfirmationDepth("m1p"); !ok {
		t.Fatalf("m1p should be present after reorg")
	}
}

func TestDestinationOrderedDelivery(t *testing.T) {
	d := NewDestinationLedger()

	if st, _ := d.Commit(msg("m2", 1, 2, "alice", 5)); st != types.StatusSeqOutOfOrder {
		t.Fatalf("expected seq gap rejection, got %s", st)
	}
	if st, _ := d.Commit(msg("m1", 1, 1, "alice", 10)); st != types.StatusExecuted {
		t.Fatalf("expected execute, got %s", st)
	}
	if st, _ := d.Commit(msg("m1-again", 1, 1, "alice", 10)); st != types.StatusSeqOutOfOrder {
		t.Fatalf("expected seq rollback rejection, got %s", st)
	}
	if got := d.Balance("alice"); got != 10 {
		t.Fatalf("balance = %d, want 10 (rejected deliveries must not mutate state)", got)
	}
}

func TestDestinationConcurrentClaimSingleWinner(t *testing.T) {
	d := NewDestinationLedger()
	const n = 16
	var wg sync.WaitGroup
	acquired := make(chan bool, n)
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			ok, _, _ := d.TryAcquire("m1")
			acquired <- ok
			if !ok {
				return
			}
			_, _ = d.Commit(msg("m1", 1, 1, "alice", 7))
		}()
	}
	close(start)
	wg.Wait()
	close(acquired)

	winners, losers := 0, 0
	for ok := range acquired {
		if ok {
			winners++
		} else {
			losers++
		}
	}
	if winners != 1 || losers != n-1 {
		t.Fatalf("winners=%d losers=%d, want 1/%d", winners, losers, n-1)
	}
	if d.AppliedCount() != 1 || d.Balance("alice") != 7 {
		t.Fatalf("unexpected final state: applied=%d balance=%d", d.AppliedCount(), d.Balance("alice"))
	}
}
