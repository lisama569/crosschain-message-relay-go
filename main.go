// Command crosschain-relay runs a local demo of the cross-chain message relay:
// shallow submit, finalized execution, replay protection, a reorg refusal and
// a rate-limited submit, with per-relayer logs of depth and outcome.
package main

import (
	"fmt"
	"time"

	"github.com/lisama569/crosschain-message-relay-go/internal/ledger"
	"github.com/lisama569/crosschain-message-relay-go/internal/relay"
	"github.com/lisama569/crosschain-message-relay-go/internal/types"
)

func main() {
	src := ledger.NewSourceLedger()
	dst := ledger.NewDestinationLedger()
	r := relay.New("demo-relayer", src, dst, relay.Limits{
		RequiredDepth: 2,
		PerSecond:     10,
		TotalQuota:    100,
	})

	appendBlock := func(h uint64, hash, parent string, msgs ...types.Message) {
		if err := src.AppendBlock(&ledger.Block{Height: h, Hash: hash, Parent: parent, Messages: msgs}); err != nil {
			panic(err)
		}
	}

	appendBlock(0, "g0", "")
	appendBlock(1, "g1", "g0", types.Message{ID: "m1", SourceBlock: 1, Seq: 1, To: "alice", Amount: 100})
	submit(r, types.Message{ID: "m1", SourceBlock: 1, Seq: 1, To: "alice", Amount: 100}) // depth 1 < 2

	appendBlock(2, "g2", "g1")
	submit(r, types.Message{ID: "m1", SourceBlock: 1, Seq: 1, To: "alice", Amount: 100}) // depth 2 -> execute
	submit(r, types.Message{ID: "m1", SourceBlock: 1, Seq: 1, To: "alice", Amount: 100}) // replay

	appendBlock(3, "g3", "g2", types.Message{ID: "m2", SourceBlock: 3, Seq: 2, To: "bob", Amount: 50})
	// Reorg height 3 away: m2's block disappears.
	if _, err := src.Reorg(3, []*ledger.Block{{Height: 3, Hash: "p3", Parent: "g2"}}); err != nil {
		panic(err)
	}
	submit(r, types.Message{ID: "m2", SourceBlock: 3, Seq: 2, To: "bob", Amount: 50}) // reorg rollback

	// Rate-limited relayer on its own destination ledger, using blocks from
	// the new canonical branch (p3 at height 3 and two blocks above it).
	appendBlock(4, "p4", "p3", types.Message{ID: "x1", SourceBlock: 4, Seq: 1, To: "carol", Amount: 1})
	appendBlock(5, "p5", "p4", types.Message{ID: "x2", SourceBlock: 5, Seq: 2, To: "carol", Amount: 1})
	dst2 := ledger.NewDestinationLedger()
	tight := relay.New("tight-relayer", src, dst2, relay.Limits{RequiredDepth: 1, PerSecond: 1})
	submit(tight, types.Message{ID: "x1", SourceBlock: 4, Seq: 1, To: "carol", Amount: 1}) // execute
	submit(tight, types.Message{ID: "x2", SourceBlock: 5, Seq: 2, To: "carol", Amount: 1}) // rate limited
	time.Sleep(10 * time.Millisecond)

	fmt.Printf("final: dst applied=%d alice=%d bob=%d; tight applied=%d committed=%d\n",
		dst.AppliedCount(), dst.Balance("alice"), dst.Balance("bob"),
		dst2.AppliedCount(), tight.Committed())
}

func submit(r *relay.Relayer, m types.Message) {
	res := r.Submit(m)
	fmt.Printf("[%s] message=%s height=%d depth=%d result=%s detail=%s\n",
		r.Name(), res.MessageID, res.Height, res.Depth, res.Status, res.Detail)
}
