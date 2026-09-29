package main

import (
	"fmt"
	"sync"

	"github.com/lisama569/crosschain-message-relay-go/relay"
)

func printResult(tag string, r relay.RelayResult) {
	fmt.Printf("%-22s message=%-8s depth=%d status=%-9s reason=%-18q by=%s balance=%d\n",
		tag, r.MessageID, r.Confirmations, r.Status, string(r.Reason), r.ExecutedBy, r.Balance)
}

func main() {
	src := relay.NewSourceChain()
	dst := relay.NewTargetChain()
	dst.Deposit("alice", 1000)

	svc := relay.NewService(src, dst, relay.RelayConfig{
		RequiredConfirmations: 3,
		RatePerEpoch:          10,
		Epoch:                 1_000_000_000,
		TotalQuota:            100,
	})

	// 区块 1：m1 上链，深度不足。
	src.AppendBlock([]relay.Message{
		{ID: "m1", From: "bob", To: "alice", Amount: 100, Sequence: 1},
	})
	printResult("submit depth=1", svc.Submit("rA", relay.Message{ID: "m1"}, 0))

	// 追加到深度 3 后确认。
	src.AppendBlock(nil)
	src.AppendBlock(nil)
	printResult("submit depth=3", svc.Submit("rA", relay.Message{ID: "m1"}, 0))

	// 重复投递：幂等拒绝。
	printResult("duplicate", svc.Submit("rA", relay.Message{ID: "m1"}, 0))

	// 区块 4 的消息随后被重组丢弃。
	src.AppendBlock([]relay.Message{
		{ID: "m2", From: "bob", To: "alice", Amount: 20, Sequence: 2},
	})
	src.AppendBlock(nil)
	src.ReorgTo(3) // 丢弃区块 4、5
	printResult("after reorg", svc.Submit("rA", relay.Message{ID: "m2"}, 0))

	// 多中继器并发竞争同一条消息（重新在高度 4 上链，补到深度 3）。
	src.AppendBlock([]relay.Message{
		{ID: "m3", From: "bob", To: "alice", Amount: 30, Sequence: 2},
	})
	src.AppendBlock(nil)
	src.AppendBlock(nil)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := svc.Submit(fmt.Sprintf("r%d", i), relay.Message{ID: "m3"}, 0)
			if r.Reason == relay.FailNone || r.Reason == relay.FailRaceLost {
				printResult(fmt.Sprintf("race r%d", i), r)
			}
		}(i)
	}
	wg.Wait()

	snap := svc.Snapshot("m1")
	fmt.Printf("snapshot: %+v\n", snap)
	fmt.Printf("final balance(alice)=%d nextSeq=%d\n", dst.Balance("alice"), dst.NextExpectedSequence())
}
