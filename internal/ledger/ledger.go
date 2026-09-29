// Package ledger simulates local source and destination blockchains.
package ledger

import (
	"fmt"
	"sync"

	"github.com/lisama569/crosschain-message-relay-go/internal/types"
)

// Block is a minimal source-chain block carrying messages.
type Block struct {
	Height   uint64
	Hash     string
	Parent   string
	Messages []types.Message
}

// SourceLedger simulates a source chain that can grow and reorganize.
// Block heights are contiguous starting at 0 and equal to the slice index.
type SourceLedger struct {
	mu       sync.RWMutex
	chain    []*Block            // height -> block currently in the canonical chain
	msgAt    map[string]uint64   // message id -> height in the current canonical chain
	everSeen map[string]struct{} // message ids that appeared in any block ever
}

// NewSourceLedger creates an empty source ledger.
func NewSourceLedger() *SourceLedger {
	return &SourceLedger{
		msgAt:    make(map[string]uint64),
		everSeen: make(map[string]struct{}),
	}
}

// TipHeight returns the current chain tip height. An empty chain tips at nothing.
func (s *SourceLedger) TipHeight() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.chain) == 0 {
		return 0, false
	}
	return s.chain[len(s.chain)-1].Height, true
}

// AppendBlock extends the chain tip. The block must extend contiguously.
func (s *SourceLedger) AppendBlock(b *Block) error {
	if b == nil {
		return fmt.Errorf("cannot append nil block")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.Height != uint64(len(s.chain)) {
		return fmt.Errorf("block height %d does not extend tip at %d", b.Height, len(s.chain))
	}
	if len(s.chain) > 0 && b.Parent != s.chain[len(s.chain)-1].Hash {
		return fmt.Errorf("block %s parent hash mismatch", b.Hash)
	}
	s.chain = append(s.chain, b)
	for _, m := range b.Messages {
		s.msgAt[m.ID] = b.Height
		s.everSeen[m.ID] = struct{}{}
	}
	return nil
}

// Reorg switches to a new branch that splits at commonAncestor: blocks at
// heights >= commonAncestor are removed and replaced with the given branch.
// It returns the blocks removed from the old canonical chain.
func (s *SourceLedger) Reorg(commonAncestor uint64, branch []*Block) ([]*Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if commonAncestor > uint64(len(s.chain)) {
		return nil, fmt.Errorf("common ancestor %d beyond chain length %d", commonAncestor, len(s.chain))
	}
	removed := make([]*Block, 0, len(s.chain)-int(commonAncestor))
	removed = append(removed, s.chain[commonAncestor:]...)
	s.chain = append([]*Block(nil), s.chain[:commonAncestor]...)
	for i, b := range branch {
		if b.Height != commonAncestor+uint64(i) {
			return nil, fmt.Errorf("branch block at index %d has height %d, want %d", i, b.Height, commonAncestor+uint64(i))
		}
		s.chain = append(s.chain, b)
		for _, m := range b.Messages {
			s.everSeen[m.ID] = struct{}{}
		}
	}
	// Rebuild the canonical message index.
	s.msgAt = make(map[string]uint64)
	for _, b := range s.chain {
		for _, m := range b.Messages {
			s.msgAt[m.ID] = b.Height
		}
	}
	return removed, nil
}

// ConfirmationDepth reports the source block height of a message and how many
// confirmations it currently has (the block itself counts as one). present is
// false when no block containing the message exists in the canonical chain;
// everObserved additionally tells whether the message existed in any past block.
func (s *SourceLedger) ConfirmationDepth(msgID string) (height, depth uint64, present, everObserved bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, everObserved = s.everSeen[msgID]
	h, ok := s.msgAt[msgID]
	if !ok || len(s.chain) == 0 {
		return 0, 0, false, everObserved
	}
	tip := s.chain[len(s.chain)-1].Height
	return h, tip - h + 1, true, true
}

// DestinationLedger simulates a destination chain with balances and a strict
// ordered, replay-protected message inbox. All mutating operations are
// serialized by a mutex; TryAcquire additionally exposes concurrent racers.
type DestinationLedger struct {
	mu        sync.Mutex
	balances  map[string]int64
	delivered map[string]bool // message ids already applied
	inflight  map[string]bool // message ids currently being committed by a relayer
	nextSeq   uint64
	applied   []string
}

// NewDestinationLedger creates an empty destination ledger expecting seq from 1.
func NewDestinationLedger() *DestinationLedger {
	return &DestinationLedger{
		balances:  make(map[string]int64),
		delivered: make(map[string]bool),
		inflight:  make(map[string]bool),
		nextSeq:   1,
	}
}

// TryAcquire attempts to claim a message for execution before commit.
// It distinguishes three cases:
//   - acquired: caller owns the claim and must call Commit;
//   - race_lost: another relayer is executing the same message right now;
//   - duplicate: the message was already committed earlier (replay).
func (d *DestinationLedger) TryAcquire(msgID string) (acquired bool, status types.Status, detail string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.delivered[msgID] {
		return false, types.StatusDuplicate, "message already executed on destination chain"
	}
	if d.inflight[msgID] {
		return false, types.StatusRaceLost, "another relayer is currently executing this message"
	}
	d.inflight[msgID] = true
	return true, "", ""
}

// Release gives up a previously acquired claim without applying anything.
func (d *DestinationLedger) Release(msgID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.inflight, msgID)
}

// Commit applies an acquired message, enforcing strict per-inbox sequence
// ordering. The claim is always released. On failure no state is mutated.
func (d *DestinationLedger) Commit(m types.Message) (types.Status, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	defer delete(d.inflight, m.ID)

	if d.delivered[m.ID] {
		return types.StatusDuplicate, "message already executed on destination chain"
	}
	if m.Seq < d.nextSeq {
		return types.StatusSeqOutOfOrder, fmt.Sprintf("sequence rollback: got seq=%d, expected %d", m.Seq, d.nextSeq)
	}
	if m.Seq > d.nextSeq {
		return types.StatusSeqOutOfOrder, fmt.Sprintf("sequence gap: got seq=%d, expected %d", m.Seq, d.nextSeq)
	}
	if m.Amount < 0 {
		return types.StatusInvalid, "negative amount"
	}
	d.balances[m.To] += m.Amount
	d.delivered[m.ID] = true
	d.applied = append(d.applied, m.ID)
	d.nextSeq++
	return types.StatusExecuted, "applied to destination ledger"
}

// Balance returns the balance of an account.
func (d *DestinationLedger) Balance(acct string) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.balances[acct]
}

// NextExpectedSeq returns the next sequence number the inbox expects.
func (d *DestinationLedger) NextExpectedSeq() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nextSeq
}

// AppliedCount returns how many messages have been executed.
func (d *DestinationLedger) AppliedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.applied)
}

// AppliedAt returns the id of the message executed at the given apply index.
func (d *DestinationLedger) AppliedAt(i int) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.applied[i]
}
