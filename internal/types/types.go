// Package types defines shared domain types for the local cross-chain message relay.
package types

// Status is the conclusion of handling a cross-chain message submission.
type Status string

const (
	StatusExecuted             Status = "executed"              // relayed and applied to the destination ledger
	StatusPendingConfirmations Status = "pending_confirmations" // source block not deep enough yet
	StatusReorgRolledBack      Status = "reorg_rolled_back"     // source block disappeared in a reorg
	StatusDuplicate            Status = "duplicate"             // same message id already delivered
	StatusSeqOutOfOrder        Status = "seq_out_of_order"      // sequence number gap or rollback
	StatusRaceLost             Status = "race_lost"             // another relayer won the message
	StatusRateLimited          Status = "rate_limited"          // per-second rate limit exceeded
	StatusQuotaExceeded        Status = "quota_exceeded"        // lifetime execution quota exhausted
	StatusInvalid              Status = "invalid"               // malformed message
)

// Message is a cross-chain message emitted on the source chain.
type Message struct {
	ID          string // globally unique message identifier
	SourceBlock uint64 // height of the source block containing the message
	Seq         uint64 // per-destination monotonically increasing sequence number, starts at 1
	To          string // recipient account on the destination chain
	Amount      int64  // amount to credit on the destination chain
}

// Result is an explainable outcome of one relay attempt.
type Result struct {
	MessageID string
	Status    Status
	Detail    string // human-readable, distinguishable reason
	Height    uint64 // source block height seen by the relayer
	Depth     uint64 // confirmations observed at handling time
}
