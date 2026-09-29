// Package relay wires source confirmation rules, reorg detection, the
// destination inbox and per-relayer rate/quota limits into the relay workflow.
package relay

import (
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/crosschain-message-relay-go/internal/ledger"
	"github.com/lisama569/crosschain-message-relay-go/internal/types"
)

// Limits configures per-relayer rate and total execution quota.
// Zero or negative PerSecond / TotalQuota means that dimension is unlimited.
type Limits struct {
	RequiredDepth uint64 // source confirmations required before relay
	PerSecond     int    // max executions committed in any rolling 1s window
	TotalQuota    int    // max executions committed over the relayer lifetime
}

// reservation is one reserved or committed rate-window slot.
type reservation struct {
	at        time.Time
	committed bool
}

// limiter enforces a rolling per-second rate and a lifetime quota. Only
// successfully committed executions are finally billed; cancelled reservations
// (e.g. sequence rejection) never consume capacity.
type limiter struct {
	mu        sync.Mutex
	window    []*reservation // reservations younger than ~1s
	committed int            // lifetime committed executions
	limit     Limits
}

func newLimiter(l Limits) *limiter { return &limiter{limit: l} }

// reserve atomically checks capacity and holds a slot until commit/cancel.
func (l *limiter) reserve(now time.Time) (types.Status, string, *reservation) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pruneLocked(now)
	if l.limit.TotalQuota > 0 && l.committed >= l.limit.TotalQuota {
		return types.StatusQuotaExceeded,
			fmt.Sprintf("lifetime quota exhausted: %d/%d", l.committed, l.limit.TotalQuota), nil
	}
	if l.limit.PerSecond > 0 {
		active := 0
		for _, r := range l.window {
			if !r.committed || now.Sub(r.at) < time.Second {
				active++
			}
		}
		if active >= l.limit.PerSecond {
			return types.StatusRateLimited,
				fmt.Sprintf("rate limit exceeded: %d executions in the last second, limit %d/s", active, l.limit.PerSecond), nil
		}
	}
	r := &reservation{committed: false}
	l.window = append(l.window, r)
	return "", "", r
}

// commit bills a reservation as a successful execution.
func (l *limiter) commit(r *reservation, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r.at = now
	r.committed = true
	l.committed++
}

// cancel releases a reservation that did not end in an execution.
func (l *limiter) cancel(r *reservation) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.window[:0]
	for _, w := range l.window {
		if w != r {
			out = append(out, w)
		}
	}
	l.window = out
}

// pruneLocked drops committed reservations older than one second.
func (l *limiter) pruneLocked(now time.Time) {
	out := l.window[:0]
	for _, r := range l.window {
		if !r.committed || now.Sub(r.at) < time.Second {
			out = append(out, r)
		}
	}
	l.window = out
}

// Committed reports how many executions this relayer has billed.
func (l *limiter) Committed() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.committed
}

// Relayer coordinates one relay path between two local ledgers.
type Relayer struct {
	name    string
	src     *ledger.SourceLedger
	dst     *ledger.DestinationLedger
	limits  Limits
	buckets *limiter
	now     func() time.Time

	// acquiredHook, when set, runs while the destination claim is held and
	// before commit. Intended for tests that need a deterministic race window.
	acquiredHook func(msgID string)
}

// New creates a Relayer.
func New(name string, src *ledger.SourceLedger, dst *ledger.DestinationLedger, limits Limits) *Relayer {
	return &Relayer{
		name:    name,
		src:     src,
		dst:     dst,
		limits:  limits,
		buckets: newLimiter(limits),
		now:     time.Now,
	}
}

// Name returns the relayer's identifier.
func (r *Relayer) Name() string { return r.name }

// Committed returns how many messages the relayer has successfully executed.
func (r *Relayer) Committed() int { return r.buckets.Committed() }

// Submit handles one relay attempt of a message and always returns an
// explainable, distinguishable result.
func (r *Relayer) Submit(m types.Message) types.Result {
	res := types.Result{MessageID: m.ID, Height: m.SourceBlock}

	if m.ID == "" {
		res.Status = types.StatusInvalid
		res.Detail = "message id is empty"
		return res
	}
	if m.Seq == 0 {
		res.Status = types.StatusInvalid
		res.Detail = "sequence number must start at 1"
		return res
	}

	// 1) Source-chain finalization rule: depth check and reorg detection.
	height, depth, present, ever := r.src.ConfirmationDepth(m.ID)
	res.Height = height
	res.Depth = depth
	if !present {
		if ever {
			res.Status = types.StatusReorgRolledBack
			res.Detail = fmt.Sprintf("message block at height %d was removed by a chain reorg; relay refused", m.SourceBlock)
			return res
		}
		res.Status = types.StatusInvalid
		res.Detail = fmt.Sprintf("message is not contained in any source block (height %d)", m.SourceBlock)
		return res
	}
	if depth < r.limits.RequiredDepth {
		res.Status = types.StatusPendingConfirmations
		res.Detail = fmt.Sprintf("confirmation depth %d < required %d at height %d", depth, r.limits.RequiredDepth, height)
		return res
	}

	// 2) Single-winner claim on the destination (replay vs concurrent race).
	acquired, st, detail := r.dst.TryAcquire(m.ID)
	if !acquired {
		res.Status = st
		res.Detail = detail
		return res
	}
	if r.acquiredHook != nil {
		r.acquiredHook(m.ID)
	}

	// 3) Rate and quota limits. The claim is released on rejection.
	st, detail, slot := r.buckets.reserve(r.now())
	if slot == nil {
		r.dst.Release(m.ID)
		res.Status = st
		res.Detail = detail
		return res
	}

	// 4) Ordered, replay-protected commit on the destination ledger.
	st, detail = r.dst.Commit(m)
	if st != types.StatusExecuted {
		r.buckets.cancel(slot)
		res.Status = st
		res.Detail = detail
		return res
	}
	r.buckets.commit(slot, r.now())
	res.Status = types.StatusExecuted
	res.Detail = detail
	return res
}
