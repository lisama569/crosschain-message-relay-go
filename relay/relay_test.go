package relay

import "testing"

func newTestService(cfg RelayConfig) (*Service, *SourceChain, *TargetChain) {
	src := NewSourceChain()
	dst := NewTargetChain()
	return NewService(src, dst, cfg), src, dst
}

func TestSkeleton(t *testing.T) {
	svc, _, _ := newTestService(RelayConfig{RequiredConfirmations: 1})
	r := svc.Submit("r1", Message{ID: "missing"}, 0)
	logResult(t, "missing", r)
	if r.Reason != FailNotFound {
		t.Fatalf("want not found, got %s", r.Reason)
	}
}

// logResult 打印消息标识、确认深度与中继执行结果。
func logResult(t *testing.T, phase string, r RelayResult) {
	t.Helper()
	t.Logf("[%s] message=%s confirmations=%d status=%s reason=%q executedBy=%s balance=%d",
		phase, r.MessageID, r.Confirmations, r.Status, string(r.Reason), r.ExecutedBy, r.Balance)
}
