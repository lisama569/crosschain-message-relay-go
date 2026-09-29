package relay

import (
	"errors"
	"fmt"
	"time"
)

// Message 是跨链消息的最小载体。
type Message struct {
	ID        string // 消息唯一标识（源链交易哈希等）
	From      string // 源账户
	To        string // 目标账户
	Amount    int64  // 金额（本地账本使用整数金额）
	Sequence  uint64 // 目标链上的严格递增序列号
	BlockHash string // 消息所在源链区块哈希
	BlockNum  uint64 // 消息所在源链区块高度
}

// 链上消息状态。
const (
	StatusPending   = "PENDING"   // 已提交但未达确认深度
	StatusConfirmed = "CONFIRMED" // 已达确认深度，可中继
	StatusReorged   = "REORGED"   // 所在区块因链重组被丢弃
	StatusRelayed   = "RELAYED"   // 已在目标链执行
	StatusRejected  = "REJECTED"  // 明确拒绝（重放、序列号冲突等）
)

// FailReason 是可区分的失败原因码。
type FailReason string

const (
	FailNone             FailReason = ""                  // 非失败
	FailNotFound         FailReason = "MESSAGE_NOT_FOUND" // 源链账本中不存在该消息
	FailUnconfirmed      FailReason = "UNCONFIRMED"       // 未达到要求的确认深度
	FailReorged          FailReason = "REORGED"           // 消息所在区块被链重组回滚
	FailAlreadyRelayed   FailReason = "ALREADY_RELAYED"   // 重复投递：消息已执行
	FailSequenceConflict FailReason = "SEQUENCE_CONFLICT" // 序列号已被其他消息占用
	FailSequenceGap      FailReason = "SEQUENCE_GAP"      // 序列号空缺（前序消息尚未执行）
	FailSequenceRollback FailReason = "SEQUENCE_ROLLBACK" // 序列号回退（持有同一消息的过期副本）
	FailPayloadMismatch  FailReason = "PAYLOAD_MISMATCH"  // 中继副本与源链规范化内容不一致
	FailRaceLost         FailReason = "RELAY_RACE_LOST"   // 多中继器竞争失败
	FailRateLimited      FailReason = "RATE_LIMITED"      // 超过每周期执行速率
	FailQuotaExceeded    FailReason = "QUOTA_EXCEEDED"    // 超过总额度
	FailInsufficientFund FailReason = "INSUFFICIENT_FUND" // 目标账户余额不足
)

// RelayError 携带可区分失败原因的错误。
type RelayError struct {
	Reason FailReason
	Msg    string
}

func (e *RelayError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Msg)
}

// NewRelayError 构造一个带原因码的错误。
func NewRelayError(reason FailReason, format string, args ...any) *RelayError {
	return &RelayError{Reason: reason, Msg: fmt.Sprintf(format, args...)}
}

// ReasonOf 从错误中提取 FailReason，普通错误返回空原因码。
func ReasonOf(err error) FailReason {
	var re *RelayError
	if errors.As(err, &re) {
		return re.Reason
	}
	return FailNone
}

// RelayResult 是一次中继提交的结论。
type RelayResult struct {
	MessageID     string
	Status        string
	Reason        FailReason
	Confirmations uint64 // 提交时该消息在源链上的确认深度
	ExecutedBy    string // 实际执行成功的中继器标识
	Balance       int64  // 执行后目标账户余额（便于日志解释）
}

// SourceStatus 描述一条消息在源链上的查询结论。
type SourceStatus struct {
	Message       Message
	Exists        bool
	Confirmations uint64
	Finalized     bool
	Reorged       bool
}

// RelayConfig 中继服务配置。
type RelayConfig struct {
	RequiredConfirmations uint64        // 要求的源链确认深度
	RatePerEpoch          int           // 每个周期最多执行条数
	Epoch                 time.Duration // 一个速率周期长度
	TotalQuota            int           // 服务生命周期内最多执行条数
}

// Snapshot 用于可解释地输出当前账本与消息状态。
type Snapshot struct {
	MessageID     string
	SourceStatus  string
	Confirmations uint64
	TargetStatus  string
	Balance       int64
}
