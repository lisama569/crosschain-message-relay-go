package relay

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

type sourceBlock struct {
	num      uint64
	hash     string
	messages []Message
}

// SourceChain 用本地账本模拟源链：保存消息所在区块，并通过追加/回滚区块
// 模拟出块与链重组。确认深度 = 链头高度 - 消息所在高度 + 1。
type SourceChain struct {
	mu       sync.Mutex
	blocks   []*sourceBlock      // 按高度排列，blocks[h-1] 即高度 h
	msgBlock map[string]uint64   // 消息 ID -> 所在高度
	reorged  map[string]struct{} // 因重组被回滚的消息 ID
}

// NewSourceChain 创建源链账本。
func NewSourceChain() *SourceChain {
	return &SourceChain{
		msgBlock: make(map[string]uint64),
		reorged:  make(map[string]struct{}),
	}
}

func blockHash(num uint64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("source-block-%d", num)))
	return hex.EncodeToString(sum[:8])
}

// AppendBlock 追加一个新区块，登记其中的消息并回填区块高度/哈希。
// 重复的消息 ID 会被拒绝（防止测试构造时意外覆盖）。
func (s *SourceChain) AppendBlock(messages []Message) (uint64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	num := uint64(len(s.blocks)) + 1
	hash := blockHash(num)
	stored := make([]Message, 0, len(messages))
	for _, m := range messages {
		if _, ok := s.msgBlock[m.ID]; ok {
			panic(fmt.Sprintf("source chain: duplicate message %s in block %d", m.ID, num))
		}
		if _, bad := s.reorged[m.ID]; bad {
			panic(fmt.Sprintf("source chain: message %s was reorged and cannot be re-included", m.ID))
		}
		m.BlockNum = num
		m.BlockHash = hash
		stored = append(stored, m)
		s.msgBlock[m.ID] = num
	}
	s.blocks = append(s.blocks, &sourceBlock{num: num, hash: hash, messages: stored})
	return num, hash
}

// Tip 返回当前链头高度。
func (s *SourceChain) Tip() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.blocks))
}

// GetStatus 查询消息状态与确认深度。
func (s *SourceChain) GetStatus(messageID string) SourceStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	tip := uint64(len(s.blocks))
	if _, ok := s.reorged[messageID]; ok {
		return SourceStatus{Exists: false, Reorged: true, Confirmations: 0}
	}
	height, ok := s.msgBlock[messageID]
	if !ok {
		return SourceStatus{Exists: false, Confirmations: 0}
	}
	var msg Message
	for _, m := range s.blocks[height-1].messages {
		if m.ID == messageID {
			msg = m
			break
		}
	}
	confs := uint64(0)
	if tip >= height {
		confs = tip - height + 1
	}
	return SourceStatus{
		Message:       msg,
		Exists:        true,
		Confirmations: confs,
	}
}

// ReorgTo 模拟链重组：丢弃高度高于 keepHeight 的所有区块，
// 其中的消息全部标记为回滚并从活跃账本索引中移除。
// 返回被回滚的消息条数。
func (s *SourceChain) ReorgTo(keepHeight uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if keepHeight >= uint64(len(s.blocks)) {
		return 0
	}
	removed := 0
	for h := keepHeight + 1; h <= uint64(len(s.blocks)); h++ {
		for _, m := range s.blocks[h-1].messages {
			s.reorged[m.ID] = struct{}{}
			delete(s.msgBlock, m.ID)
			removed++
		}
		s.blocks[h-1] = nil
	}
	s.blocks = s.blocks[:keepHeight]
	return removed
}
