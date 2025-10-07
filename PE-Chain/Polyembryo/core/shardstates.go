package core

import (
	mitosisbls "github.com/KyrinCode/Mitosis/bls"
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/trie"
	"github.com/KyrinCode/Mitosis/types"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"
)

type ShardState struct {
	Headers       map[common.Hash]*types.Header
	InboundChunks map[common.Hash]*types.OutboundChunk
	mu            sync.Mutex
}

func NewShardState(c *config.Config) *ShardState {
	shardState := &ShardState{
		Headers:       make(map[common.Hash]*types.Header),
		InboundChunks: make(map[common.Hash]*types.OutboundChunk),
	}
	return shardState
}

func (s *ShardState) UpdateShardStateWithInboundChunk(inboundChunk *types.OutboundChunk) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.InboundChunks[inboundChunk.BlockHash]
	if !ok {
		s.InboundChunks[inboundChunk.BlockHash] = inboundChunk
	} else {
		return false, false
	}

	checked := false
	header, ok := s.Headers[inboundChunk.BlockHash] // check header exists or not, if exists: verify
	if ok {
		checked = s.CheckInboundChunk(header, inboundChunk)
		if checked {
			logChain.Infof("已成功检查入站块，来自 shard-%d block-%s。", inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
		}
	}
	return true, checked // if both header and inboundChunk: return true for outside function to decide whether put into txpool (leader)
}

func (s *ShardState) UpdateShardStateWithHeader(header *types.Header) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.Headers[header.Hash]
	if !ok {
		s.Headers[header.Hash] = header
	} else {
		return false, false
	}

	checked := false
	inboundChunk, ok := s.InboundChunks[header.Hash]
	if ok {
		checked = s.CheckInboundChunk(header, inboundChunk)
		if checked {
			logChain.Infof("已成功检查入站块，来自 shard-%d block-%s。", inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
		}
	}
	return true, checked // if both header and inboundChunk: return true for outside function to decide whether put into txpool (leader)
}

func (s *ShardState) UpdateShardStateWithHeaderRShard(header *types.Header) bool {
	// RShard only record header here
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.Headers[header.Hash]
	if !ok {
		s.Headers[header.Hash] = header
		return true
	} else {
		return false
	}
}

func (s *ShardState) CheckInboundChunk(header *types.Header, inboundChunk *types.OutboundChunk) bool {
	if len(inboundChunk.Txs) != 0 {
		key, _ := rlp.EncodeToBytes(inboundChunk.Txs[0].ToShard) // key 是 shardId
		chunkRoot := inboundChunk.Root()
		db := memorydb.New()
		for j := 0; j < len(inboundChunk.ChunkProof); j++ {
			db.Put(crypto.Keccak256(inboundChunk.ChunkProof[j]), inboundChunk.ChunkProof[j])
		}
		hash, _ := trie.VerifyProof(header.TxRoot, key, db)

		if common.BytesToHash(hash) != chunkRoot {
			logChain.Errorf("Chunk proof from shard-%d block-%s is incorrect.", inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
			return false
		}
	}
	return true
}

type ShardStates struct {
	StateDB   map[uint32]*ShardState
	stateDBMu sync.Mutex

	txPool *TxPool

	//BLS
	BLSSign *mitosisbls.BLS
	Config  *config.Config
}

func NewShardStates(conf *config.Config, txpool *TxPool, bls *mitosisbls.BLS) *ShardStates {
	s := &ShardStates{
		StateDB: make(map[uint32]*ShardState),
		txPool:  txpool,
		BLSSign: bls,
		Config:  conf,
	}

	for i := uint32(0); i < conf.PShardNum; i++ {
		s.StateDB[i+1001] = NewShardState(conf)
	}
	return s
}

func (s *ShardStates) UpdateHeader(msg types.Header) bool {
	s.stateDBMu.Lock()
	defer s.stateDBMu.Unlock()

	first, checked := false, false
	st, ok := s.StateDB[msg.ShardId]
	if ok && s.CheckHeader(msg) {
		if s.Config.ShardId > 1000 {
			first, checked = st.UpdateShardStateWithHeader(&msg)
		} else {
			first = st.UpdateShardStateWithHeaderRShard((&msg))
		}
	}
	if checked && s.Config.IsLeader {
		inboundChunk := st.InboundChunks[msg.Hash]
		s.AddInboundChunkToTxPool(inboundChunk)
	}
	return first
}

// 块头达成签名共识
func (s *ShardStates) CheckHeader(msg types.Header) bool {
	consensusThreshold := s.Config.ConsensusThreshold
	if len(msg.SignBitMap.GetElement()) < int(consensusThreshold) {
		logChain.Errorf("Header-%d-%d 的签名不够, (%d / %d)", msg.ShardId, msg.Height, len(msg.SignBitMap.GetElement()), consensusThreshold)
		return false
	}
	offset := s.getKeyOffset(msg.ShardId)
	if !s.BLSSign.VerifyAggregateSig(msg.Signature, msg.Hash.Bytes(), msg.SignBitMap, msg.ShardId, offset) {
		if s.BLSSign.PubKeys.GetAggregatePubKey(msg.ShardId, msg.SignBitMap, offset) == nil {
			logChain.Infof("[Node %d-%d] CrossMSG Error: Block %d-%d signature is incorrect !!! NoPUB!!, Nodes: %v",
				s.Config.ShardId, s.Config.NodeId, msg.ShardId, msg.Height, msg.SignBitMap.GetElement())
			for _, i := range msg.SignBitMap.GetElement() {
				pub := s.BLSSign.GetPubKeyWithNodeId(msg.ShardId, i)
				logChain.Infof("[Node %d-%d] CrossMSG Error: Block %d-%d signature is incorrect !!! NoPUB!!, Nodes %d-%d Pub: %v",
					s.Config.ShardId, s.Config.NodeId, msg.ShardId, msg.Height, msg.ShardId, i, pub)
			}

		} else {
			logChain.Infof("[Node %d-%d] CrossMSG Error: Block %d-%d signature is incorrect !!! SignMsg: %s, Sign: %v, AggPub: %v, ValidateResult:%v",
				s.Config.ShardId, s.Config.NodeId, msg.ShardId, msg.Height, msg.Hash.String(), msg.Signature,
				s.BLSSign.PubKeys.GetAggregatePubKey(msg.ShardId, msg.SignBitMap, offset).Serialize(), s.BLSSign.VerifyAggregateSig(msg.Signature, msg.Hash.Bytes(), msg.SignBitMap, msg.ShardId, offset))

		}
		return false
	}
	return true
}

func (s *ShardStates) UpdateInboundChunk(msg types.OutboundChunk) bool {
	s.stateDBMu.Lock()
	defer s.stateDBMu.Unlock()

	first, checked := false, false
	st, ok := s.StateDB[msg.Txs[0].FromShard]
	if ok {
		first, checked = st.UpdateShardStateWithInboundChunk(&msg)
	}
	if checked && s.Config.IsLeader {
		s.AddInboundChunkToTxPool(&msg)
	}
	return first
}

func (s *ShardStates) AddInboundChunkToTxPool(inboundChunk *types.OutboundChunk) {
	if len(inboundChunk.Txs) > 0 {
		s.txPool.AddInboundChunk(inboundChunk)
		logChain.Infof("[Node %d-%d] 入站块来自 block-%s 共 %d 个交易已提交到 txpool",
			s.Config.ShardId, s.Config.NodeId, inboundChunk.BlockHash, len(inboundChunk.Txs))
	}
}

func (s *ShardStates) getKeyOffset(shardId uint32) uint32 {
	return (s.Config.RShardNum+shardId-1001)*s.Config.NodeNumPerShard + 1
}
