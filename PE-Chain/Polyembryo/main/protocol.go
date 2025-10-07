package main

import (
	"fmt"
	"sync"
	"time"

	mitosisbls "github.com/KyrinCode/Mitosis/bls"
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/core"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/message"
	"github.com/KyrinCode/Mitosis/p2p"
	"github.com/KyrinCode/Mitosis/topics"
	"github.com/KyrinCode/Mitosis/types"

	"github.com/ethereum/go-ethereum/common"
	"github.com/herumi/bls-eth-go-binary/bls"
	logger "github.com/sirupsen/logrus"
)

var logBFT = logger.WithField("process", "consensus")

type BFTPhase uint

const (
	BFT_INIT BFTPhase = iota
	BFT_PREPARE
	BFT_PRECOMMIT
	BFT_COMMIT
)

type Phase struct {
	height   uint32   // 块高度
	bftPhase BFTPhase // bft 阶段
}

func (p *Phase) Switch(height uint32, bftPhase BFTPhase) {
	if p.height > height || (p.height == height && p.bftPhase >= bftPhase) {
		return
	}
	p.height = height
	p.bftPhase = bftPhase
}

func (p *Phase) IsNew(x Phase) bool { // if x is later than p?
	if p.height > x.height || (p.height == x.height && p.bftPhase >= x.bftPhase) {
		return false
	}
	return true
}

// used to rebroadcast msgs until outdated
func (p *Phase) IsOut(x Phase) bool {
	if p.height > x.height || (p.height == x.height && p.bftPhase > x.bftPhase) {
		return true
	}
	return false
}

type BFTProtocol struct {
	node       *p2p.Protocol
	broker     *eventbus.EventBus
	bls        *mitosisbls.BLS
	blockchain *core.Blockchain
	config     *config.Config

	phase      Phase
	startphase Phase
	mu         sync.Mutex

	latestCommittedBlockHash common.Hash
	currentBlock             *types.Block
	aggregatePrepareSig      *bls.Sign
	aggregatePrecommitSig    *bls.Sign
	aggregateCommitSig       *bls.Sign
	prepareBitmap            *types.Bitmap
	precommitBitmap          *types.Bitmap
	commitBitmap             *types.Bitmap

	bftMsgQueue chan message.Message
	bftMsgSubID uint32
}

func NewBFTProtocol(p2pNode *p2p.Protocol, eb *eventbus.EventBus, blockchain *core.Blockchain, config *config.Config) *BFTProtocol {

	return &BFTProtocol{
		node:         p2pNode,
		broker:       eb,
		blockchain:   blockchain,
		bls:          blockchain.BLS,
		config:       config,
		phase:        Phase{0, BFT_INIT},
		startphase:   Phase{0, BFT_INIT},
		currentBlock: nil,
		bftMsgQueue:  make(chan message.Message, 1000000),
	}
}

func (bft *BFTProtocol) Server() {
	if bft.config.IsLeader {
		bft.bftMsgSubID = bft.broker.Subscribe(topics.ConsensusLeader, eventbus.NewChanListener(bft.bftMsgQueue))
	} else {
		bft.bftMsgSubID = bft.broker.Subscribe(topics.ConsensusValidator, eventbus.NewChanListener(bft.bftMsgQueue))
	}
	go bft.HandleBFTMsg()
}

func (bft *BFTProtocol) HandleBFTMsg() {
	for data := range bft.bftMsgQueue {
		bftMsg := data.Payload().(types.BFTMessage)
		var err error
		if bft.config.IsLeader {
			switch bftMsg.MessageType {
			case types.MessageType_PREPAREVOTE:
				err = bft.OnPrepareVoteMsg(bftMsg)
			case types.MessageType_PRECOMMITVOTE:
				err = bft.OnPrecommitVoteMsg(bftMsg)
			case types.MessageType_COMMITVOTE:
				err = bft.OnCommitVoteMsg(bftMsg)
			}
		} else {
			switch bftMsg.MessageType {
			case types.MessageType_PREPARE:
				err = bft.OnPrepareMsg(bftMsg)
			case types.MessageType_PRECOMMIT:
				err = bft.OnPrecommitMsg(bftMsg)
			case types.MessageType_COMMIT:
				err = bft.OnCommitMsg(bftMsg)
			}
		}
		if err != nil {
			logBFT.WithError(err).Error(fmt.Sprintf("[Node-%d-%d] 进程 block-%d-%d-%x，类型 %d 错误",
				bft.config.ShardId, bft.config.NodeId, bftMsg.Block.ShardId, bftMsg.BlockNum, bftMsg.BlockHash, bftMsg.MessageType))
		}
	}
	logBFT.Infof("[Node-%d-%d] 停!!!", bft.config.ShardId, bft.config.NodeId)
}

func (bft *BFTProtocol) Start() {
	if bft.config.IsLeader {
		go func() {
			if bft.blockchain.LatestBlock != nil {
				bft.currentBlock = bft.blockchain.LatestBlock
				bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash
			}
			logBFT.Infof("[Node-%d-%d] 开始共识", bft.config.ShardId, bft.config.NodeId)
			time.Sleep(5 * time.Minute)
			bft.Prepare()
		}()
	} else {
		if bft.blockchain.LatestBlock != nil {
			bft.currentBlock = bft.blockchain.LatestBlock
			bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash
		}
	}
}

func (bft *BFTProtocol) Start2(bfts []*BFTProtocol) {
	bft.mu.Lock()
	bft.startphase = bfts[0].phase
	bft.mu.Unlock()
	if bft.config.IsLeader {
		go func() {
			if bft.blockchain.LatestBlock != nil {
				bft.currentBlock = bft.blockchain.LatestBlock
				bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash
			}
			logBFT.Infof("[Node-%d-%d] 开始共识", bft.config.ShardId, bft.config.NodeId)
			bft.Prepare()
		}()
	} else {
		if bft.blockchain.LatestBlock != nil {
			bft.currentBlock = bft.blockchain.LatestBlock
			bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash
		}
	}
}

func (bft *BFTProtocol) TryGossip(phase Phase, bftMessage *types.BFTMessage, topic topics.Topic) {
	for {
		msg := message.NewBlockchainMessage(topic, *bftMessage)
		msgBytes, _ := msg.MarshalBinary()
		bft.node.Gossip(msgBytes, bft.config.ShardId)
		bftMessage.Time = time.Now()
		// 去掉区块哈希 %x bftMessage.BlockHash
		logBFT.Infof("[Node-%d-%d] gossip BFT 消息<block-%d-%d，type-%d>，大小：%d。",
			bft.config.ShardId, bft.config.NodeId, bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.MessageType, len(msgBytes))
		time.Sleep(15 * time.Second)
		bft.mu.Lock()
		out := bft.phase.IsOut(phase)
		bft.mu.Unlock()
		if !out {
			continue
		} else {
			return
		}
	}
}

func (bft *BFTProtocol) nodeId2Key() uint32 {
	return bft.config.NodeId - (bft.config.RShardNum+bft.config.ShardId-1001)*bft.config.NodeNumPerShard - 1
}

func (bft *BFTProtocol) key2NodeId(key uint32) uint32 {
	return key + (bft.config.RShardNum+bft.config.ShardId-1001)*bft.config.NodeNumPerShard + 1
}

func (bft *BFTProtocol) getKeyOffset() uint32 {
	return (bft.config.RShardNum+bft.config.ShardId-1001)*bft.config.NodeNumPerShard + 1
}

func (bft *BFTProtocol) TryGossip_validator(phase Phase, bftMessage *types.BFTMessage, topic topics.Topic) {
	msg := message.NewBlockchainMessage(topic, *bftMessage)
	msgBytes, _ := msg.MarshalBinary()
	logBFT.Infof("[Node-%d-%d] gossip bft msg <block-%d-%d-%x, type-%d>, size: %d.", bft.config.ShardId, bft.config.NodeId, bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash, bftMessage.MessageType, len(msgBytes))
	bft.node.Gossip_validator(bftMessage, bft.config.ShardId, len(msgBytes), int(phase.bftPhase))
}
