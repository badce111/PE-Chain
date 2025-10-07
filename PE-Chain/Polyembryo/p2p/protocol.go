package p2p

import (
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/types"
	"github.com/ethereum/go-ethereum/common"
)

// Protocol
type Protocol struct {
	node   *P2PNode
	config *config.Config
}

// NewProtocol create a new Protocol object
func NewProtocol(broker *eventbus.EventBus, config *config.Config) *Protocol {
	br := NewBaseReader(broker)
	node := NewP2PNode(br, config)
	return &Protocol{
		node:   node,
		config: config,
	}
}

func (p *Protocol) Start() {
	go p.node.Launch() // 并发进程
}

func (p *Protocol) Gossip(msg []byte, shardId uint32) {
	p.node.Gossip(msg, shardId)
}

// GShard only
func (p *Protocol) GossipAll(msg []byte) {
	if p.node.conf.ShardId != 0 {
		return
	}
	p.node.GossipAll(msg)
}

func (p *Protocol) Gossip_validator(bftMessage *types.BFTMessage, shardId uint32, length1 int, phase int) {
	p.node.Gossip_validator(bftMessage, shardId, length1, phase)
}

func (p *Protocol) Transfer(shardId uint32, nodeId uint32, block_height uint32, block_hash common.Hash, rshardId uint32) {
	p.node.Transfer1(shardId, nodeId, block_height, block_hash, rshardId)
}
