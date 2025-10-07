package config

import (
	"bytes"
	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
)

type Topology struct {
	EShardIds []uint32            `json:"EShardIds"`
	PShardIds map[uint32][]uint32 `json:"PShardIds"`
}

type Config struct {
	Version string `json:"version"` // ShardFlag

	Topo            Topology            `json:"topology"`
	Nodes           map[uint32][]uint32 `json:"nodes"`
	NodeNumPerShard uint32              `json:"nodeNumPerShard"`
	NodeId          uint32              `json:"nodeId"`
	ShardId         uint32              `json:"shardId"`
	RShardId        uint32              `json:"RShardId"`

	IsLeader        bool `json:"isLeader"`
	IsMaliciousNode bool `json:"isBadNode"`

	RShardNum uint32 `json:"RShardNum"`
	PShardNum uint32 `json:"PShardNum"`

	ConsensusThreshold uint32 `json:"consensusThreshold"`

	Accounts   []common.Address `json:"accounts"`
	Bootnode   string           `json:"bootnode"`
	Rsinpsnum  uint32           `json:"rsinpsnum"`
	StartNode  uint32           `json:"startNode"`
	StartRS    uint32           `json:"startRS"`
	Speed      uint32           `json:"speed"`
	CrossRatio uint32           `json:"crossRatio"`
}

func NewConfig(version string, topo Topology, nodeNumPerShard, nodeId uint32, accounts []common.Address, bootnode string) *Config {

	version = "PEC-Shard-" + version

	nodes := make(map[uint32][]uint32)
	nodeIdCounter := uint32(1)
	// EShards
	for shardId := 1; shardId <= len(topo.EShardIds); shardId++ {
		nodes[uint32(shardId)] = make([]uint32, nodeNumPerShard)
		for i := 0; i < int(nodeNumPerShard); i++ {
			nodes[uint32(shardId)][i] = nodeIdCounter
			nodeIdCounter++
		}
	}
	// PShards
	for _, shardId := range topo.EShardIds {
		for _, pShardId := range topo.PShardIds[topo.EShardIds[shardId-1]] {
			nodes[pShardId] = make([]uint32, nodeNumPerShard)
			for i := 0; i < int(nodeNumPerShard); i++ {
				nodes[pShardId][i] = nodeIdCounter
				nodeIdCounter++
			}
		}
	}

	var shardId uint32
	a := (nodeId - 1) / nodeNumPerShard
	if int(a) < len(topo.EShardIds) {
		shardId = a + 1
	} else {
		shardId = a + 1001 - uint32(len(topo.EShardIds))
	}

	shardIds := []uint32{}
	var rShardId uint32
	for _, RShardId := range topo.EShardIds {
		shardIds = append(shardIds, topo.PShardIds[RShardId]...)
		for _, PShardId := range topo.PShardIds[RShardId] {
			if PShardId == shardId {
				rShardId = RShardId
			}
		}
	}

	consensusThreshold := nodeNumPerShard*2/3 + 1
	isLeader := false
	if nodeId%nodeNumPerShard == 0 {
		isLeader = true
	}

	rShardNum := uint32(len(topo.EShardIds))
	pShardNum := uint32(len(shardIds))
	rsinpsnum := nodeNumPerShard * pShardNum / rShardNum
	startRS := uint32(2)
	StartNode := rShardNum*nodeNumPerShard + rsinpsnum*startRS

	isMaliciousNode := false

	return &Config{
		Version:            version,
		Topo:               topo,
		Nodes:              nodes,
		NodeNumPerShard:    nodeNumPerShard,
		NodeId:             nodeId,
		ShardId:            shardId,
		RShardId:           rShardId,
		IsLeader:           isLeader,
		IsMaliciousNode:    isMaliciousNode,
		ConsensusThreshold: consensusThreshold,
		RShardNum:          rShardNum,
		PShardNum:          pShardNum,
		Accounts:           accounts,
		Bootnode:           bootnode,
		Rsinpsnum:          rsinpsnum,
		StartNode:          StartNode,
		StartRS:            startRS,
		Speed:              10000,
		CrossRatio:         5,
	}
}

func (cfg *Config) String() string {
	var prettyJSON bytes.Buffer
	data, _ := json.Marshal(cfg)

	json.Indent(&prettyJSON, data, "", "\t")

	return prettyJSON.String()
}
