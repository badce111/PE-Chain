package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"log"
	"time"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/p2p"

	"github.com/KyrinCode/Mitosis/core"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type ConfigData struct {
	RShardIds []uint32            `json:"EShardIds"`
	PShardIds map[uint32][]uint32 `json:"PShardIds"`
	Nodes     map[string][]uint32 `json:"Nodes"` // 添加 Nodes 字段
}

type BootNodeData struct {
	Bootnode string `json:"bootnode"`
}

var configData *ConfigData
var bootnodeData *BootNodeData

func LoadConfigFromFile(filePath string) (*ConfigData, error) {
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var configData ConfigData
	err = json.Unmarshal(file, &configData)
	if err != nil {
		return nil, err
	}

	return &configData, nil
}

func LoadBootnodeFromFile(filePath string) (*BootNodeData, error) {
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var bootnodeData BootNodeData
	err = json.Unmarshal(file, &bootnodeData)
	if err != nil {
		return nil, err
	}

	return &bootnodeData, nil
}

func NewConfig(nodeId uint32) *config.Config {
	if configData == nil {
		log.Fatalf("Config data not loaded")
	}

	if bootnodeData == nil {
		log.Fatalf("bootnode data not loaded")
	}

	topo := config.Topology{
		EShardIds: configData.RShardIds,
		PShardIds: configData.PShardIds,
	}

	bootnode := bootnodeData.Bootnode

	topo = config.Topology{
		EShardIds: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		PShardIds: map[uint32][]uint32{
			1:  {1001, 1002, 1003, 1004, 1005},
			2:  {1006, 1007, 1008, 1009, 1010},
			3:  {1011, 1012, 1013, 1014, 1015},
			4:  {1016, 1017, 1018, 1019, 1020},
			5:  {1021, 1022, 1023, 1024, 1025},
			6:  {1026, 1027, 1028, 1029, 1030},
			7:  {1031, 1032, 1033, 1034, 1035},
			8:  {1036, 1037, 1038, 1039, 1040},
			9:  {1041, 1042, 1043, 1044, 1045},
			10: {1046, 1047, 1048, 1049, 1050},
			11: {1051, 1052, 1053, 1054, 1055},
			12: {1056, 1057, 1058, 1059, 1060},
			13: {1061, 1062, 1063, 1064, 1065},
			14: {1066, 1067, 1068, 1069, 1070},
			15: {1071, 1072, 1073, 1074, 1075},
			16: {1076, 1077, 1078, 1079, 1080},
			17: {1081, 1082, 1083, 1084, 1085},
			18: {1086, 1087, 1088, 1089, 1090},
			19: {1091, 1092, 1093, 1094, 1095},
			20: {1096, 1097, 1098, 1099, 1100},
		},
	}

	accounts := make([]common.Address, 10) // 以太坊结构体切片，存储地址
	for i := 0; i < len(accounts); i++ {   // 生成10个以太坊地址
		data := int64(i)
		bytebuf := bytes.NewBuffer([]byte{})                // 创建缓冲区
		binary.Write(bytebuf, binary.BigEndian, data)       // 写入缓冲区（目标缓冲区指针，字节序（大端序列），写入的数据）
		a := crypto.Keccak256Hash(bytebuf.Bytes()).String() // 计算256哈希值（缓冲区字节切片）.哈希值转换为字符串形式
		accounts[i] = common.HexToAddress(a)
	}

	return config.NewConfig("test", topo, 4, nodeId, accounts, bootnode)
}

// 传入：165~664，连接其他节点，接收块消息，对块进行签名
func NewBFTProtocolWithId(nodeId uint32) *BFTProtocol {
	conf := NewConfig(nodeId)
	eb := eventbus.New()
	p2pNode := p2p.NewProtocol(eb, conf)
	bc := core.NewBlockchain(p2pNode, conf, eb)
	bft := NewBFTProtocol(p2pNode, eb, bc, conf)
	p2pNode.Start()
	bc.Server()
	bft.Server()
	return bft
}

// 并发 p2pNode.Start，返回：区块结构体（协议结构体<共识>，事件总线结构体<监听和回调>，配置结构体<分片的信息>，密钥结构体<验证>），传入：1~20
func NewBlockChainWithId(nodeId uint32) *core.Blockchain {
	conf := NewConfig(nodeId)
	eb := eventbus.New()                        // 事件总线结构体
	p2pNode := p2p.NewProtocol(eb, conf)        // 协议结构体（事件总线结构体，配置结构体），返回：协议结构体（p2p 节点，配置）
	bc := core.NewBlockchain(p2pNode, conf, eb) // 区块结构体（协议结构体，事件总线结构体，配置结构体），配置了密钥
	p2pNode.Start()                             // P2P，节点连接 bootnode 加入 topic
	return bc
}

func changeTopo(bfts []*BFTProtocol, startnode uint32, str uint32) {
	for _, bft := range bfts {
		bft.config.StartRS += str
		if bft.config.StartRS > bft.config.RShardNum {
			bft.config.StartRS = bft.config.RShardNum
		}
		bft.config.StartNode = startnode
		bft.blockchain.TxPool.NewTxPool(bft.config)
	}
}

func SetConfigData(data *ConfigData) {
	configData = data
}

func SetBootnodeData(data *BootNodeData) {
	bootnodeData = data
}

func main() {
	host := "host1"

	// 读取配置
	configData1, err := LoadConfigFromFile("../config/config.json")
	if err != nil {
		log.Fatal("读文件错误：", err)
	}
	configData = configData1

	// 读取 bootnode
	bootnodeData1, err := LoadConfigFromFile("../config/bootnode.json")
	if err != nil {
		log.Fatal("读文件错误：", err)
	}
	bootnodeData = bootnodeData1

	nodes := configData.Nodes[host]
	r_begin, r_end := nodes[0], nodes[1]
	p_begin, p_end := nodes[2], nodes[3]

	blockchains := []*core.Blockchain{}

	for nodeId := r_begin; nodeId <= r_end; nodeId++ {
		bc := NewBlockChainWithId(nodeId)
		bc.Server()
		blockchains = append(blockchains, bc)
	}

	startnode := r_end + blockchains[0].Config.Rsinpsnum*2

	var bfts []*BFTProtocol
	for nodeId := p_begin; nodeId <= p_end; nodeId++ {
		bft := NewBFTProtocolWithId(nodeId)
		if startnode >= nodeId {
			bft.Start()
		}
		bfts = append(bfts, bft)
	}
	str := uint32(1)

	for startnode < p_end {
		for _, tp := range bfts {
			le := tp.blockchain.TxPool.GetTxsLen()
			if le > 512 {
				for i := tp.config.Rsinpsnum * str; i > 0; i-- {
					if len(bfts) <= int(startnode-r_end) {
						break
					}
					bfts[startnode-r_end].Start2(bfts)
					startnode++
				}
				changeTopo(bfts, startnode, str)
				str++
				break
			}
		}
		time.Sleep(4 * time.Second)
	}

	for {
		time.Sleep(time.Minute)
	}
}
