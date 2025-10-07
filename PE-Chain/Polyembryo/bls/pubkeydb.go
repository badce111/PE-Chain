package mitosisbls

import (
	"encoding/json"
	logger "github.com/sirupsen/logrus"
	"os"
	"sync"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/types"

	"github.com/ethereum/go-ethereum/log"
	"github.com/herumi/bls-eth-go-binary/bls"
)

// 节点公钥结构体，含互斥锁
type PubKeyDB struct {
	DB    map[uint32]*bls.PublicKey // key: nodeId, value: 公钥指针
	mutex sync.Mutex
}

// 返回：节点公钥结构体
func NewPubKeyDB() *PubKeyDB {
	return &PubKeyDB{
		DB: make(map[uint32]*bls.PublicKey),
	}
}

func (p *PubKeyDB) Add(nodeId uint32, pubByte []byte) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	var pubKey bls.PublicKey
	addNewKey := false
	err := pubKey.Deserialize(pubByte)
	if err != nil {
		return false
	}
	if _, ok := p.DB[nodeId]; !ok {
		addNewKey = true
	}
	p.DB[nodeId] = &pubKey

	return addNewKey
}

// 获取签名节点的公钥
func (p *PubKeyDB) GetAggregatePubKey(bitmap types.Bitmap, offset uint32) *bls.PublicKey {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	elements := bitmap.GetElement()
	var pubKey bls.PublicKey
	for _, i := range elements {
		if pbk, ok := p.DB[i+offset]; ok {
			pubKey.Add(pbk)
		} else {
			log.Error("PubKey %d is missing", i)
		}
	}
	return &pubKey
}

func (p *PubKeyDB) GetPubKey(nodeId uint32) *bls.PublicKey {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	ps, ok := p.DB[nodeId]
	if !ok {
		return nil
	}
	return ps
}

// 节点 id 映射公钥
func (p *PubKeyDB) Reset(nodeIds []uint32) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	cert, success := LoadCert("../cert/cert.json")
	if !success {
		log.Error("cert.json 缺失")
		return
	}

	bls.Init(bls.BLS12_381)            // 在调用所有其他操作之前调用此函数
	bls.SetETHmode(bls.EthModeDraft07) // 设置哈希函数
	for _, nodeId := range nodeIds {
		var pubKey bls.PublicKey
		// 从 cert 中得到 nodeId 到 pubKey 的映射关系，赋值给 pubKey
		err := pubKey.DeserializeHexStr(cert[nodeId])
		if err != nil {
			log.Error("反序列化公钥错误")
		}

		p.DB[nodeId] = &pubKey
	}
}

func LoadCert(filename string) (map[uint32]string, bool) {
	cert := make(map[uint32]string)

	data, err := os.ReadFile(filename)
	if err != nil {
		logger.Error("读 cert.json 文件错误")
		return cert, false
	}
	certJson := []byte(data)
	err = json.Unmarshal(certJson, &cert)
	if err != nil {
		logger.Error("解组 json 数据错误")
		return cert, false
	}

	return cert, true
}

// 分片公钥结构体，含互斥锁
type PubKeyStore struct {
	PubKeyDBs map[uint32]*PubKeyDB
	mutex     sync.Mutex
}

func NewPubKeyStore(topo config.Topology) *PubKeyStore {
	pubKeyDBs := make(map[uint32]*PubKeyDB)
	// EShards
	for shardId := 0; shardId <= len(topo.EShardIds); shardId++ {
		pubKeyDBs[uint32(shardId)] = NewPubKeyDB()
	}
	// PShards
	for _, shardId := range topo.EShardIds {
		for _, pShardId := range topo.PShardIds[topo.EShardIds[shardId-1]] {
			pubKeyDBs[pShardId] = NewPubKeyDB()
		}
	}
	return &PubKeyStore{
		PubKeyDBs: pubKeyDBs,
	}
}

func (p *PubKeyStore) AddPubKey(shardId, nodeId uint32, pubByte []byte) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	pubKeyDB, ok := p.PubKeyDBs[shardId]
	if !ok {
		pubKeyDB = NewPubKeyDB()
	}
	addNewKey := pubKeyDB.Add(nodeId, pubByte)
	p.PubKeyDBs[shardId] = pubKeyDB
	return addNewKey
}

// 获取签名节点的公钥
func (p *PubKeyStore) GetAggregatePubKey(shardId uint32, bitmap types.Bitmap, offset uint32) *bls.PublicKey {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	pubKeyDB, ok := p.PubKeyDBs[shardId] // 获得分片的公钥切片的地址
	if !ok {
		return nil
	}
	return pubKeyDB.GetAggregatePubKey(bitmap, offset)
}

// 获取节点公钥
func (p *PubKeyStore) GetPubKey(shardId, nodeId uint32) *bls.PublicKey {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	pubKeyDB, ok := p.PubKeyDBs[shardId]
	if !ok {
		return nil
	}
	return pubKeyDB.GetPubKey(nodeId)
}

// 分片 id 映射节点公钥结构体
func (p *PubKeyStore) Reset(nodes map[uint32][]uint32) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	// 直接遍历 nodes map
	for shardId, nodeIds := range nodes {
		pubKeyDB, ok := p.PubKeyDBs[shardId]
		if !ok {
			pubKeyDB = NewPubKeyDB()

		}
		pubKeyDB.Reset(nodeIds) // 节点映射
		p.PubKeyDBs[shardId] = pubKeyDB
	}
}
