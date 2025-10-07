package core

import (
	"bytes"
	"errors"
	"fmt"
	mitosisbls "github.com/KyrinCode/Mitosis/bls"
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/message"
	"github.com/KyrinCode/Mitosis/p2p"
	"github.com/KyrinCode/Mitosis/state"
	"github.com/KyrinCode/Mitosis/topics"
	"github.com/KyrinCode/Mitosis/trie"
	"github.com/KyrinCode/Mitosis/types"
	"os"
	"sync"
	"time"

	"github.com/emirpasic/gods/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"

	logger "github.com/sirupsen/logrus"
)

var logChain = logger.WithField("process", "blockchain")

type ProofList [][]byte

func (n *ProofList) Put(key []byte, value []byte) error {
	*n = append(*n, value)
	return nil
}

func (n *ProofList) Delete(key []byte) error {
	panic("not supported")
}

type Blockchain struct {
	Node        *p2p.Protocol
	BLS         *mitosisbls.BLS
	ShardStates *ShardStates
	broker      *eventbus.EventBus

	// Block DataBase
	LatestBlock *types.Block
	BlockDB     map[uint32]*types.Block

	// PShard
	StateDB         *state.StateDB
	stateMutex      sync.Mutex
	TxPool          *TxPool
	OutboundChunkDB map[common.Hash][]types.OutboundChunk

	// network message channel
	HeaderMsgQueue       chan message.Message
	InboundChunkMsgQueue chan message.Message

	HeaderMsgSubID       uint32
	InboundChunkMsgSubID uint32

	Config *config.Config
}

func NewBlockchain(node *p2p.Protocol, config *config.Config, eb *eventbus.EventBus) *Blockchain {
	chain := &Blockchain{
		Node:            node,
		broker:          eb,
		LatestBlock:     nil,
		BlockDB:         make(map[uint32]*types.Block),
		OutboundChunkDB: make(map[common.Hash][]types.OutboundChunk),
		Config:          config,
	}

	filename := "../bls/keygen/keystore/node" + fmt.Sprint(config.NodeId) + ".keystore"
	kp, success := mitosisbls.LoadKeyPair(filename)
	if success == false {
		logChain.Errorln("加载密钥错误")
	}

	chain.BLS = mitosisbls.NewBLS(*kp, config.Topo, config.Nodes)

	chain.StateDB, _ = state.New(common.Hash{}, state.NewDatabase(rawdb.NewMemoryDatabase()))
	chain.TxPool = NewTxPool(config)
	chain.ShardStates = NewShardStates(config, chain.TxPool, chain.BLS)

	return chain
}

func (chain *Blockchain) Genesis() *types.Block {
	chain.stateMutex.Lock()
	defer chain.stateMutex.Unlock()

	accounts := chain.Config.Accounts
	for _, addr := range accounts {
		chain.StateDB.AddBalance(addr, 10000000)
	}
	root, _ := chain.StateDB.Commit(false)
	head := types.NewHeader(chain.Config.ShardId, 0, common.Hash{}, root, common.Hash{}, types.NewBitmap(chain.Config.PShardNum), uint64(time.Now().UnixMilli()))
	block := types.NewBlock(*head, nil, nil)
	chain.LatestBlock = block
	return block

}

func (chain *Blockchain) CreateNewBlock() *types.Block {
	if chain.LatestBlock == nil {
		return chain.Genesis()
	}

	chain.stateMutex.Lock()
	defer chain.stateMutex.Unlock()

	var txs []types.Transaction
	var inboundChunks []types.OutboundChunk

	txs, inboundChunks = chain.TxPool.takeTxs(1024)

	txnum := 0
	for _, tx := range txs {
		if tx.FromShard != tx.ToShard {
			logChain.Infof("[Node-%d-%d] 已生成从分片 %d 到 %d 的跨分片交易，FromAddr：%s，ToAddr：%s，Value：%d",
				chain.Config.ShardId, chain.Config.NodeId, tx.FromShard, tx.ToShard, tx.FromAddr.Hex(), tx.ToAddr.Hex(), tx.Value)
		} else {
			txnum++
		}
	}

	for _, inboundChunk := range inboundChunks {
		for _, tx := range inboundChunk.Txs {
			if tx.FromShard != chain.Config.ShardId && tx.ToShard == chain.Config.ShardId {
				logChain.Infof("[Node-%d-%d] 账户余额增加：%d，交易哈希：%x", chain.Config.ShardId, chain.Config.NodeId, tx.Value, tx.Hash)
				chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
			}
		}
	}

	outboundDstBitmap := types.NewBitmap(chain.Config.PShardNum)

	// intra-shard or outbound txs
	for _, tx := range txs {
		if tx.FromShard == chain.Config.ShardId && tx.ToShard == chain.Config.ShardId {
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
			chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
		}
		if tx.FromShard == chain.Config.ShardId && tx.ToShard != chain.Config.ShardId {
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
			outboundDstBitmap.SetKey(tx.ToShard - 1001)
		}
	}

	stateRoot, _ := chain.StateDB.Commit(false)
	outboundChunks := make([]types.OutboundChunk, chain.Config.PShardNum)
	for _, tx := range txs {
		outboundChunks[tx.ToShard-1001].Txs = append(outboundChunks[tx.ToShard-1001].Txs, tx)
	}

	// merkle tree
	txTrie := new(trie.Trie)
	txRoot := types.GetBlockTxRoot(outboundChunks, txTrie)

	height := chain.LatestBlock.Height + 1
	prevHash := chain.LatestBlock.GetHash()
	head := types.NewHeader(chain.Config.ShardId, height, prevHash, stateRoot, txRoot, outboundDstBitmap, uint64(time.Now().UnixMilli()))

	keyBuf := new(bytes.Buffer)
	for i := range outboundChunks {
		var px ProofList
		keyBuf.Reset()
		err := rlp.Encode(keyBuf, uint(i+1001))
		if err != nil {
			return nil
		}
		err = txTrie.Prove(keyBuf.Bytes(), 0, &px)
		if err != nil {
			return nil
		}
		outboundChunks[i].ChunkProof = px
		outboundChunks[i].BlockHash = head.Hash
	}

	block := types.NewBlock(*head, txs, inboundChunks)
	for _, tx := range txs {
		if tx.FromShard != tx.ToShard {
			logChain.Infof("[Node-%d-%d] 从分片 %d 到 %d 交易，已由领导者打包到块中，tx.hash 为 %x, block.hash 为 %x",
				chain.Config.ShardId, chain.Config.NodeId, tx.FromShard, tx.ToShard, tx.Hash, block.Hash)
		}
	}
	chain.LatestBlock = block
	chain.OutboundChunkDB[block.Hash] = outboundChunks
	return block
}

// 出站块写入文件
func writeFileInGoroutine(fileName string, data []byte, permissions os.FileMode) {
	err := os.WriteFile(fileName, data, permissions)
	if err != nil {
		panic(err)
	}
}

// leader 落盘区块
func (chain *Blockchain) Commit(block *types.Block) {
	logChain.Infof("[Node-%d-%d] 提交 block-%d-%d",
		chain.Config.ShardId, chain.Config.NodeId, block.ShardId, block.Height)
	chain.LatestBlock = block
	chain.BlockDB[block.Height] = block

	if chain.Config.IsLeader {
		jsonByte := block.MarshalJson()
		blockName := "../blocks/" + utils.ToString(chain.Config.ShardId) + "-" + utils.ToString(block.Height) + ".json"
		go writeFileInGoroutine(blockName, jsonByte, 0644)
	}
	if ok := chain.RollBack(block.StateRoot); !ok {
		chain.ExecuteBlock(*block)
	}

	chain.ShardStates.UpdateHeader(block.Header)

	// send header to RShard
	headerMsg := message.NewBlockchainMessage(topics.HeaderGossip, block.Header)
	headerMsgByte, _ := headerMsg.MarshalBinary()
	chain.Node.Gossip(headerMsgByte, chain.Config.RShardId)
	logChain.Infof("[Node-%d-%d] block-%d-%d 的块头已经发送到 RShard-%d",
		chain.Config.ShardId, chain.Config.NodeId, chain.Config.ShardId, block.Height, chain.Config.RShardId)
	// send crossShardMsg to dst PShard
	for i, outboundChunk := range chain.OutboundChunkDB[block.Hash] {
		if uint32(i)+1001 == chain.Config.ShardId { // skip intra-shard
			continue
		}
		if len(outboundChunk.Txs) > 0 { // skip 0 txs chunk
			outboundChunkMsg := message.NewBlockchainMessage(topics.OutboundChunkGossip, outboundChunk)
			outboundChunkMsgByte, _ := outboundChunkMsg.MarshalBinary()
			chain.Node.Gossip(outboundChunkMsgByte, uint32(i)+1001)
			logChain.Infof("[Node-%d-%d] block-%d-%d 的出站块已经发送到 PShard-%d 带有 %d 个交易(大小：%d)",
				chain.Config.ShardId, chain.Config.NodeId, chain.Config.ShardId, block.Height, uint32(i)+1001, len(outboundChunk.Txs), len(outboundChunkMsgByte))

		}
	}
}

func (chain *Blockchain) Server() {
	chain.HeaderMsgQueue = make(chan message.Message, 1000000)
	chain.HeaderMsgSubID = chain.broker.Subscribe(topics.HeaderGossip, eventbus.NewChanListener(chain.HeaderMsgQueue))
	go chain.HandleHeaderMsg()

	// only PShards receive inboundChunks
	if chain.Config.ShardId > 1000 {
		chain.InboundChunkMsgQueue = make(chan message.Message, 1000000)
		chain.InboundChunkMsgSubID = chain.broker.Subscribe(topics.OutboundChunkGossip, eventbus.NewChanListener(chain.InboundChunkMsgQueue))
		go chain.HandleInboundChunkMsg()
	}
}

// 处理块头缓冲区内消息
func (chain *Blockchain) HandleHeaderMsg() {
	for data := range chain.HeaderMsgQueue {
		header, _ := data.Payload().(types.Header)
		first := chain.ShardStates.UpdateHeader(header)
		logChain.Infof("[Node-%d-%d] 接收到 block-%d-%d 的块头",
			chain.Config.ShardId, chain.Config.NodeId, header.ShardId, header.Height)

		if first {
			// gossip to local shard
			headerMsg := message.NewBlockchainMessage(topics.HeaderGossip, header)
			headerMsgByte, _ := headerMsg.MarshalBinary()
			chain.Node.Gossip(headerMsgByte, chain.Config.ShardId)
			logChain.Infof("[Node-%d-%d] gossip block-%d-%d 的块头到分片内 (大小：%d)",
				chain.Config.ShardId, chain.Config.NodeId, header.ShardId, header.Height, len(headerMsgByte))

			if chain.Config.ShardId < 1001 {
				dstPShardIds := header.OutboundDstBitmap.GetElement()
				for _, PShardId := range dstPShardIds {
					chain.Node.Gossip(headerMsgByte, PShardId+1001)
					logChain.Infof("[Node-%d-%d] gossip block-%d-%d 的块头到 dst shard-%d (大小: %d)",
						chain.Config.ShardId, chain.Config.NodeId, header.ShardId, header.Height, PShardId, len(headerMsgByte))
				}
			}
		}
	}
}

// 处理入站块消息，目标分片接收入站块
func (chain *Blockchain) HandleInboundChunkMsg() {
	for data := range chain.InboundChunkMsgQueue {
		inboundChunk, _ := data.Payload().(types.OutboundChunk) // 将 data.Payload() 返回的值转换为 types.OutboundChunk 类型
		if len(inboundChunk.Txs) > 0 && inboundChunk.Txs[0].ToShard == chain.Config.ShardId {
			first := chain.ShardStates.UpdateInboundChunk(inboundChunk)
			logChain.Infof("[Node-%d-%d] 接收 block-%d 的入站块",
				chain.Config.ShardId, chain.Config.NodeId, inboundChunk.Txs[0].FromShard)
			for _, tx := range inboundChunk.Txs {
				// 打印交易信息
				logChain.Infof("已接收 txs,[Node-%d-%d] 从分片 %d 到分片 %d 的交易已接收, block.hash is %x, FromAddr: %s, ToAddr: %s, Value: %d",
					chain.Config.ShardId, chain.Config.NodeId, tx.FromShard, tx.ToShard, inboundChunk.BlockHash, tx.FromAddr.Hex(), tx.ToAddr.Hex(), tx.Value)
			}
			if first {
				inboundChunkMsg := message.NewBlockchainMessage(topics.OutboundChunkGossip, inboundChunk)
				inboundChunkMsgByte, _ := inboundChunkMsg.MarshalBinary()
				chain.Node.Gossip(inboundChunkMsgByte, chain.Config.ShardId)
				logChain.Infof("[Node-%d-%d] gossip 来自 block-%d 的入站块消息 (大小：%d)",
					chain.Config.ShardId, chain.Config.NodeId, inboundChunk.Txs[0].FromShard, len(inboundChunkMsgByte))
			}
		}
	}
}

func (chain *Blockchain) CheckBlock(block types.Block) error { // do not check signatures here
	chain.stateMutex.Lock()
	defer chain.stateMutex.Unlock()

	prevStateRoot, _ := chain.StateDB.Commit(false)
	defer chain.RollBack(prevStateRoot)

	if block.Height == 0 {
		accounts := chain.Config.Accounts
		for _, addr := range accounts {
			chain.StateDB.AddBalance(addr, 10000000)
		}
		stateRoot, _ := chain.StateDB.Commit(false)
		if stateRoot != block.StateRoot {
			return fmt.Errorf("[Node-%d-%d] block-%d-%d-%x state root is incorrect",
				chain.Config.ShardId, chain.Config.NodeId, block.ShardId, block.Height, block.Hash)
		}
		return nil
	}
	if chain.LatestBlock == nil {
		return errors.New("genesis block has not been committed")
	}
	if block.Height != chain.LatestBlock.Height+1 {
		return fmt.Errorf("[Node-%d-%d] previous block has not been committed (latest committed height: %d, this block height: %d)",
			chain.Config.ShardId, chain.Config.NodeId, chain.LatestBlock.Height, block.Height)
	}

	txs, inboundChunks := block.Transactions, block.InboundChunks

	// inbound txs
	for _, inboundChunk := range inboundChunks {
		if len(inboundChunk.Txs) == 0 {
			return errors.New("inboundChunk with 0 txs")
		}
		// check inboundChunk proof
		key, _ := rlp.EncodeToBytes(inboundChunk.Txs[0].ToShard)
		chunkRoot := inboundChunk.Root()
		db := memorydb.New()
		for j := 0; j < len(inboundChunk.ChunkProof); j++ {
			db.Put(crypto.Keccak256(inboundChunk.ChunkProof[j]), inboundChunk.ChunkProof[j])
		}
		// get the corresponding header
		fromShard := inboundChunk.Txs[0].FromShard
		var fromShardHeader *types.Header
		for {
			v, ok := chain.ShardStates.StateDB[fromShard].Headers[inboundChunk.BlockHash]
			if ok {
				fromShardHeader = v
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		hash, _ := trie.VerifyProof(fromShardHeader.TxRoot, key, db)
		if common.BytesToHash(hash) != chunkRoot {
			return fmt.Errorf("[Node-%d-%d] chunk proof from block-%d-%x is incorrect",
				chain.Config.ShardId, chain.Config.NodeId, inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
		}
		// update balance
		for _, tx := range inboundChunk.Txs {
			if tx.FromShard != chain.Config.ShardId && tx.ToShard == chain.Config.ShardId {
				chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
			}
		}
	}

	// intra-shard and outbound txs
	for _, tx := range txs {
		if tx.FromShard == chain.Config.ShardId && tx.ToShard == chain.Config.ShardId { // intra-shard
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
			chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
		}
		if tx.FromShard == chain.Config.ShardId && tx.ToShard != chain.Config.ShardId { // outbound
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
		}
	}

	// Check StateRoot
	stateRoot, _ := chain.StateDB.Commit(false)
	if stateRoot != block.StateRoot {
		return fmt.Errorf("block-%d-%d-%x stateRoot is incorrect", block.ShardId, block.Height, block.Hash)
	}

	// Check TxRoot
	outboundChunks := make([]types.OutboundChunk, chain.Config.PShardNum)
	for _, tx := range txs {
		outboundChunks[tx.ToShard-1001].Txs = append(outboundChunks[tx.ToShard-1001].Txs, tx)
	}
	txTrie := new(trie.Trie)
	txRoot := types.GetBlockTxRoot(outboundChunks, txTrie)
	if txRoot != block.TxRoot {
		return fmt.Errorf("block-%d-%d-%x txRoot is incorrect", block.ShardId, block.Height, block.Hash)
	}

	return nil
}

// 重新执行区块中的交易以更新状态。
func (chain *Blockchain) ExecuteBlock(block types.Block) {
	chain.stateMutex.Lock()
	defer chain.stateMutex.Unlock()

	if block.Height == 0 {
		accounts := chain.Config.Accounts
		for _, addr := range accounts {
			chain.StateDB.AddBalance(addr, 10000000)
		}
		stateRoot, _ := chain.StateDB.Commit(false)
		if stateRoot != block.StateRoot {
			logChain.Errorf("[Node-%d-%d] block-%d-%d-%x state root is incorrect",
				chain.Config.ShardId, chain.Config.NodeId, chain.Config.ShardId, block.Height, block.Hash)
		}
	}
	if chain.LatestBlock == nil {
		logChain.Errorf("[Node-%d-%d] genesis block has not been committed",
			chain.Config.ShardId, chain.Config.NodeId)
	}
	if block.Height != chain.LatestBlock.Height+1 {
		logChain.Errorf("[Node-%d-%d] previous block has not been committed (latest committed height: %d, this block height: %d)",
			chain.Config.ShardId, chain.Config.NodeId, chain.LatestBlock.Height, block.Height)
	}

	txs, inboundChunks := block.Transactions, block.InboundChunks

	// inbound txs
	for _, inboundChunk := range inboundChunks {
		if len(inboundChunk.Txs) == 0 {
			logChain.Errorf("[Node-%d-%d] inboundChunk with 0 txs", chain.Config.ShardId, chain.Config.NodeId)
		}
		// check inboundChunk proof
		key, _ := rlp.EncodeToBytes(inboundChunk.Txs[0].ToShard)
		chunkRoot := inboundChunk.Root()
		db := memorydb.New()
		for j := 0; j < len(inboundChunk.ChunkProof); j++ { // 交易块证明中的每个条目添加到数据库
			db.Put(crypto.Keccak256(inboundChunk.ChunkProof[j]), inboundChunk.ChunkProof[j])
		}
		// 获取相应的块头
		fromShard := inboundChunk.Txs[0].FromShard
		fromShardHeader := chain.ShardStates.StateDB[fromShard].Headers[inboundChunk.BlockHash]
		hash, _ := trie.VerifyProof(fromShardHeader.TxRoot, key, db)
		if common.BytesToHash(hash) != chunkRoot {
			logChain.Errorf("[Node-%d-%d] chunk proof from block-%d-%x is incorrect",
				chain.Config.ShardId, chain.Config.NodeId, inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
		}
		// 更新账户余额
		for _, tx := range inboundChunk.Txs {
			if tx.FromShard != chain.Config.ShardId && tx.ToShard == chain.Config.ShardId { // 如果交易是从其他分片发送到当前分片
				chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
			}
		}
	}

	// intra-shard and outbound txs
	for _, tx := range txs {
		if tx.FromShard == chain.Config.ShardId && tx.ToShard == chain.Config.ShardId { // 同分片交易
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
			chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
		}
		if tx.FromShard == chain.Config.ShardId && tx.ToShard != chain.Config.ShardId { // 出站交易
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
		}
	}

	// Check StateRoot
	stateRoot, _ := chain.StateDB.Commit(false)
	if stateRoot != block.StateRoot {
		logChain.Errorf("[Node-%d-%d] block-%d-%d-%x stateRoot is incorrect", chain.Config.ShardId, chain.Config.NodeId, block.ShardId, block.Height, block.Hash)
	}

	// Check TxRoot
	outboundChunks := make([]types.OutboundChunk, chain.Config.PShardNum)
	for _, tx := range txs {
		outboundChunks[tx.ToShard-1001].Txs = append(outboundChunks[tx.ToShard-1001].Txs, tx)
	}
	txTrie := new(trie.Trie)
	txRoot := types.GetBlockTxRoot(outboundChunks, txTrie)
	if txRoot != block.TxRoot {
		logChain.Errorf("[Node-%d-%d] block-%d-%d-%x txRoot is incorrect",
			chain.Config.ShardId, chain.Config.NodeId, block.ShardId, block.Height, block.Hash)
	}
}

// 回滚的数据
func (chain *Blockchain) RollBack(hash common.Hash) bool { // stateRoot
	stateDB, err := state.New(hash, chain.StateDB.Database())
	if err != nil {
		return false
	}
	chain.StateDB = stateDB
	return true
}
