// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	crytporand "crypto/rand"
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/types"
	"math/rand"
	"sync"
)

type TxPool struct {
	PendingTxs []types.Transaction
	txMutex    sync.Mutex

	PendingInboundChunks []types.OutboundChunk
	inboundChunkMutex    sync.Mutex

	IsAdd  bool
	config *config.Config
}

func NewTxPool(config *config.Config) *TxPool {
	txPool := &TxPool{
		IsAdd:  false,
		config: config,
	}
	return txPool
}

func (pool *TxPool) NewTxPool(config *config.Config) *TxPool {
	txPool := &TxPool{
		IsAdd:  false,
		config: config,
	}
	return txPool
}

func (pool *TxPool) takeTxs(txLen int) ([]types.Transaction, []types.OutboundChunk) {
	pool.inboundChunkMutex.Lock()
	var txList []types.Transaction
	var inboundChunkList []types.OutboundChunk
	// cross-shard txs have higher priority
	for _, inboundChunk := range pool.PendingInboundChunks {
		if txLen > 0 {
			txLen -= len(inboundChunk.Txs)
			inboundChunkList = append(inboundChunkList, inboundChunk)
		} else {
			break
		}
	}
	pool.PendingInboundChunks = pool.PendingInboundChunks[len(inboundChunkList):]
	pool.inboundChunkMutex.Unlock()
	if txLen <= 0 {
		return txList, inboundChunkList
	}
	pool.txMutex.Lock()
	if txLen < len(pool.PendingTxs) {
		txList = pool.PendingTxs[:txLen]
		pool.PendingTxs = pool.PendingTxs[txLen:]
	} else {
		txList = pool.PendingTxs
		pool.PendingTxs = []types.Transaction{}
	}
	pool.txMutex.Unlock()
	if !pool.IsAdd {
		go pool.AddTxs()
		pool.IsAdd = true
	}
	return txList, inboundChunkList
}

func (pool *TxPool) takeTxs1(txLen int) ([]types.Transaction, []types.OutboundChunk) {
	pool.AddTxs1(txLen - pool.GetTxsLen())
	pool.inboundChunkMutex.Lock()
	var txList []types.Transaction
	var inboundChunkList []types.OutboundChunk
	// cross-shard txs have higher priority
	for _, inboundChunk := range pool.PendingInboundChunks {
		if txLen > 0 {
			txLen -= len(inboundChunk.Txs)
			inboundChunkList = append(inboundChunkList, inboundChunk)
		} else {
			break
		}
	}
	pool.PendingInboundChunks = pool.PendingInboundChunks[len(inboundChunkList):]
	pool.inboundChunkMutex.Unlock()
	if txLen <= 0 {
		return txList, inboundChunkList
	}
	pool.txMutex.Lock()
	if txLen < len(pool.PendingTxs) {
		txList = pool.PendingTxs[:txLen]
		pool.PendingTxs = pool.PendingTxs[txLen:]
	} else {
		txList = pool.PendingTxs
		pool.PendingTxs = []types.Transaction{}
	}
	pool.txMutex.Unlock()
	return txList, inboundChunkList
}

func (pool *TxPool) AddInboundChunk(inboundChunk *types.OutboundChunk) {
	newInboundChunk := inboundChunk.Copy().(types.OutboundChunk)
	pool.inboundChunkMutex.Lock()
	pool.PendingInboundChunks = append(pool.PendingInboundChunks, newInboundChunk)
	pool.inboundChunkMutex.Unlock()
}

func (pool *TxPool) AddTxs() {
	for {
		speed := pool.config.Speed / (pool.config.PShardNum / pool.config.RShardNum * pool.config.StartRS)
		for i := speed; i > 0; i-- {
			tx := pool.RandTx()
			if i <= speed*pool.config.CrossRatio/100 {
				toShards := pool.GetToShards(pool.config.ShardId)
				randomIndex := rand.Intn(len(toShards))
				toShard := toShards[randomIndex]
				tx.ToShard = toShard
			} else {
				tx.ToShard = tx.FromShard
			}
			pool.txMutex.Lock()
			pool.PendingTxs = append(pool.PendingTxs, *tx)
			pool.txMutex.Unlock()
		}
	}
}

func (pool *TxPool) AddTxs1(txLen int) {
	for i := 0; i < txLen; i++ {
		tx := pool.RandTx()
		if rand.Uint32()%100 < pool.config.CrossRatio {
			toShards := pool.GetToShards(pool.config.ShardId)
			randomIndex := rand.Intn(len(toShards))
			toShard := toShards[randomIndex]
			tx.ToShard = toShard
		} else {
			tx.ToShard = tx.FromShard
		}
		pool.txMutex.Lock()
		pool.PendingTxs = append(pool.PendingTxs, *tx)
		pool.txMutex.Unlock()
	}
}

func (pool *TxPool) RandTx() *types.Transaction {
	fromShard := pool.config.ShardId
	toShard := rand.Uint32()%pool.config.PShardNum + 1001
	fromIdx := rand.Uint32() % uint32(len(pool.config.Accounts))
	toIdx := rand.Uint32() % uint32(len(pool.config.Accounts))
	fromAddress := pool.config.Accounts[fromIdx]
	toAddress := pool.config.Accounts[toIdx]
	value := uint64(rand.Uint32() % 100)
	data := make([]byte, 4)
	crytporand.Read(data)
	return types.NewTransaction(fromShard, toShard, fromAddress, toAddress, value, data)
}

func (pool *TxPool) GetToShards(fromShard uint32) []uint32 {
	var toShards []uint32
	for i, shards := range pool.config.Topo.PShardIds {
		if i <= pool.config.StartRS {
			for _, shard := range shards {
				if shard != fromShard {
					toShards = append(toShards, shard)
				}
			}
		}
	}
	return toShards
}

func (pool *TxPool) GenerateCrossShardTxs4() *types.Transaction {
	fromShard := pool.config.ShardId
	toShards := pool.GetToShards(fromShard)
	randomIndex := rand.Intn(len(toShards))
	toShard := toShards[randomIndex]
	tx := pool.RandTx()
	tx.FromShard = fromShard
	tx.ToShard = toShard
	return tx
}

func (pool *TxPool) GenerateCrossShardTxs(a int) []types.Transaction {
	fromShard := pool.config.ShardId
	toShards := pool.GetToShards(fromShard)
	var txs []types.Transaction
	numShardsToProcess := 10
	if numShardsToProcess*2 > len(toShards) {
		numShardsToProcess = len(toShards) / 2
	}
	if a == 0 {
		for i, toShard := range toShards {
			if i >= numShardsToProcess {
				break
			}
			tx := pool.RandTx()
			tx.FromShard = fromShard
			tx.ToShard = toShard
			txs = append(txs, *tx)
		}
	} else if a == 1 {
		for _, toShard := range toShards[numShardsToProcess : numShardsToProcess*2] {
			tx := pool.RandTx()
			tx.FromShard = fromShard
			tx.ToShard = toShard
			txs = append(txs, *tx)
		}
	} else {
		for _, toShard := range toShards[numShardsToProcess*2:] {
			tx := pool.RandTx()
			tx.FromShard = fromShard
			tx.ToShard = toShard
			txs = append(txs, *tx)
		}
	}
	return txs
}

func (pool *TxPool) GetTxsLen() int {
	return len(pool.PendingTxs)
}
