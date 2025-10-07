package main

import (
	"errors"
	"fmt"
	"github.com/KyrinCode/Mitosis/topics"

	"github.com/KyrinCode/Mitosis/types"
	"time"

	"github.com/herumi/bls-eth-go-binary/bls"
)

func (bft *BFTProtocol) Prepare() error {
	block := bft.blockchain.CreateNewBlock()

	logBFT.Infof("[Node-%d-%d] 以下入站快交易将包含在区块中：", bft.config.ShardId, bft.config.NodeId)
	for _, inboundChunk := range block.InboundChunks {
		for _, tx := range inboundChunk.Txs {
			logBFT.Infof("[Node-%d-%d] 来自 Shard-%d 交易哈希：%x，块哈希：%x",
				bft.config.ShardId, bft.config.NodeId, tx.FromShard, tx.Hash, block.Hash)
		}
	}
	logBFT.Infof("[Node-%d-%d] 新的 block-%d-%d 启动 BFT 共识。",
		bft.config.ShardId, bft.config.NodeId, block.ShardId, block.Height)
	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())
	msg := append(block.Hash.Bytes(), byte(types.MessageType_PREPARE))
	signedMsg := bft.bls.Sign(msg)
	height := bft.startphase.height
	prepareMsg := types.NewBFTMsg(types.MessageType_PREPARE, *block, signedMsg.Serialize(), bitmap, height)
	bft.currentBlock = block
	bft.prepareBitmap = &bitmap
	bft.aggregatePrepareSig = signedMsg

	bft.mu.Lock()
	bft.phase.Switch(block.Height, BFT_PREPARE)
	phase := bft.phase
	bft.mu.Unlock()
	go bft.TryGossip(phase, prepareMsg, topics.ConsensusValidator)
	logBFT.Infof("[Node-%d-%d] (领导者) 准备阶段新的 block-%d-%d",
		bft.config.ShardId, bft.config.NodeId, block.ShardId, block.Height)
	return nil
}

// 准备阶段
func (bft *BFTProtocol) OnPrepareVoteMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (领导者) 阶段：%+v，OnPrepareVoteMsg",
		bft.config.ShardId, bft.config.NodeId, bft.phase)
	if bft.currentBlock == nil {
		return errors.New("bft.currentBlock is empty")
	}
	if bft.phase.IsOut(Phase{bftMsg.BlockNum, BFT_PREPARE}) { // 阶段相同的话也要查看
		return nil
	}
	if bftMsg.BlockHash != bft.currentBlock.Hash {
		return errors.New("block in prepare-vote msg differs from the current block")
	}
	if bft.prepareBitmap.GetSize() >= bft.config.ConsensusThreshold { // enough votes
		return nil
	}

	msg := append(bftMsg.BlockHash.Bytes(), byte(types.MessageType_PREPARE))
	key := bftMsg.SenderPubkeyBitmap.GetElement()[0]
	signer := bft.key2NodeId(key)
	if !bft.bls.VerifySign(bftMsg.SenderSig, msg, signer, bft.config.ShardId) {
		return fmt.Errorf("wrong signature (signer: node-%d)", signer)
	}

	newSigners := bft.prepareBitmap.Merge(bftMsg.SenderPubkeyBitmap)
	if len(newSigners) == 0 { // prepareBitmap has included node
		return nil
	}

	var sign bls.Sign
	err := sign.Deserialize(bftMsg.SenderSig)
	if err != nil {
		return err
	}
	bft.aggregatePrepareSig.Add(&sign)

	if bft.prepareBitmap.GetSize() >= bft.config.ConsensusThreshold { // enough votes
		bft.Precommit()
	} else {
		logBFT.Infof("[Shard-%d] block-%d-%d 准备投票进度：%d/%d",
			bft.config.ShardId, bftMsg.Block.ShardId, bftMsg.BlockNum, bft.prepareBitmap.GetSize(), bft.config.ConsensusThreshold)
	}
	return nil
}

// 准备 -> 预提交，将自己的签名计入新位图
func (bft *BFTProtocol) Precommit() error {
	bft.mu.Lock()
	bft.phase.Switch(bft.currentBlock.Height, BFT_PRECOMMIT)
	phase := bft.phase
	bft.mu.Unlock()
	precommitMsg := types.NewBFTMsg(types.MessageType_PRECOMMIT, *bft.currentBlock, bft.aggregatePrepareSig.Serialize(), *bft.prepareBitmap, bft.startphase.height)
	go bft.TryGossip(phase, precommitMsg, topics.ConsensusValidator)
	logBFT.Infof("[Node-%d-%d] (领导者) 预提交阶段新的 block-%d-%d",
		bft.config.ShardId, bft.config.NodeId, bft.currentBlock.ShardId, bft.currentBlock.Height)
	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())
	bft.precommitBitmap = &bitmap
	msg := bft.currentBlock.Hash.Bytes()
	signedMsg := bft.bls.Sign(msg)
	bft.aggregatePrecommitSig = signedMsg
	return nil
}

func (bft *BFTProtocol) OnPrecommitVoteMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (领导者) 阶段：%+v，OnPrecommitVoteMsg",
		bft.config.ShardId, bft.config.NodeId, bft.phase)
	if bft.currentBlock == nil {
		return errors.New("bft.currentBlock is empty")
	}
	if bft.phase.IsOut(Phase{bftMsg.BlockNum, BFT_PRECOMMIT}) { // phase 相同的话也要查看
		return nil // 进来说明阶段或高度过时了
	}

	if bftMsg.BlockHash != bft.currentBlock.Hash {
		return errors.New("block in precommit-vote msg differs from the current block")
	}
	if bft.precommitBitmap.GetSize() >= bft.config.ConsensusThreshold { // enough votes
		return nil
	}

	msg := bftMsg.BlockHash.Bytes()
	key := bftMsg.SenderPubkeyBitmap.GetElement()[0]
	signer := bft.key2NodeId(key)
	if !bft.bls.VerifySign(bftMsg.SenderSig, msg, signer, bft.config.ShardId) {
		return fmt.Errorf("wrong signature (signer: node-%d)", signer)
	}

	newSigners := bft.precommitBitmap.Merge(bftMsg.SenderPubkeyBitmap)
	if len(newSigners) == 0 { // included before
		return nil
	}

	var sign bls.Sign
	err := sign.Deserialize(bftMsg.SenderSig)
	if err != nil {
		return err
	}
	bft.aggregatePrecommitSig.Add(&sign)

	if bft.precommitBitmap.GetSize() >= bft.config.ConsensusThreshold {
		bft.Commit()
	} else {
		logBFT.Infof("[Shard-%d] block-%d-%d 预提交投票进度：%d/%d",
			bft.config.ShardId, bftMsg.Block.ShardId, bftMsg.BlockNum, bft.precommitBitmap.GetSize(), bft.config.ConsensusThreshold)
	}
	return nil
}

// 将 precommit 过程的签名（对 header 的签名）更新到 block 中 commit，提交区块（广播）
func (bft *BFTProtocol) Commit() error {
	bft.mu.Lock()
	bft.phase.Switch(bft.currentBlock.Height, BFT_COMMIT)
	phase := bft.phase
	bft.mu.Unlock()
	commitMsg := types.NewBFTMsg(types.MessageType_COMMIT, *bft.currentBlock, bft.aggregatePrecommitSig.Serialize(), *bft.precommitBitmap, bft.startphase.height)
	go bft.TryGossip(phase, commitMsg, topics.ConsensusValidator)
	logBFT.Infof("[Node-%d-%d] (领导者) 提交阶段新的 block-%d-%d",
		bft.config.ShardId, bft.config.NodeId, bft.currentBlock.ShardId, bft.currentBlock.Height)
	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())
	bft.commitBitmap = &bitmap

	msg := append(bft.currentBlock.Hash.Bytes(), byte(types.MessageType_COMMIT))
	signedMsg := bft.bls.Sign(msg)
	bft.aggregateCommitSig = signedMsg

	bft.currentBlock.Signature = bft.aggregatePrecommitSig.Serialize()
	bft.currentBlock.SignBitMap = bft.precommitBitmap.Copy()
	bft.blockchain.Commit(bft.currentBlock)
	commitMsg.Block.CommitTime = time.Now()
	bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash
	return nil
}

func (bft *BFTProtocol) OnCommitVoteMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (领导者) 阶段：%+v，OnCommitVoteMsg",
		bft.config.ShardId, bft.config.NodeId, bft.phase)
	if bft.currentBlock == nil {
		return errors.New("bft.currentBlock is empty")
	}
	if bft.phase.IsOut(Phase{bftMsg.BlockNum, BFT_COMMIT}) { // phase 相同的话也要查看
		return nil
	}
	if bftMsg.BlockHash != bft.currentBlock.Hash {
		return errors.New("block in commit-vote msg differs from the current block")
	}
	if bft.commitBitmap.GetSize() >= bft.config.ConsensusThreshold { //enough votes
		return nil
	}

	msg := append(bft.currentBlock.Hash.Bytes(), byte(types.MessageType_COMMIT))
	key := bftMsg.SenderPubkeyBitmap.GetElement()[0]
	signer := bft.key2NodeId(key)
	if !bft.bls.VerifySign(bftMsg.SenderSig, msg, signer, bft.config.ShardId) {
		return fmt.Errorf("wrong signature (signer: %d)", signer)
	}

	newSigners := bft.commitBitmap.Merge(bftMsg.SenderPubkeyBitmap)
	if len(newSigners) == 0 { // included before
		return nil
	}

	var sign bls.Sign
	err := sign.Deserialize(bftMsg.SenderSig)
	if err != nil {
		return err
	}
	bft.aggregateCommitSig.Add(&sign)

	if bft.commitBitmap.GetSize() >= bft.config.ConsensusThreshold { // decide 就不把 commit 的签名下发了
		time.Sleep(4 * time.Second) // 出块间隔
		bft.Prepare()
	} else {
		logBFT.Infof("[Shard-%d] block-%d-%d 提交投票进度：%d/%d",
			bft.config.ShardId, bftMsg.Block.ShardId, bftMsg.BlockNum, bft.commitBitmap.GetSize(), bft.config.ConsensusThreshold)
	}
	return nil
}
