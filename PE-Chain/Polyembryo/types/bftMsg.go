package types

import (
	"github.com/KyrinCode/Mitosis/message/payload"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"time"
)

type MessageType byte

const (
	MessageType_PREPARE MessageType = iota
	MessageType_PREPAREVOTE
	MessageType_PRECOMMIT
	MessageType_PRECOMMITVOTE
	MessageType_COMMIT
	MessageType_COMMITVOTE
)

type BFTMessage struct {
	MessageType MessageType

	BlockNum    uint32
	StartHeight uint32
	BlockHash   common.Hash

	Block              Block
	SenderSig          []byte
	SenderPubkeyBitmap Bitmap

	Time time.Time
}

func (msg BFTMessage) MarshalBinary() []byte {
	msgBytes, err := rlp.EncodeToBytes(msg)
	if err != nil {
		log.Error("BFT Encode To Bytes error", err)
	}
	return msgBytes
}

func (msg *BFTMessage) UnmarshalBinary(data []byte) error {
	return rlp.DecodeBytes(data, msg)
}

func (msg BFTMessage) Copy() payload.Safe {
	sign := make([]byte, len(msg.SenderSig))
	copy(sign[:], msg.SenderSig[:])
	bitmap := make([]byte, len(msg.SenderPubkeyBitmap))
	copy(bitmap[:], msg.SenderPubkeyBitmap[:])
	var blockHash [32]byte
	copy(blockHash[:], msg.BlockHash[:])

	newB := msg.Block.Copy().(Block)
	return BFTMessage{
		msg.MessageType,
		msg.BlockNum,
		msg.StartHeight,
		blockHash,
		newB,
		sign,
		bitmap,
		msg.Time,
	}

}

func NewBFTMsg(messageType MessageType, block Block, sign []byte, bitmap Bitmap, start uint32) *BFTMessage {
	return &BFTMessage{
		messageType,
		block.Height,
		start,
		block.Hash,
		block,
		sign,
		bitmap,
		time.Now(),
	}
}
