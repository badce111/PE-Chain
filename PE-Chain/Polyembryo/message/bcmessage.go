package message

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"

	"github.com/KyrinCode/Mitosis/message/payload"
	"github.com/KyrinCode/Mitosis/topics"
	"github.com/KyrinCode/Mitosis/types"
	logger "github.com/sirupsen/logrus"
)

type BlockchainMessage struct {
	topic   topics.Topic
	payload payload.Safe

	marshaled *bytes.Buffer
}

func (bcm *BlockchainMessage) Topic() topics.Topic {
	return bcm.topic
}

func (bcm *BlockchainMessage) Payload() payload.Safe {
	return bcm.payload
}

func (bcm *BlockchainMessage) SetPayload(p payload.Safe) {
	bcm.payload = p
}

func (bcm *BlockchainMessage) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	if bcm.marshaled != nil {
		return bcm.marshaled.Bytes(), nil // .Bytes()：返回一个长度为 b.Len() 的片，其中包含缓冲区的未读部分。
	}

	if bcm.payload == nil {
		buf = bcm.Topic().ToBuffer()
		bcm.marshaled = &buf
		return buf.Bytes(), nil
	}

	_, err := buf.Write(bcm.payload.MarshalBinary())
	if err != nil {
		return []byte{}, nil
	}

	topics.Prepend(&buf, bcm.Topic())

	bcm.marshaled = &buf
	return buf.Bytes(), nil
}

func (bcm *BlockchainMessage) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)

	topic, err := topics.Extract(buf)

	bcm.topic = topic

	switch topic {
	case topics.BlockGossip:
		var b types.Block
		err = b.UnmarshalBinary(buf.Bytes())
		bcm.SetPayload(b)
	//case topics.TxGossip:
	//	var tx types.Transaction
	//	err = tx.UnmarshalBinary(buf.Bytes())
	//	bcm.SetPayload(tx)
	case topics.ConsensusLeader:
		var vote types.BFTMessage
		err = vote.UnmarshalBinary(buf.Bytes())
		bcm.SetPayload(vote)
	case topics.ConsensusValidator:
		var vote types.BFTMessage
		err = vote.UnmarshalBinary(buf.Bytes())
		bcm.SetPayload(vote)
	case topics.OutboundChunkGossip:
		var outboundChunk types.OutboundChunk
		err = outboundChunk.UnmarshalBinary(buf.Bytes())
		bcm.SetPayload(outboundChunk)
	case topics.HeaderGossip:
		var header types.Header
		err = header.UnmarshalBinary(buf.Bytes())
		bcm.SetPayload(header)
	}
	if err != nil {
		logger.Error("err data: ", data)
	}
	return err
}

func (bcm *BlockchainMessage) Equal(m Message) bool {
	msg, ok := m.(*BlockchainMessage)
	a, _ := bcm.MarshalBinary()
	b, _ := msg.MarshalBinary()
	return ok && bytes.Equal(a, b)
}

func (bcm *BlockchainMessage) ID() int64 {
	data, _ := bcm.MarshalBinary()
	ret := md5.Sum(data)

	byteOrder := binary.LittleEndian
	sig := byteOrder.Uint64(ret[0:8])

	return int64(sig)
}

func (bcm *BlockchainMessage) CachedBinary() bytes.Buffer {
	bcm.MarshalBinary()
	return *bcm.marshaled
}

func (bcm *BlockchainMessage) Header() []byte {
	return []byte{}
}

func NewBlockchainMessage(topic topics.Topic, p payload.Safe) Message {
	return &BlockchainMessage{
		topic:   topic,
		payload: p,
	}
}
