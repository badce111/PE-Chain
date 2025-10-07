package p2p

import (
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/message"
)

// BaseReader implements the common part between other Readers.
// Other readers are capable of processing all kinds of information
type BaseReader struct {
	publisher eventbus.Publisher
}

// NewBaseReader create object of BaseReader
func NewBaseReader(publisher eventbus.Publisher) *BaseReader {
	return &BaseReader{
		publisher: publisher,
	}
}

// ProcessMessage handle informations retrieved from other readers
func (r *BaseReader) ProcessMessage(addr string, data []byte) error {
	defer func() {
		if r := recover(); r != nil {
			logP2P.Errorf("data readloop failed %v,+++%d", r, data[0])
		}
	}()
	var msg message.BlockchainMessage
	if err := msg.UnmarshalBinary(data); err != nil {
		logP2P.Errorf("Unmarshal msg error, %v", err)
		return err
	}
	r.publisher.Publish(msg.Topic(), &msg)
	return nil
}
