package gossip

import (
	"github.com/ohait/gossip2/enc"
)

const (
	event       = 'E' // used for signal and for broadcasts from server
	subscribe   = 'S' // subscribe to a topic
	unsubscribe = 'U' // unsubscribe from a topic
	publish     = 'P' // publish with CAS
	ack         = '1' // reply OK to a request
	nack        = '0' // reply error to a request
)

var DBG = func(format string, args ...any) {
	// log.Printf("DBG "+format, args...)
}

type msg struct {
	cmd     byte
	rid     uint64
	topic   string
	id      string
	v       enc.Version
	message []byte
}

func (m msg) write(b *enc.Buffer) error {
	b.Lock()
	defer b.Unlock()
	DBG("SEND %c topic=%q id=%q v=%d %dB",
		m.cmd, m.topic, m.id, m.v, len(m.message))
	err := b.WriteByte(m.cmd)
	if err != nil {
		return err
	}
	_, err = b.WriteUint64(m.rid)
	if err != nil {
		return err
	}
	_, err = b.WriteID(m.topic)
	if err != nil {
		return err
	}
	_, err = b.WriteID(m.id)
	if err != nil {
		return err
	}
	_, err = b.WriteUint64(uint64(m.v))
	if err != nil {
		return err
	}
	_, err = b.WriteData(m.message)
	if err != nil {
		return err
	}
	return nil
}

func (m *msg) read(b *enc.Buffer) error {
	var err error
	m.cmd, err = b.ReadByte()
	if err != nil {
		return err
	}
	_, m.rid, err = b.ReadUint64()
	if err != nil {
		return err
	}
	_, m.topic, err = b.ReadID()
	if err != nil {
		return err
	}
	_, m.id, err = b.ReadID()
	if err != nil {
		return err
	}
	var v uint64
	_, v, err = b.ReadUint64()
	if err != nil {
		return err
	}
	m.v = enc.Version(v)
	_, m.message, err = b.ReadData()
	if err != nil {
		return err
	}
	return nil
}
