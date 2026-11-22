package gossip

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/ohait/gossip2/enc"
	lib "github.com/ohait/gossip2/lib"
)

const (
	event       = 'E' // used for signal and for broadcasts from server
	subscribe   = 'S' // subscribe to a topic
	unsubscribe = 'U' // unsubscribe from a topic
	publish     = 'P' // publish with CAS
	lock        = 'L' // acquire a lease
	lockExtend  = 'X' // extend a lease, v = lease version
	lockRelease = 'R' // release a lease, v = lease version
	ack         = '1' // reply OK to a request
	nack        = '0' // reply error to a request
)

// lockReq is the JSON payload of L and X. Unknown keys are ignored, so a newer
// client talking to an older server degrades gracefully. The command byte is
// the schema selector: a breaking change gets a new command byte.
type lockReq struct {
	TTL     int64 `json:"ttl"`     // nanoseconds
	Timeout int64 `json:"timeout"` // nanoseconds, only used by L
}

func decodeLockReq(data []byte) (lockReq, error) {
	var r lockReq
	if len(data) > lib.MaxLockMsg {
		return r, fmt.Errorf("lock payload too large: %d bytes (max %d)", len(data), lib.MaxLockMsg)
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return r, fmt.Errorf("invalid lock payload: %w", err)
	}
	return r, nil
}

var DBG = func(format string, args ...any) {
	// log.Printf("DBG "+format, args...)
}

type msg struct {
	cmd     byte
	rid     uint64
	topic   string
	id      string
	v       lib.Version
	message []byte
}

var casFailedPrefix = []byte("CAS1")

func encodeCASFailed(err lib.CASFailed) []byte {
	data := make([]byte, len(casFailedPrefix)+16)
	copy(data, casFailedPrefix)
	binary.BigEndian.PutUint64(data[len(casFailedPrefix):], uint64(err.Given))
	binary.BigEndian.PutUint64(data[len(casFailedPrefix)+8:], uint64(err.Expected))
	return data
}

func decodeCASFailed(data []byte) (lib.CASFailed, bool) {
	if len(data) != len(casFailedPrefix)+16 || string(data[:len(casFailedPrefix)]) != string(casFailedPrefix) {
		return lib.CASFailed{}, false
	}
	return lib.CASFailed{
		Given:    lib.Version(binary.BigEndian.Uint64(data[len(casFailedPrefix):])),
		Expected: lib.Version(binary.BigEndian.Uint64(data[len(casFailedPrefix)+8:])),
	}, true
}

func (m msg) String() string {
	switch m.cmd {
	case event:
		return fmt.Sprintf("event (rid: %d) %s/%s", m.rid, m.topic, m.id)
	case subscribe:
		return fmt.Sprintf("subscribe (rid: %d)", m.rid)
	case unsubscribe:
		return fmt.Sprintf("unsubscribe (rid: %d)", m.rid)
	case publish:
		return fmt.Sprintf("publish (rid: %d)", m.rid)
	case lock:
		return fmt.Sprintf("lock (rid: %d) %s", m.rid, m.id)
	case lockExtend:
		return fmt.Sprintf("lock extend (rid: %d) %s v=%d", m.rid, m.id, m.v)
	case lockRelease:
		return fmt.Sprintf("lock release (rid: %d) %s v=%d", m.rid, m.id, m.v)
	case ack:
		return fmt.Sprintf("ack (rid: %d)", m.rid)
	case nack:
		return fmt.Sprintf("nack (rid: %d) %s", m.rid, m.message)
	default:
		return fmt.Sprintf("%c (rid: %d)", m.cmd, m.rid)
	}
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
	m.v = Version(v)
	_, m.message, err = b.ReadData()
	if err != nil {
		return err
	}
	return nil
}
