package gossip

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"time"

	"github.com/ohait/gossip2/enc"
	lib "github.com/ohait/gossip2/lib"
	"github.com/ohait/gossip2/sync"
)

type Client = lib.Client

type Version = lib.Version

func New(addr string) (Client, error) {
	client := &tcpClient{
		closing: make(chan struct{}),
		addr:    addr,
		holds:   map[string]*lib.LockHandle{},
	}
	// this ch will get an error when connecting the first time
	// providing feedback for invalid setup but allow for reconnection
	ch := make(chan error)
	go client.stayConnected(ch)
	err := <-ch
	return client, err
}

type tcpClient struct {
	closing chan struct{}
	addr    string
	conn    net.Conn
	buf     enc.Buffer
	subs    sync.Map[uint64, *sub]
	pending sync.Map[uint64, func(msg)]
	mu      sync.Mutex
	holds   map[string]*lib.LockHandle // newest handle per key, released on Close
}

type sub struct {
	topic string
	h     lib.Handler
	mu    sync.Mutex
	last  map[string]Version
}

// accept verify if the message is not out of order
func (s *sub) accept(id string, v Version) bool {
	if v == 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		s.last = make(map[string]Version)
	}
	prev, ok := s.last[id]
	if ok && prev >= v {
		return false
	}
	s.last[id] = v
	return true
}

var _ Client = (*tcpClient)(nil)

// Publish implements [gossip.Client].
func (c *tcpClient) Publish(topic string, id string, v lib.Version, message []byte) (Version, error) {
	raw, err := enc.Compress(message)
	if err != nil {
		return 0, fmt.Errorf("failed to compress data: %w", err)
	}
	rid := rand.Uint64()
	ch := make(chan msg, 1)
	c.pending.Store(rid, func(x msg) {
		ch <- x
	})
	defer c.pending.Delete(rid)

	err = msg{
		cmd: publish, rid: rid,
		topic: topic, id: id,
		v: v, message: raw,
	}.write(&c.buf)
	if err != nil {
		return 0, fmt.Errorf("failed to send publish request: %w", err)
	}

	msg := <-ch
	if msg.cmd == ack {
		return msg.v, nil
	} else {
		if casErr, ok := decodeCASFailed(msg.message); ok {
			return 0, casErr
		}
		return 0, fmt.Errorf("cas failed: %s", msg.message)
	}
}

// request sends a request and waits for the ack/nack. A nack with v != 0 is a
// "try again at v" hint (lock contention), not a hard failure.
func (c *tcpClient) request(cmd byte, id string, v Version, payload []byte) (msg, error) {
	rid := rand.Uint64()
	ch := make(chan msg, 1)
	c.pending.Store(rid, func(x msg) {
		ch <- x
	})
	defer c.pending.Delete(rid)

	if err := (msg{cmd: cmd, rid: rid, id: id, v: v, message: payload}).write(&c.buf); err != nil {
		return msg{}, fmt.Errorf("failed to send %c request: %w", cmd, err)
	}
	select {
	case m := <-ch:
		return m, nil
	case <-c.closing:
		return msg{}, errors.New("client closed")
	}
}

// Lock implements [gossip.Client]. It blocks until the key is free, timeout
// elapses, or the client is closed.
func (c *tcpClient) Lock(key string, ttl, timeout time.Duration) (*lib.LockHandle, error) {
	if err := lib.ValidateLockKey(key); err != nil {
		return nil, err
	}
	if err := lib.ValidateTTL(ttl); err != nil {
		return nil, err
	}
	if err := lib.ValidateTimeout(timeout); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(lockReq{TTL: int64(ttl), Timeout: int64(timeout)})
	if err != nil {
		return nil, fmt.Errorf("failed to encode lock request: %w", err)
	}

	end := time.Now().Add(timeout)
	for {
		m, err := c.request(lock, key, 0, payload)
		if err != nil {
			return nil, err
		}
		switch m.cmd {
		case ack:
			h := lib.NewLockHandle(key, m.v, time.Now().Add(ttl), ttl,
				func(v Version, ttl time.Duration) (Version, time.Time, error) {
					return c.extendLock(key, v, ttl)
				},
				func(v Version) error {
					return c.releaseLock(key, v)
				})
			c.mu.Lock()
			c.holds[key] = h
			c.mu.Unlock()
			return h, nil

		case nack:
			if m.v == 0 {
				// hard failure: bad arguments, connection closed, server error
				return nil, fmt.Errorf("lock %q: %s", key, m.message)
			}
			// the key is held; m.v is the instant it becomes free
			if !time.Now().Before(end) {
				return nil, lib.ErrLockTimeout
			}
			wait := min(time.Unix(0, int64(m.v)).Sub(time.Now()), end.Sub(time.Now()))
			if wait < lib.MinLockSleep {
				wait = lib.MinLockSleep
			}
			select {
			case <-time.After(wait):
			case <-c.closing:
				return nil, errors.New("client closed")
			}

		default:
			return nil, fmt.Errorf("unexpected lock response: %v", m.cmd)
		}
	}
}

func (c *tcpClient) extendLock(key string, v Version, ttl time.Duration) (Version, time.Time, error) {
	payload, err := json.Marshal(lockReq{TTL: int64(ttl)})
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("failed to encode lock request: %w", err)
	}
	m, err := c.request(lockExtend, key, v, payload)
	if err != nil {
		return 0, time.Time{}, err
	}
	if m.cmd != ack {
		return 0, time.Time{}, fmt.Errorf("extend lock %q: %s", key, m.message)
	}
	// the server's deadline is max(old, now+ttl); the client only needs this
	// locally for backoff and logging, so the approximation is fine.
	return m.v, time.Now().Add(ttl), nil
}

func (c *tcpClient) releaseLock(key string, v Version) error {
	m, err := c.request(lockRelease, key, v, nil)
	if err != nil {
		return err
	}
	if m.cmd != ack {
		return fmt.Errorf("release lock %q: %s", key, m.message)
	}
	return nil
}

// Signal implements [gossip.Client].
func (c *tcpClient) Signal(topic string, id string, message []byte) error {
	raw, err := enc.Compress(message)
	if err != nil {
		return fmt.Errorf("failed to compress signal: %w", err)
	}
	return msg{
		cmd:   event,
		topic: topic, id: id,
		message: raw,
	}.write(&c.buf)
}

// Subscribe registers a handler for the given topic and returns an unsub function.
// NOTE: message might be out of order
func (c *tcpClient) Subscribe(topic string, handler lib.Handler) (unsub func(), err error) {
	sid := rand.Uint64()

	// mechanism that waits for the ACK (end of replay)
	res := make(chan msg, 1)
	c.pending.Store(sid, func(m msg) {
		res <- m
	})
	defer c.pending.Delete(sid)

	// store the handler
	sub := &sub{
		topic: topic,
	}
	sub.h = func(topic string, id string, v Version, data []byte) error {
		if !sub.accept(id, v) {
			// enc.LOG("drop out of order: topic=%q id=%s v=%d", topic, id, v)
			return nil
		}
		data, err := enc.Decompress(data)
		if err != nil {
			return fmt.Errorf("failed to decompress data: %w", err)
		}
		return handler(topic, id, v, data)
	}
	c.subs.Store(sid, sub)

	// send the subscribe request
	enc.LOG("subscribing: topic=%q with rid=%d", topic, sid)
	err = msg{
		rid:   sid,
		cmd:   subscribe,
		topic: topic,
	}.write(&c.buf)
	if err != nil {
		enc.LOG("subscribe error: %v", err)
		c.subs.Delete(sid)
		return nil, err
	}

	// wait for the replay ACK
	// NOTE: while waiting, we will receive old messages
	// and other data from the server
	m := <-res

	switch m.cmd {
	case ack:
		// replay finished
		return func() {
			enc.LOG("unsubscribing: rid=%d", sid)
			if c.subs.Delete(sid) {
				err := msg{
					rid: sid,
					cmd: unsubscribe,
				}.write(&c.buf)
				if err != nil {
					enc.LOG("unsubscribe error: %v", err)
				}
			}
		}, nil

	case nack:
		// subscribe failed
		enc.LOG("subscribe failed: %s", string(m.message))
		c.subs.Delete(sid)
		return nil, fmt.Errorf("subscribe failed: %s", string(m.message))

	default:
		// something wrong
		c.subs.Delete(sid)
		return nil, fmt.Errorf("unexpected response: %v", m)
	}
}

func (c *tcpClient) stayConnected(ch chan error) {
	timer := time.NewTimer(0)
	for {
		timer.Reset(5 * time.Second)
		err := c.connect(ch)
		select {
		case ch <- err:
		default:
		}
		enc.LOG("connection error: %v", err)
		select {
		case <-c.closing:
			return
		case <-timer.C:
			enc.LOG("reconnecting...")
		}
	}
}

func (c *tcpClient) connect(ch chan error) error {
	conn, err := (&net.Dialer{
		KeepAlive: 10 * time.Second,
	}).Dial("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.addr, err)
	}
	c.conn = conn
	_, err = c.conn.Write([]byte("GSP2"))
	if err != nil {
		enc.LOG("connect error: %v", err)
		c.conn.Close()
		return fmt.Errorf("failed to send handshake: %w", err)
	}
	c.buf = enc.Buffer{ReadWriter: c.conn}

	var head [4]byte
	_, err = io.ReadFull(c.conn, head[:])
	if err != nil {
		c.conn.Close()
		return fmt.Errorf("reading handshake: %w", err)
	}
	if string(head[:]) != "GSP2" {
		c.conn.Close()
		return fmt.Errorf("invalid handshake")
	}

	// resubscribe to all topics
	// this will trigger a replay
	c.subs.Range(func(sid uint64, h *sub) bool {
		enc.LOG("re-subscribing: topic=%q with rid=%d", h.topic, sid)
		err = msg{
			rid:   sid,
			cmd:   subscribe,
			topic: h.topic,
		}.write(&c.buf)
		return err == nil
	})
	if err != nil {
		return fmt.Errorf("failed to resubscribe: %w", err)
	}

	select {
	case ch <- nil: // connected ok
	default:
	}

	return c.loop()
}

// Close closes the connection to the server, releasing any lease still held.
func (c *tcpClient) Close() error {
	select {
	case <-c.closing:
		return nil
	default:
	}

	// release the leases while the connection is still usable
	c.mu.Lock()
	holds := c.holds
	c.holds = map[string]*lib.LockHandle{}
	c.mu.Unlock()
	for _, h := range holds {
		if err := h.Release(); err != nil {
			enc.LOG("release on close: %v", err)
			break
		}
	}

	close(c.closing) // stop reconnecting, including while disconnected
	enc.LOG("closing connection")
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *tcpClient) loop() error {
	defer func() {
		// make current pending requests fail
		c.pending.Range(func(_ uint64, f func(msg)) bool {
			f(msg{
				cmd:     nack,
				message: []byte("connection closed"),
			})
			return true
		})
	}()

	enc.LOG("waiting for messages...")
	var req msg
	for {
		err := req.read(&c.buf)
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}
		switch req.cmd {

		case event:
			sub, ok := c.subs.Load(req.rid)
			if !ok {
				enc.LOG("ignore msg for unsubscribed rid %d", req.rid)
				continue
			}
			err := sub.h(req.topic, req.id, req.v, req.message)
			if err != nil {
				enc.LOG("callback error: %v", err)
				c.subs.Delete(req.rid) // clean up
				err := msg{
					cmd: unsubscribe,
					rid: req.rid,
				}.write(&c.buf)
				if err != nil {
					return fmt.Errorf("failed to send unsubscribe: %w", err)
				}
			}

		case nack:
			enc.LOG("nack: rid=%d", req.rid)
			fallthrough
		case ack:
			cb, _ := c.pending.Load(req.rid)
			if cb != nil {
				cb(req)
				c.pending.Delete(req.rid)
			}

		default:
			return fmt.Errorf("unknown command: %v", req.cmd)
		}
	}
}
