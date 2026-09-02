package gossip

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"time"

	"github.com/ohait/gossip2/enc"
	lib "github.com/ohait/gossip2/lib"
	"github.com/ohait/gossip2/sync"
)

func New(addr string) (lib.Client, error) {
	client := &tcpClient{
		addr: addr,
	}
	return client, client.connect()
}

type tcpClient struct {
	closed  chan struct{}
	addr    string
	conn    net.Conn
	buf     enc.Buffer
	subs    sync.Map[uint64, lib.Handler]
	pending sync.Map[uint64, func(msg)]
}

// Publish implements [gossip.Client].
func (c *tcpClient) Publish(topic string, id string, v enc.Version, message []byte) (enc.Version, error) {
	rid := rand.Uint64()
	ch := make(chan msg, 1)
	c.pending.Store(rid, func(x msg) {
		ch <- x
	})
	defer c.pending.Delete(rid)

	err := msg{
		cmd: publish, rid: rid,
		topic: topic, id: id,
		v: v, message: message,
	}.write(&c.buf)
	if err != nil {
		return 0, err
	}

	select {
	case msg := <-ch:
		if msg.cmd == ack {
			return msg.v, nil
		} else {
			return 0, fmt.Errorf("cas failed: %s", msg.message)
		}
	case <-c.closed:
		return 0, fmt.Errorf("closed")
	}
}

// Signal implements [gossip.Client].
func (c *tcpClient) Signal(topic string, id string, message []byte) error {
	return msg{
		cmd:   event,
		topic: topic, id: id,
		message: message,
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
	c.subs.Store(sid, handler)

	// send the subscribe request
	log.Printf("subscribing: topic=%q", topic)
	err = msg{
		rid:   sid,
		cmd:   subscribe,
		topic: topic,
	}.write(&c.buf)
	if err != nil {
		c.subs.Delete(sid)
		return nil, err
	}

	// wait for the replay ACK
	// NOTE: while waiting, we will receive old messages
	// and other data from the server
	select {
	case <-c.closed:
		// connection closed
		return nil, fmt.Errorf("closed")

	case m := <-res:
		// replay finished
		switch m.cmd {
		case ack:
			return func() {
				if c.subs.Delete(sid) {
					err := msg{
						rid: sid,
						cmd: unsubscribe,
					}.write(&c.buf)
					if err != nil {
						log.Printf("unsubscribe error: %v", err)
					}
				}
			}, nil

		case nack:
			// subscribe failed
			c.subs.Delete(sid)
			return nil, fmt.Errorf("subscribe failed: %s", string(m.message))

		default:
			// something wrong
			c.subs.Delete(sid)
			return nil, fmt.Errorf("unexpected response: %v", m)
		}
	}
}

func (c *tcpClient) connect() error {
	var err error
	conn, err := (&net.Dialer{
		KeepAlive: 10 * time.Second,
	}).Dial("tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn
	_, err = c.conn.Write([]byte("GSP2"))
	if err != nil {
		c.conn.Close()
		return err
	}
	c.buf = enc.Buffer{ReadWriter: c.conn}
	return c.setup()
}

func (c *tcpClient) Write(data []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(15 * time.Second))
	return c.conn.Write(data)
}

func (c *tcpClient) Read(data []byte) (int, error) {
	c.conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	return c.conn.Read(data)
}

func (c *tcpClient) setup() error {
	c.closed = make(chan struct{})
	var head [4]byte
	_, err := io.ReadFull(c, head[:])
	if err != nil {
		c.conn.Close()
		return err
	}
	if string(head[:]) != "GSP2" {
		c.conn.Close()
		return fmt.Errorf("invalid handshake")
	}
	go func() {
		defer close(c.closed)
		err := c.loop()
		if errors.Is(err, net.ErrClosed) {
			fmt.Printf("connection closed\n")
		} else {
			fmt.Printf("loop error: %v\n", err)
			c.conn.Close()
		}
	}()
	return nil
}

func (c *tcpClient) loop() error {
	var req msg
	for {
		err := req.read(&c.buf)
		if err != nil {
			return err
		}
		switch req.cmd {

		case event:
			cb, ok := c.subs.Load(req.rid)
			if !ok {
				log.Printf("ignore msg for unsubscribed topic %q", req.topic)
				continue
			}
			err := cb(req.topic, req.id, req.v, req.message)
			if err != nil {
				log.Printf("callback error: %v", err)
				c.subs.Delete(req.rid) // clean up
				err := msg{
					cmd: unsubscribe,
					rid: req.rid,
				}.write(&c.buf)
				if err != nil {
					return err // fatal failure
				}
			}

		case ack, nack:
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
