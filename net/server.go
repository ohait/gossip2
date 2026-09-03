package gossip

import (
	"fmt"
	"io"
	"net"

	"github.com/ohait/gossip2/enc"
	lib "github.com/ohait/gossip2/lib"
)

type Server struct {
	Client    lib.Client
	listeners []net.Listener
	closing   chan struct{}
}

func (s *Server) Shutdown() {
	enc.LOG("shutting down...")
	for _, l := range s.listeners {
		l.Close() // stop accepting new connections
	}
	close(s.closing)
}

// Listen starts the server and listens for incoming connections
// blocks until the server is stopped
func (s *Server) Listen(addr string) error {
	if s.closing == nil {
		s.closing = make(chan struct{})
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				enc.LOG("accept error: %v", err)
			}
			go func() {
				err := s.handle(conn)
				if err != nil {
					enc.LOG("client fatal error: %v", err)
				}
			}()
		}
	}()
	return nil
}

type conn struct {
	queue chan msg
	gone  chan struct{}
	conn  net.Conn
	buf   enc.Buffer
	subs  map[uint64]func()
	cli   rawClient
}

func (s *Server) handle(socket net.Conn) error {
	conn := &conn{
		queue: make(chan msg, 100),
		gone:  make(chan struct{}),
		conn:  socket,
		buf:   enc.Buffer{ReadWriter: socket},
		subs:  map[uint64]func(){},
		cli:   s.Client.(rawClient),
	}
	defer socket.Close()
	defer close(conn.gone)

	defer func() {
		for _, unsub := range conn.subs {
			unsub()
		}
	}()

	// GSP2 handshake: the client sends "GSP2" first, the server echoes it back.
	var head [4]byte
	if _, err := io.ReadFull(&conn.buf, head[:]); err != nil {
		return err
	}
	if string(head[:]) != "GSP2" {
		return fmt.Errorf("invalid handshake: %q", string(head[:]))
	}
	if _, err := conn.buf.Write([]byte("GSP2")); err != nil {
		return err
	}

	go func() {
		defer conn.conn.Close()
		for {
			select {
			case <-s.closing:
				enc.LOG("closing...")
				// TODO we should consider sending a BYE message and be graceful
				return
			case <-conn.gone:
				return
			case msg := <-conn.queue:
				err := msg.write(&conn.buf)
				if err != nil {
					enc.LOG("write error: %v", err)
					return
				}
			}
		}
	}()

	for {
		var req msg
		err := req.read(&conn.buf)
		if err != nil {
			return err
		}
		res := msg{cmd: ack, rid: req.rid}
		err = conn.handleOne(&req, &res)

		if res.rid != 0 {
			// with a RID, errors are sent back to the client
			if err != nil {
				DBG("ERROR sent to client: %v", err)
				res.cmd = nack
				res.message = []byte(err.Error())
			}
			select {
			case conn.queue <- res:
			default:
				return io.ErrShortBuffer
			}
		} else if err != nil {
			// otherwise, errors are logged and connection is closed
			return err
		}
	}
}

func (conn *conn) handleOne(req, res *msg) (err error) {
	switch req.cmd {

	case event:
		return conn.cli.SignalRaw(req.topic, req.id, req.message)

	case publish:
		res.v, err = conn.cli.PublishRaw(req.topic, req.id, req.v, req.message)
		return err

	case subscribe:
		if _, ok := conn.subs[req.rid]; ok {
			return fmt.Errorf("already subscribed with id: %q", req.rid)
		}
		var unsub func()
		unsub, err = conn.cli.SubscribeRaw(req.topic,
			func(topic, id string, v Version, message []byte) error {
				select {
				case <-conn.gone:
					return io.EOF
				case conn.queue <- msg{
					rid: req.rid, // we broadcast to the same RID as the subscribe
					cmd: event, topic: topic,
					id: id, v: v,
					message: message,
				}:
				default:
					return fmt.Errorf("channel full")
				}
				return nil
			})
		if err != nil {
			return err
		}
		conn.subs[req.rid] = unsub
		return nil

	case unsubscribe:
		if unsub, ok := conn.subs[req.rid]; ok {
			unsub()
			delete(conn.subs, req.rid)
		}
		return nil

	default:
		return fmt.Errorf("unknown command: %v", req.cmd)
	}
}

type rawClient interface {
	lib.Client
	PublishRaw(topic string, id string, v Version, data []byte) (Version, error)
	SignalRaw(topic string, id string, data []byte) error
	SubscribeRaw(topic string, h lib.Handler) (func(), error)
}
