package gossip

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ohait/gossip2/enc"
	lib "github.com/ohait/gossip2/lib"
)

func rawConn(addr string) (net.Conn, *enc.Buffer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	buf := enc.Buffer{ReadWriter: conn}
	var head [4]byte
	if err := buf.ReadFull(head[:]); err != nil { // the server greets first
		conn.Close()
		return nil, nil, err
	}
	if string(head[:]) != "GSP2" {
		conn.Close()
		return nil, nil, fmt.Errorf("unexpected handshake: %q", string(head[:]))
	}
	if err := buf.WriteFull([]byte("GSP2")); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, &buf, nil
}

func testServer(name, addr string) (*Server, lib.Client) {
	impl, err := lib.Config{Folder: name, LockGrace: 250 * time.Millisecond}.New()
	if err != nil {
		panic(err)
	}
	srv := Server{Client: impl}
	go srv.Listen(addr)
	time.Sleep(500 * time.Millisecond)
	return &srv, impl
}

// The lock payload is size-capped before it is decoded.
func TestLockPayloadGuard(t *testing.T) {
	srv, impl := testServer("lockguard", "localhost:12355")
	defer srv.Shutdown()
	defer impl.Close()

	conn, buf, err := rawConn("localhost:12355")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	req := msg{cmd: lock, rid: 1, id: "k", message: []byte(`{"ttl":` + strings.Repeat("1", 2000) + `}`)}
	if err := req.write(buf); err != nil {
		t.Fatalf("send: %v", err)
	}
	var res msg
	if err := res.read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if res.cmd != nack {
		t.Fatalf("expected a nack for an oversized payload, got %v", res.String())
	}
}

// Unknown JSON keys are ignored: a newer client still works against this server.
func TestLockUnknownKeyTolerated(t *testing.T) {
	srv, impl := testServer("lockcompat", "localhost:12357")
	defer srv.Shutdown()
	defer impl.Close()

	conn, buf, err := rawConn("localhost:12357")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	req := msg{
		cmd:     lock,
		rid:     2,
		id:      "k",
		message: []byte(`{"ttl":1000000000,"timeout":1000000000,"future":"whatever"}`),
	}
	if err := req.write(buf); err != nil {
		t.Fatalf("send: %v", err)
	}
	var res msg
	if err := res.read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if res.cmd != ack || res.v == 0 {
		t.Fatalf("expected an ack with a lease version, got %v", res.String())
	}
}

// The lease lives in the server, not in the connection: dropping the socket
// does not release it, and does not let the holder extend it either.
func TestLockSurvivesConnectionLoss(t *testing.T) {
	srv, impl := testServer("lockconn", "localhost:12359")
	defer srv.Shutdown()
	defer impl.Close()

	a, err := New("localhost:12359")
	if err != nil {
		t.Fatalf("connect a: %v", err)
	}
	defer a.Close()
	ta := a.(*tcpClient)

	ttl := 5 * time.Second
	if _, err := a.Lock("k", ttl, time.Second); err != nil {
		t.Fatalf("a: %v", err)
	}

	// kill the socket without going through Close(), so nothing releases the lease
	if err := ta.conn.Close(); err != nil {
		t.Fatalf("drop connection: %v", err)
	}

	b, err := New("localhost:12359")
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	defer b.Close()

	if _, err := b.Lock("k", ttl, 100*time.Millisecond); !errors.Is(err, lib.ErrLockTimeout) {
		t.Fatalf("the lease should outlive the connection, got %v", err)
	}
}

// A nack carrying a wake-at version is a retry hint, not a hard failure.
func TestLockContentionHint(t *testing.T) {
	srv, impl := testServer("lockhint", "localhost:12361")
	defer srv.Shutdown()
	defer impl.Close()

	conn, buf, err := rawConn("localhost:12361")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	ttl := int64(2 * time.Second)
	grace := 250 * time.Millisecond
	req := msg{cmd: lock, rid: 3, id: "k", message: []byte(`{"ttl":` + itoa(ttl) + `,"timeout":` + itoa(ttl) + `}`)}
	if err := req.write(buf); err != nil {
		t.Fatalf("send: %v", err)
	}
	var res msg
	if err := res.read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if res.cmd != ack {
		t.Fatalf("first acquire should succeed: %v", res.String())
	}
	acquired := time.Now()

	req = msg{cmd: lock, rid: 4, id: "k", message: []byte(`{"ttl":` + itoa(ttl) + `,"timeout":` + itoa(ttl) + `}`)}
	if err := req.write(buf); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := res.read(buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if res.cmd != nack {
		t.Fatalf("second acquire should be refused: %v", res.String())
	}
	wake := time.Unix(0, int64(res.v))
	if wake.Before(acquired.Add(time.Duration(ttl))) {
		t.Fatalf("wake-at %v is before the lease deadline %v", wake, acquired.Add(time.Duration(ttl)))
	}
	if wake.After(acquired.Add(time.Duration(ttl)).Add(grace)) {
		t.Fatalf("wake-at %v is beyond deadline+grace %v", wake, acquired.Add(time.Duration(ttl)).Add(grace))
	}
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
