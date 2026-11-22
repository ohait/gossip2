package gossip_test

import (
	"errors"
	"testing"
	"time"

	lib "github.com/ohait/gossip2/lib"
	gossip "github.com/ohait/gossip2/net"
)

func lockServer(t *testing.T, addr string) (*gossip.Server, lib.Client) {
	// a short quarantine keeps the cooldown tests fast; production uses DefaultGrace
	impl, err := lib.Config{Folder: "locktest-" + addr, LockGrace: 150 * time.Millisecond}.New()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	srv := &gossip.Server{Client: impl}
	go srv.Listen(addr)
	time.Sleep(500 * time.Millisecond)
	return srv, impl
}

func TestLockNet(t *testing.T) {
	addr := "localhost:12347"
	srv, impl := lockServer(t, addr)
	defer srv.Shutdown()
	defer impl.Close()

	a, err := gossip.New(addr)
	if err != nil {
		t.Fatalf("connect a: %v", err)
	}
	defer a.Close()
	b, err := gossip.New(addr)
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	defer b.Close()

	ttl := 2 * time.Second

	h, err := a.Lock("resource", ttl, 5*time.Second)
	if err != nil {
		t.Fatalf("a: %v", err)
	}
	if h.Key() != "resource" {
		t.Fatalf("wrong key: %q", h.Key())
	}

	// b is blocked: the lease is active, so the wait times out
	start := time.Now()
	if _, err := b.Lock("resource", ttl, 100*time.Millisecond); !errors.Is(err, lib.ErrLockTimeout) {
		t.Fatalf("b should have timed out, got %v", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("b gave up before its timeout")
	}

	// a keeps the lease alive
	if err := h.Extends(); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if h.Expired() {
		t.Fatal("handle reports expired right after an extend")
	}

	// releasing frees the key at once, no cooldown
	if err := h.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	h2, err := b.Lock("resource", ttl, time.Second)
	if err != nil {
		t.Fatalf("b should get the key immediately after the release: %v", err)
	}
	if err := h2.Release(); err != nil {
		t.Fatalf("release h2: %v", err)
	}

	// releasing twice is an error, not a panic
	if err := h2.Release(); err == nil {
		t.Fatal("double release must fail")
	}
}

func TestLockNetExpiry(t *testing.T) {
	addr := "localhost:12349"
	srv, impl := lockServer(t, addr)
	defer srv.Shutdown()
	defer impl.Close()

	a, err := gossip.New(addr)
	if err != nil {
		t.Fatalf("connect a: %v", err)
	}
	defer a.Close()
	b, err := gossip.New(addr)
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	defer b.Close()

	ttl := 300 * time.Millisecond
	h, err := a.Lock("k", ttl, time.Second)
	if err != nil {
		t.Fatalf("a: %v", err)
	}

	time.Sleep(ttl + 100*time.Millisecond)
	if !h.Expired() {
		t.Fatal("handle should report itself expired")
	}
	if err := h.Extends(); err == nil {
		t.Fatal("an expired lease must not be extendable")
	}
	if err := h.Release(); err == nil {
		t.Fatal("an expired lease must not be releasable")
	}

	// cooldown blocks b until deadline+grace (150ms on this test server):
	// at ttl+100ms the key is expired but still in quarantine
	if _, err := b.Lock("k", ttl, 20*time.Millisecond); !errors.Is(err, lib.ErrLockTimeout) {
		t.Fatalf("b should still be blocked by the cooldown, got %v", err)
	}
	h2, err := b.Lock("k", ttl, 5*time.Second)
	if err != nil {
		t.Fatalf("b should get the key after the cooldown: %v", err)
	}
	_ = h2.Release()
}

// Closing the client releases the leases it still holds.
func TestLockNetCloseReleases(t *testing.T) {
	addr := "localhost:12351"
	srv, impl := lockServer(t, addr)
	defer srv.Shutdown()
	defer impl.Close()

	a, err := gossip.New(addr)
	if err != nil {
		t.Fatalf("connect a: %v", err)
	}
	ttl := 10 * time.Second
	if _, err := a.Lock("k", ttl, time.Second); err != nil {
		t.Fatalf("a: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	b, err := gossip.New(addr)
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	defer b.Close()
	if _, err := b.Lock("k", ttl, time.Second); err != nil {
		t.Fatalf("the key should be free after a.Close(): %v", err)
	}
}

// A lease lives in the server, not in the connection: see lock_internal_test.go
