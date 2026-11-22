package gossip

import (
	"errors"
	"testing"
	"time"
)

func newTestIdx() *idx {
	return &idx{
		closed:    make(chan struct{}),
		locks:     map[string]*lockEntry{},
		holds:     map[string]*LockHandle{},
		lockGrace: DefaultGrace,
		now:       time.Now,
	}
}

// The state machine driven by an injected clock: no sleeps, exact instants.
func TestLockStateMachine(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	now := t0
	i := newTestIdx()
	i.now = func() time.Time { return now }

	ttl := 10 * time.Second
	grace := DefaultGrace // the quarantine does not scale with the ttl

	v, deadline, ok := i.TryLock("k", ttl)
	if !ok || v == 0 {
		t.Fatalf("first acquire failed: v=%d ok=%v", v, ok)
	}
	if !deadline.Equal(t0.Add(ttl)) {
		t.Fatalf("deadline = %v, want %v", deadline, t0.Add(ttl))
	}

	// still held 9s in: a contender gets the wake instant back
	now = t0.Add(9 * time.Second)
	_, wake, ok := i.TryLock("k", ttl)
	if ok {
		t.Fatal("key should still be held")
	}
	if !wake.Equal(t0.Add(ttl).Add(grace)) {
		t.Fatalf("wake = %v, want %v", wake, t0.Add(ttl).Add(grace))
	}

	// the holder extends: deadline moves, version moves, old version is dead
	now = t0.Add(9 * time.Second)
	v2, deadline2, err := i.ExtendLock("k", v, ttl)
	if err != nil {
		t.Fatalf("extend failed: %v", err)
	}
	if v2 <= v {
		t.Fatalf("version did not move: %d -> %d", v, v2)
	}
	if !deadline2.Equal(now.Add(ttl)) {
		t.Fatalf("deadline after extend = %v, want %v", deadline2, now.Add(ttl))
	}
	if _, _, err := i.ExtendLock("k", v, ttl); err == nil {
		t.Fatal("the old version must not be able to extend")
	}
	if err := i.ReleaseLock("k", v); err == nil {
		t.Fatal("the old version must not be able to release")
	}

	// extend keeps the lease alive: a contender still cannot get in
	_, _, ok = i.TryLock("k", ttl)
	if ok {
		t.Fatal("key should still be held after the extend")
	}

	// expiry is terminal: no extend, no release, no acquire
	now = deadline2.Add(time.Second)
	if _, _, err := i.ExtendLock("k", v2, ttl); err == nil {
		t.Fatal("extending an expired lease must fail")
	}
	if err := i.ReleaseLock("k", v2); err == nil {
		t.Fatal("releasing an expired lease must fail")
	}
	if _, _, ok := i.TryLock("k", ttl); ok {
		t.Fatal("the cooldown must block a new acquire")
	}

	// once the cooldown is over, the key is free
	now = deadline2.Add(grace)
	if _, _, ok := i.TryLock("k", ttl); !ok {
		t.Fatal("key should be free after the cooldown")
	}
}

func TestSweepLocks(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	now := t0
	i := newTestIdx()
	i.now = func() time.Time { return now }

	if _, _, ok := i.TryLock("k", time.Second); !ok {
		t.Fatal("acquire failed")
	}

	now = t0.Add(time.Second) // expired, but in cooldown: entry stays
	if n := i.SweepLocks(); n != 0 {
		t.Fatalf("swept %d entries during the cooldown", n)
	}

	now = t0.Add(time.Second + DefaultGrace) // past deadline+grace
	if n := i.SweepLocks(); n != 1 {
		t.Fatalf("swept %d entries, want 1", n)
	}
	if len(i.locks) != 0 {
		t.Fatalf("lease table not empty: %d entries", len(i.locks))
	}
}

// The quarantine window is the same for a 1s lease and a 1h lease.
func TestGraceIsIndependentOfTTL(t *testing.T) {
	t0 := time.Unix(1_000_000_000, 0)
	now := t0
	i := newTestIdx()
	i.now = func() time.Time { return now }

	for _, ttl := range []time.Duration{time.Second, time.Minute, time.Hour} {
		i.locks = map[string]*lockEntry{}
		_, deadline, ok := i.TryLock("k", ttl)
		if !ok {
			t.Fatalf("acquire of a %v lease failed", ttl)
		}

		now = deadline // just expired
		_, wake, ok := i.TryLock("k", ttl)
		if ok {
			t.Fatalf("a %v lease must still be in quarantine at expiry", ttl)
		}
		if !wake.Equal(deadline.Add(DefaultGrace)) {
			t.Fatalf("quarantine for a %v lease is %v, want %v", ttl, wake.Sub(deadline), DefaultGrace)
		}

		now = deadline.Add(DefaultGrace)
		if _, _, ok := i.TryLock("k", ttl); !ok {
			t.Fatalf("key should be free after the %v quarantine", DefaultGrace)
		}
		now = t0
	}
}

func TestLockGraceOption(t *testing.T) {
	i := newTestIdx()
	if i.lockGrace != DefaultGrace {
		t.Fatalf("default grace is %v, want %v", i.lockGrace, DefaultGrace)
	}
	WithLockGrace(5 * time.Second)(i)
	if i.lockGrace != 5*time.Second {
		t.Fatalf("WithLockGrace ignored: %v", i.lockGrace)
	}
	WithLockGrace(0)(i) // a nonsense value must not disable the quarantine
	if i.lockGrace != 5*time.Second {
		t.Fatalf("zero grace clobbered the configured value: %v", i.lockGrace)
	}
}

// Real-clock behaviour of the blocking acquire.
func TestLockBlocking(t *testing.T) {
	i := newTestIdx()
	i.lockGrace = 50 * time.Millisecond // short quarantine so the test stays fast
	ttl := 100 * time.Millisecond

	h1, err := i.Lock("k", ttl, 5*time.Second)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	if h1.Expired() || h1.Key() != "k" {
		t.Fatalf("handle looks wrong: key=%q expired=%v", h1.Key(), h1.Expired())
	}

	start := time.Now()
	h2, err := i.Lock("k", ttl, 5*time.Second)
	if err != nil {
		t.Fatalf("second lock: %v", err)
	}
	waited := time.Since(start)
	if waited < ttl {
		t.Fatalf("acquired after %v, before the lease expired", waited)
	}
	if h2.Key() != "k" {
		t.Fatalf("wrong key")
	}

	// the second holder must be able to extend the fresh lease
	if err := h2.Extends(); err != nil {
		t.Fatalf("extend failed: %v", err)
	}
}

func TestLockTimeout(t *testing.T) {
	i := newTestIdx()
	ttl := 200 * time.Millisecond

	if _, err := i.Lock("k", ttl, 5*time.Second); err != nil {
		t.Fatalf("first lock: %v", err)
	}
	_, err := i.Lock("k", ttl, 50*time.Millisecond)
	if !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("expected ErrLockTimeout, got %v", err)
	}
}

func TestReleaseFreesImmediately(t *testing.T) {
	i := newTestIdx()
	ttl := 10 * time.Second

	h, err := i.Lock("k", ttl, time.Second)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	h2, err := i.Lock("k", ttl, time.Second)
	if err != nil {
		t.Fatalf("re-acquire after release should be immediate: %v", err)
	}
	if h2.Key() != "k" {
		t.Fatal("wrong key")
	}
	if err := h.Release(); err == nil {
		t.Fatal("releasing twice must fail")
	}
}

// Self-contention is expected: a client blocks on a key it already holds.
func TestSelfContention(t *testing.T) {
	i := newTestIdx()
	i.lockGrace = 50 * time.Millisecond
	ttl := 100 * time.Millisecond

	if _, err := i.Lock("k", ttl, 5*time.Second); err != nil {
		t.Fatalf("lock: %v", err)
	}
	start := time.Now()
	if _, err := i.Lock("k", ttl, 5*time.Second); err != nil {
		t.Fatalf("second lock: %v", err)
	}
	if time.Since(start) < ttl {
		t.Fatal("the client acquired its own key before the lease expired")
	}
}

func TestLockValidation(t *testing.T) {
	i := newTestIdx()
	for _, tc := range []struct {
		key     string
		ttl     time.Duration
		timeout time.Duration
	}{
		{"", time.Second, time.Second},
		{"k", 0, time.Second},
		{"k", -1, time.Second},
		{"k", time.Hour * 2, time.Second},
		{"k", time.Second, 0},
		{string(make([]byte, 300)), time.Second, time.Second},
	} {
		if _, err := i.Lock(tc.key, tc.ttl, tc.timeout); err == nil {
			t.Fatalf("Lock(%q, %v, %v) should have failed", tc.key, tc.ttl, tc.timeout)
		}
	}
}

// Leases are in memory only: a restart of the server (same folder) sees none,
// and closing the client releases what it holds.
func TestLeasesAreNotPersisted(t *testing.T) {
	folder := t.TempDir()

	i1, err := New(folder)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ttl := 10 * time.Second
	h, err := i1.Lock("k", ttl, time.Second)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := i1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := h.Extends(); err == nil {
		t.Fatal("a handle from a closed client must not extend")
	}

	i2, err := New(folder)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer i2.Close()
	if _, _, ok := i2.(*idx).TryLock("k", ttl); !ok {
		t.Fatal("a fresh server must not inherit leases from disk")
	}
}
