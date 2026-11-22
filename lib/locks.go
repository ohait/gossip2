package gossip

import (
	"errors"
	"fmt"
	"time"
)

// Lease-based locks: a short-lived, in-memory lock on a key.
//
// A lease goes through three states:
//
//	t0 ─────────────── t0+T ─────────────── t0+T+G ─────────────►
//	   ACTIVE              COOLDOWN              FREE
//
// Expiry is terminal: an expired lease can neither be extended nor released.
// The cooldown (grace) is a quarantine so the previous holder's in-flight work
// cannot overlap with a new holder. It is a fixed recovery window, independent
// of the ttl: a short lease does not mean the old holder finished quickly.
// An explicit Release skips the cooldown: the key is free immediately.
//
// Ownership is proven by the CAS version of the lease, which the handle keeps
// internally. It is deliberately not exposed: a live handle is by construction
// the only one holding that version.

// ErrLockTimeout is the only typed lock error. Everything else (the lease died,
// the key is held by someone else, the server restarted, the client is closed,
// bad arguments) is a plain error: the caller's move is the same in every case.
var ErrLockTimeout = errors.New("lock timeout")

const (
	MinLockTTL    = time.Second
	MaxLockTTL    = time.Hour
	MaxLockWait   = time.Hour
	MaxLockKeyLen = 256 // the wire format caps ids at 256 bytes
	MaxLockMsg    = 1 << 10
	MinLockSleep  = 20 * time.Millisecond
	DefaultGrace  = 30 * time.Second // quarantine after expiry, independent of ttl
)

// LockHandle is a lease the caller owns. One struct serves both the local and
// the TCP client: they differ only in the extend/release closures Lock plugs in.
type LockHandle struct {
	key      string
	version  Version // CAS proof of ownership, deliberately not exposed
	deadline time.Time
	ttl      time.Duration

	extend  func(v Version, ttl time.Duration) (Version, time.Time, error)
	release func(v Version) error
}

func NewLockHandle(key string, version Version, deadline time.Time, ttl time.Duration,
	extend func(v Version, ttl time.Duration) (Version, time.Time, error),
	release func(v Version) error) *LockHandle {
	return &LockHandle{key: key, version: version, deadline: deadline, ttl: ttl, extend: extend, release: release}
}

func (h *LockHandle) Key() string { return h.key }

// Expired is a local, approximate check (client clock, no cooldown awareness).
// The server is authoritative: Extends is the real answer.
func (h *LockHandle) Expired() bool { return !time.Now().Before(h.deadline) }

func (h *LockHandle) Deadline() time.Time { return h.deadline }

func (h *LockHandle) Extends() error {
	if h.extend == nil {
		return fmt.Errorf("lock handle is not bound to a lease")
	}
	v, deadline, err := h.extend(h.version, h.ttl)
	if err != nil {
		return err
	}
	h.version = v
	h.deadline = deadline
	return nil
}

func (h *LockHandle) Release() error {
	if h.release == nil {
		return fmt.Errorf("lock handle is not bound to a lease")
	}
	return h.release(h.version)
}

func ValidateLockKey(key string) error {
	if key == "" {
		return fmt.Errorf("lock key cannot be empty")
	}
	if len(key) > MaxLockKeyLen {
		return fmt.Errorf("lock key too long: %d (max %d)", len(key), MaxLockKeyLen)
	}
	return nil
}

func ValidateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("lock ttl must be positive")
	}
	if ttl > MaxLockTTL {
		return fmt.Errorf("lock ttl too long: %v (max %v)", ttl, MaxLockTTL)
	}
	return nil
}

func ValidateTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("lock timeout must be positive")
	}
	if timeout > MaxLockWait {
		return fmt.Errorf("lock timeout too long: %v (max %v)", timeout, MaxLockWait)
	}
	return nil
}

// TryLock is the primitive behind the wait loop. On success it returns the lease
// version and the deadline; on failure it returns the instant the key becomes free.
func (i *idx) TryLock(key string, ttl time.Duration) (Version, time.Time, bool) {
	i.lockMu.Lock()
	defer i.lockMu.Unlock()

	now := i.now()
	e := i.locks[key]
	if e != nil {
		freeAt := e.deadline.Add(i.lockGrace)
		if now.Before(freeAt) {
			return 0, freeAt, false
		}
	}
	if e == nil {
		e = &lockEntry{key: key}
		i.locks[key] = e
	}
	e.version = max(Version(now.UnixNano()), e.version+1)
	e.deadline = now.Add(ttl)
	return e.version, e.deadline, true
}

func (i *idx) ExtendLock(key string, version Version, ttl time.Duration) (Version, time.Time, error) {
	i.lockMu.Lock()
	defer i.lockMu.Unlock()

	e := i.locks[key]
	if e == nil {
		return 0, time.Time{}, fmt.Errorf("lock %q does not exist", key)
	}
	if e.version != version {
		return 0, time.Time{}, fmt.Errorf("lock %q is held by someone else", key)
	}
	now := i.now()
	if !now.Before(e.deadline) {
		return 0, e.deadline, fmt.Errorf("lock %q expired at %s", key, e.deadline.Format(time.RFC3339))
	}
	e.version = max(Version(now.UnixNano()), e.version+1)
	next := now.Add(ttl)
	if e.deadline.After(next) {
		next = e.deadline
	}
	e.deadline = next
	return e.version, e.deadline, nil
}

func (i *idx) ReleaseLock(key string, version Version) error {
	i.lockMu.Lock()
	defer i.lockMu.Unlock()

	e := i.locks[key]
	if e == nil {
		return fmt.Errorf("lock %q is not held", key)
	}
	if e.version != version {
		return fmt.Errorf("lock %q is held by someone else", key)
	}
	if !i.now().Before(e.deadline) {
		return fmt.Errorf("lock %q has expired", key)
	}
	delete(i.locks, key)
	return nil
}

// SweepLocks is hygiene only: correctness comes from the acquire path above
// overwriting dead entries. It runs on the compaction tick (5s), so a dead
// entry can linger at most one tick without affecting who can acquire the key.
// Returns the number of entries removed.
func (i *idx) SweepLocks() int {
	i.lockMu.Lock()
	defer i.lockMu.Unlock()

	now := i.now()
	n := 0
	for key, e := range i.locks {
		if !now.Before(e.deadline.Add(i.lockGrace)) {
			delete(i.locks, key)
			n++
		}
	}
	return n
}

// Lock acquires a lease on key. It blocks until the key is free, timeout
// elapses, or the client is closed.
func (i *idx) Lock(key string, ttl, timeout time.Duration) (*LockHandle, error) {
	if err := ValidateLockKey(key); err != nil {
		return nil, err
	}
	if err := ValidateTTL(ttl); err != nil {
		return nil, err
	}
	if err := ValidateTimeout(timeout); err != nil {
		return nil, err
	}

	end := i.now().Add(timeout)
	for {
		v, wake, ok := i.TryLock(key, ttl)
		if ok {
			// wake is the deadline when the acquire succeeded
			h := NewLockHandle(key, v, wake, ttl,
				func(ver Version, t time.Duration) (Version, time.Time, error) {
					return i.ExtendLock(key, ver, t)
				},
				func(ver Version) error {
					return i.ReleaseLock(key, ver)
				})
			i.lockMu.Lock()
			i.holds[key] = h
			i.lockMu.Unlock()
			return h, nil
		}
		if !i.now().Before(end) {
			return nil, ErrLockTimeout
		}
		wait := min(wake.Sub(i.now()), end.Sub(i.now()))
		if wait < MinLockSleep {
			wait = MinLockSleep
		}
		select {
		case <-time.After(wait):
		case <-i.closed:
			return nil, errors.New("client closed")
		}
	}
}
