# Plan: lease-based `Lock` API

Status: **plan, not implemented**. Target: `lib.Client`, so the local client and the TCP client behave identically.

Decisions fixed by the design review:
- locks live in **their own namespace**, not in topics/records — this is not a `Publish` variant;
- ownership is proven by a **CAS version** the client holds, **not** by the connection → **disconnection does not release**;
- **no persistence** — leases are in-memory and short-lived;
- expiry is **terminal**: an expired lease cannot be extended or released;
- the cooldown exists **only after expiry** — an explicit `Release()` frees the key immediately;
- during the cooldown, `Lock` **does not error** — it behaves as "still locked" and **waits** until the cooldown ends;
- **self-contention is expected**: a client blocking on a key it already holds is treated like any other contender;
- ownership proof is the CAS version alone — guessability is accepted, not mitigated.

---

## 1. API

```go
type Client interface {
    Signal(topic string, id string, data []byte) error
    Publish(topic string, id string, old Version, data []byte) (Version, error)
    Subscribe(topic string, h Handler) (func(), error)

    Lock(key string, ttl, timeout time.Duration) (*LockHandle, error)
    Close() error
}

// one struct, not an interface: the local client and the TCP client differ
// only in the extend/release closures Lock plugs into it
type LockHandle struct {
    key      string
    version  Version // CAS proof of ownership, not exposed
    deadline time.Time
    ttl      time.Duration
    extend   func(v Version, ttl time.Duration) (Version, time.Time, error)
    release  func(v Version) error
}
```

**The CAS version is not exposed.** It is an internal field of the concrete handle and is only sent on the wire. The holder can only lose the key after the lease expires, so a live handle is by construction the only one holding that version — exposing it invites callers to reason about a detail they cannot usefully act on.

`Expired()` is a local, approximate check (client clock, no cooldown awareness). It is for logging and cheap pre-checks; the server is authoritative — `Extends()` is the real answer.

**No `context` anywhere.** `Publish`, `Signal` and `Subscribe` are already context-free, so threading a ctx only through `Lock` would be inconsistent, and returning a ctx that carries the handle in a `context.Value` is an antipattern that buys nothing. `timeout` *is* the cancellation mechanism.

Errors:

```go
ErrLockTimeout   // waited `timeout` and the key never came free
```

**This is the only typed error.** Everything else — the lease died, someone else owns the key, the server restarted, the client is closed, bad arguments — is returned as a plain wrapped `error` carrying a human-readable message. None of those are actionable in different ways: the caller's move is the same (give up, or try again later), so a `errors.Is` taxonomy would only add ceremony.

There is **no** `ErrLockHeld` and **no** cooldown error: `Lock` is a blocking acquire. It returns either a handle or an error caused by `timeout` elapsing, the client being closed, or invalid arguments.

Notes:
- `handler` collides with the existing `Handler` type → use `LockHandle`.
- **`timeout` is mandatory.** It is the maximum time the caller is willing to wait for the key. It cannot be 0 or negative — if you want "effectively forever", pass something arbitrarily large (e.g. `24 * time.Hour`).
- `ttl` is the lease duration; `timeout` bounds the lock *attempt*. Two different knobs, both explicit.
- The only other way a wait ends early is `Close()` on the client: the wait loop checks the existing `closing`/`closed` channels and returns `ErrClientClosed`.
- Future options are added as new keys in the JSON payload (§4) without changing this signature.
- With a large `timeout`, `Lock` can block for a long time against a holder that keeps extending. Document this loudly.

---

## 2. Semantics: active → expired (cooldown) → free

Acquired at `t0` with ttl `T` and grace `G`:

```
t0 ─────────────── t0+T ─────────────── t0+T+G ─────────────►
   ACTIVE              COOLDOWN              FREE
   holder owns it      nobody owns it        anyone can lock
   extend / release    extend ✗ release ✗    lock ✓
   others: wait        others: wait          others: acquire
```

- **Active** (`now < deadline`): the holder may `Extends` (full `ttl`, deadline = `max(deadline, now+ttl)`) or `Release`. Anyone else calling `Lock` waits.
- **Expired but cooling** (`deadline <= now < deadline+G`): the lease is gone. `Extends` and `Release` both fail with a plain error. Nobody can acquire — `Lock` waits until `deadline+G`.
- **Free** (`now >= deadline+G`): the entry is deleted and anyone can acquire.

The cooldown is **not** a second chance for the owner. It is a quarantine so the previous holder's in-flight work (which it still believes is protected) cannot overlap with a new holder, and so clock skew / in-flight extend packets arriving late do not instantly hand the key to someone else.

`G` is server-side, not per client (otherwise the guarantee is meaningless). It is a fixed recovery window: `G = 30s` by default, independent of `T` — a 1s lease does not mean the previous holder's work finished in 1s. Configurable with `lib.Config{Folder: ..., LockGrace: G}.New()` — it must be set explicitly, a config without a positive grace is refused — and the `-lock-grace` server flag.

Explicit `Release()` is instant: the entry is deleted and the key is free, no cooldown. Cooldown exists only for expiry — the holder is declaring it is done, so there is nothing left to protect against.

---

## 3. Storage: in-memory, CAS-guarded

New file `lib/locks.go`:

```go
type lockEntry struct {
    key      string
    version  Version // CAS guard, bumped on acquire and every extend
    deadline time.Time
}

// in idx:
lockMu sync.Mutex
locks  map[string]*lockEntry
lockGrace time.Duration // server policy, not per-lease state
```

No log records, no topics, no replay, no compaction interaction. A server restart wipes all leases; anyone holding a handle gets a plain error on the next `Extends` and must re-lock. That is acceptable because leases are short-lived.

`TryLock` (the primitive behind the wait loop):

```go
func (i *idx) TryLock(key string, ttl time.Duration) (Version, time.Time, bool) {
    i.lockMu.Lock()
    defer i.lockMu.Unlock()
    now := i.now()
    e := i.locks[key]
    if e != nil {
        freeAt := e.deadline.Add(i.lockGrace)
        if now.Before(freeAt) {
            return 0, freeAt, false   // wake at this instant
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
```

`Lock` loops on `tryLock` until it succeeds, `timeout` elapses, or the client is closed, sleeping until the returned wake instant (with a small jitter and a floor, e.g. 20–50 ms, so a busy key does not spin).

Cleanup:
- **Lazy**: the acquire path above already overwrites an expired entry — correctness does not depend on any timer.
- **Sweeper**: piggybacked on the existing compaction loop (5 s tick) rather than its own goroutine, deleting entries where `now >= deadline+lockGrace`, so the map does not accumulate dead keys. Since the lazy path is already correct, the tick interval only bounds how long a dead entry lingers.

---

## 4. Wire protocol (`net/msg.go`, `net/server.go`, `net/client.go`)

New command bytes:

```
'L'  lock     rid, id=key, v=0,           message=lockReq
'X'  extend   rid, id=key, v=leaseVersion, message=lockReq
'R'  release  rid, id=key, v=leaseVersion, no payload
```

`v` keeps its existing meaning: the CAS version (`0` = no check, exactly like `Publish(..., old=0)`). Everything else goes in the payload, so `v` is never overloaded.

### Payload encoding: JSON

`encoding/json` from the stdlib. The payload is a handful of numbers on the control plane — a new dependency is not worth it, and gossip2 currently has exactly one (`xxhash`).

Decoding into a struct **ignores unknown keys** by default, so a newer client talking to an older server degrades gracefully:

```go
// net/msg.go
type lockReq struct {
    TTL     int64 `json:"ttl"`     // nanoseconds, > 0
    Timeout int64 `json:"timeout"` // nanoseconds, > 0
    // future options are added as new keys; unknown keys are ignored on decode
}
```

Why JSON:
- **zero dependencies**, and the payload is ~30 bytes;
- **named keys** — adding `owner`, `tags` or `priority` later is a one-line struct change;
- **debuggable**: a lock payload can be dumped and read by eye;
- unknown-key tolerance gives the same forward compatibility msgpack would.

What you give up, and why it does not matter here:
- reflection-based marshal/unmarshal, roughly 1–2 µs per call. Control-plane only — never put this on the record/publish hot path.
- JSON has no type safety at the wire level (`"ttl":"5s"` parses to an error, which is fine — it is rejected).
- **size cap required.** Wrap the decode in a 1 KiB `io.LimitReader` and never decode untrusted payloads into `map[string]any` — always into the struct.

No format-version field: the **command byte is the schema selector**. `'L'` means "this is a v1 lock request". If the format ever changes incompatibly, it gets a new command byte — that is how the protocol already versions itself, and it is checked by the switch that dispatches, not by a field the server has to remember to inspect.

Rules:
- the command byte selects the codec: `'L'/'X'/'R'` payloads are JSON, `'P'/'E'` payloads stay compressed record data. No magic prefix needed.
- keys are stable API: renaming one is a breaking change.

Responses — **no payload**:

| case | cmd | what carries the information |
|---|---|---|
| acquired / extended | `ack` | `v` = the new lease CAS version. That is all the client needs. |
| released | `ack` | nothing. |
| key still held (active or in cooldown) | `nack` | `v` = wake-at unix nano; `message` = human-readable text for logs. Not an API error — the client sleeps to `v` and retries `'L'`. |
| hard failure (expired, lost, bad args, server error) | `nack` | `message` = text. Client wraps it into the error. |

Why this is enough:
- **The server is authoritative on expiry.** The client never needs the server's deadline to be correct; it only needs it to back off sensibly. It can compute `sentAt + ttl` locally and use that for logging and retry timing.
- **`Extends` genuinely needs `v`** — the handle must store the new CAS version for its next extend. Everything else is derivable from what the client already sent.
- **The wake-at hint rides in the existing `v` field**, so contention needs no encoding at all. If you dislike `v` being overloaded, drop the hint and let the client retry with jittered exponential backoff capped at 1 s — for short-lived locks that is a handful of extra round trips.

Consequence of no response payload: **the server must not silently clamp `ttl`.** If the client sends 2 h and the server quietly makes it 1 h, the client's local deadline is wrong. Either validate strictly (reject out-of-range ttl with a nack) or accept the drift. Recommend strict validation.

The **request** payload stays JSON (`lockReq` above) — that is where the options live. Only responses go payload-free.

Reuse the existing request pattern (random rid + `pending` map + wait for ack), exactly like `Publish`. Payloads are ~40 bytes; skip `enc.Compress`.

Server-side validation: `ttl` and `timeout` must both be `> 0`, and `ttl` must be within `[1s, maxTTL]` (configurable on `Server`). Reject out-of-range values with a nack rather than clamping them — with no response payload, a silent clamp would desync the client's local deadline. `timeout` is capped at a server-configured `maxWait` so a client cannot pin a request indefinitely; the cap is a server protection, not a semantic change, so returning it as a timeout error is fine.

`Extends` sends the payload back with the ttl stored in the handle, so a lease can be renewed with a different ttl than the original if the caller wants. `Release` needs no payload — the version in `v` is the whole claim.

**Where the wait happens.** Recommended: **client-side** — the server answers immediately with "held until X", the client sleeps and retries. This keeps the server from blocking a connection's request loop, and the wait bound (`timeout`) is already in the request. A server-side wait (one `'L'` that acks only when acquired, run in a goroutine per request) is the alternative, but without a ctx on the wire the client has no way to cancel it short of closing the connection — so it needs a cancel command and per-key waiter lists for the same observable behaviour.

**CAS is the ownership proof.** `Extends`/`Release` send the handle's version (internal to the handle, never surfaced). A mismatch is a CAS failure → a plain error. This is what makes the handle connection-independent: after a reconnect the same handle can still extend or release, as long as the lease is still alive.

---

## 5. Connection lifecycle

- **Disconnection does not release anything.** No per-conn lease bookkeeping is needed for correctness; the lease is keyed by `key` and guarded by its version.
- TCP reconnect (`stayConnected`): subscriptions replay, leases are untouched. The handle stays valid until its deadline. `Extends` after a reconnect works.
- Consequence to document: a client that dies without releasing keeps the key for `T + G`. Callers that want prompt release must call `Release` explicitly (defer it).
- Optional nicety: if the TCP client is `Close()`d explicitly, release any handles it still holds. Not for dropped connections.

---

## 6. Edge cases

1. `ttl <= 0` or `timeout <= 0` → error at the API boundary, before any round trip.
2. `Extends`/`Release` on an expired lease → non-nil error, never `nil`. Handles do not resurrect.
3. Concurrent `Extends` on the same handle → idempotent, deadline = `max(deadline, now+ttl)`.
4. `Release` twice → the second fails (the entry is already gone).
5. `Lock` on a key already held by the *same* client → it waits like any other contender (see §6.9). Use `handle.Extends()` when you want to keep your own lease.
6. Server restart → all leases gone → the next `Extends` errors. Callers must treat that as "re-acquire or give up".
7. Server clock is authoritative. Client-side `Deadline()` is only for logging/backoff.
8. **Guessable lease versions are accepted.** Versions are time-based (`max(now, old+1)`), so a lease version is guessable by anyone who can approximate the clock. In this trust model that is not a problem worth solving — the CAS version is the ownership proof, full stop. If a stronger model is ever needed, the answer would be a random token minted at acquire and required in `Extends`/`Release` (8 extra bytes), but that is not part of this design.
9. **Self-contention is expected behaviour.** A client that calls `Lock` on a key it already holds waits through the cooldown, exactly like any other contender. With a large `timeout` that can mean waiting a long time for your own lease to expire — that is intentional, not a deadlock to work around. Reentrancy (re-acquiring your own live lease, or counting nested acquisitions) is a **separate task**, not part of v1.
10. A holder that never extends but keeps working past `deadline` is a bug the protocol cannot detect; the cooldown bounds the overlap window but does not prevent it. If hard exclusion is required, gate writes (§7).
11. Keep `Lock` out of the topic/record machinery entirely — no `_lock` topic, no replay, no compaction interaction.

---

## 7. Later (v2): make the lock protect writes

If the point of the lock is exclusive access to a resource, gate the write:

```go
cli.PublishWithLock(h, topic, id, old, data)   // h is the LockHandle
// storeCAS: handle still active for this key? else "lock not held"
```

Separate from v1 because it changes the write path. Note this is where a `ctx` would eventually be worth reintroducing — if `Publish` ever becomes context-aware, do it for the whole API, not just for locks.

---

## 8. Implementation order

| # | work | files |
|---|---|---|
| 1 | `lockEntry`, `tryLock`, `extend`, `release`, `ErrLockTimeout`, injectable clock | `lib/locks.go` (new) |
| 2 | Add `Lock` to `Client`, sweeper loop in `lib.New` | `lib/gossip.go`, `lib/idx.go` |
| 3 | Command bytes + JSON payload structs | `net/msg.go` |
| 4 | Server handlers for `'L'`, `'X'`, `'R'` (no conn cleanup) | `net/server.go` |
| 5 | TCP client `Lock` wait/retry loop, `Extends`, `Release` | `net/client.go` |
| 6 | Tests (§9) | `net/lock_test.go` |
| 7 | README API section | `README.md` |

Rough effort: ~1 day including tests.

---

## 9. Tests

1. **State machine** with an injected clock (no sleeps): acquire → held; advance past `T` → `Extends`/`Release` both error; advance past `T+G` → acquirable and the old entry is gone.
2. **Blocking acquire**: A holds the key; B calls `Lock` → B blocks and acquires exactly at `deadline+G` (assert timing, tolerance ±10 ms). With `timeout` shorter than the hold, B returns `ErrLockTimeout`.
3. **Contention**: N goroutines `Lock` the same key → each eventually succeeds in turn, never overlapping. Run with `-race`.
4. **Extend keeps it alive**: extend at `T-ε`; a competing `Lock` waits.
5. **Release frees immediately**: no cooldown after an explicit release.
6. **Reconnect keeps the lease**: acquire over TCP, close the connection, reconnect with the same handle, `Extends` succeeds while inside `T`.
7. **Restart clears leases**: acquire, restart the server, `Extends` errors.
8. **Cleanup**: after cooldown, the sweeper empties the lease map.
9. **Payload forward compatibility**: a request payload with an extra unknown key decodes without error and the known fields still apply. `timeout` bounds the wait: with `timeout = 50 ms` and a key held for 200 ms, `Lock` returns `ErrLockTimeout` rather than blocking.
10. **Payload guard**: a lock *request* payload over 1 KiB is rejected before decoding; a malformed JSON stream returns an error rather than a panic.
11. **Self-contention**: the same client calls `Lock` twice on the same key → the second call waits until `deadline+G` and then acquires. Documented behaviour, not a bug.
12. **No response payload**: assert that a successful `Lock`/`Extends` ack has an empty `message` and that the handle's version comes from `ack.v` alone.

Keep real-clock tests short (ttl 100–200 ms) and drive the rest with the injected clock.
