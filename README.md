# gossip2

A pub/sub service over TCP with durable compare-and-swap publishes and transient signals. `Publish` records state in binary log files and broadcasts it; `Signal` broadcasts without writing to disk.

## API

### Client Interface

```go
type Client interface {
    Signal(topic string, id string, data []byte) error
    Publish(topic string, id string, old Version, data []byte) (Version, error)
    Subscribe(topic string, h Handler) (func(), error)
    Lock(key string, ttl, timeout time.Duration) (LockHandle, error)
    Close() error
}

type Handler func(topic string, id string, v Version, data []byte) error
```

#### `Signal(topic, id, data)`

Broadcasts a transient message to all subscribers of a topic. Signals are not written to the log and are delivered with version `0`.

#### `Publish(topic, id, old, data)`

Publishes a message with Compare-And-Swap semantics. The `old` parameter is the expected current version for the given topic+id. If `old` is non-zero and doesn't match the current version, `CASFailed` is returned. Otherwise, the message is stored and a new version is returned.

Use `old = 0` to skip the CAS check and always publish.

#### `Subscribe(topic, handler)`

Subscribes to messages on a topic. The handler is called for each message published to the topic. Returns an unsubscribe function that should be called to stop receiving messages.

#### `Close()`

Closes the client and releases its network connection or open log files. Call it when the client is no longer needed. Closing a client also releases the locks it still holds.

### Locks

`Lock(key, ttl, timeout)` is a short-lived, in-memory lease on a key. It blocks until the key is free, `timeout` elapses (then `ErrLockTimeout`), or the client is closed. Locks are not persisted: a server restart wipes them.

```go
handle, err := cli.Lock("shipment-42", 30*time.Second, 5*time.Minute)
if err != nil {
    return err // ErrLockTimeout, or a plain error
}
defer handle.Release()

// do work, keep the lease alive if it is long running
if err := handle.Extends(); err != nil {
    return err // the lease is gone
}
```

```go
handle.Key()      string
handle.Expired()  bool      // local, approximate: the server is authoritative
handle.Deadline() time.Time // local view of when the lease expires
handle.Extends()  error     // renew the lease; fails once the lease is dead
handle.Release()  error     // free the key immediately
```

A lease is held for `ttl` and then enters a fixed cooldown of 30 seconds (independent of `ttl`, set with `lib.Config{Folder: ..., LockGrace: G}.New()` or the server's `-lock-grace`) during which nobody can take the key. Expiry is terminal: an expired lease can neither be extended nor released. An explicit `Release()` skips the cooldown. Ownership is proven by a CAS version the handle carries internally, so a handle survives a reconnect: the lease lives in the server, not in the connection.

A client blocks like any other contender on a key it already holds; reentrancy is not part of the API.

```go
handle, err := cli.Lock("doc-1", 10*time.Second, time.Minute)
if errors.Is(err, lib.ErrLockTimeout) {
    // the key was not free within the timeout
}
```

### Version Type

`Version` is a `uint64` assigned to each durable publish. It is monotonic for successive updates of a topic+id and enables optimistic concurrency control via CAS.

### CASFailed Error

Returned by `Publish` when the expected version doesn't match. Contains the `Given` (expected) and `Expected` (actual) versions.

### Reorder Helper

`Reorder(h Handler) Handler` returns a handler that filters out-of-order messages, ensuring handlers only receive the latest version of each message ID per topic.

## Usage

### As a Library

```go
import (
    "log"
    "github.com/ohait/gossip2/lib"
)

func main() {
    cli, err := lib.Config{Folder: "/path/to/log/folder"}.New()
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()

    // Publish a message (version 0 = no CAS check)
    v, err := cli.Publish("news", "article-1", 0, []byte("Hello world"))
    if err != nil {
        log.Fatal(err)
    }
    LOG("published with version %d", v)

    // Subscribe to messages
    unsub, err := cli.Subscribe("news", func(topic, id string, v uint64, data []byte) error {
        LOG("[%s] %s: %s", topic, id, string(data))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
    defer unsub()

    // Signal a message (no CAS, always succeeds)
    cli.Signal("alerts", "alert-1", []byte("Server needs restart"))

    // Lock a key for 30 seconds, waiting at most 5 minutes for it
    h, err := cli.Lock("shipment-42", 30*time.Second, 5*time.Minute)
    if err != nil {
        log.Fatal(err)
    }
    defer h.Release()
}
```

### Running as a TCP Server

```
gossip2 [-l addr] [-lock-grace duration] folder
```

- `folder` — directory for log files
- `-l addr` — listen address (default: `localhost:1337`)
- `-lock-grace` — quarantine window after a lock lease expires (default: `30s`)

**Example:**
```bash
./gossip2 /var/lib/gossip2
./gossip2 -l :9000 /var/lib/gossip2
```

## Running Tests

```bash
go test ./...
```

## License

MIT License - see [LICENSE](LICENSE) file.
