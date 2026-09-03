# gossip2

A pub/sub service over TCP with durable compare-and-swap publishes and transient signals. `Publish` records state in binary log files and broadcasts it; `Signal` broadcasts without writing to disk.

## API

### Client Interface

```go
type Client interface {
    Signal(topic string, id string, data []byte) error
    Publish(topic string, id string, old Version, data []byte) (Version, error)
    Subscribe(topic string, h Handler) (func(), error)
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

Closes the client and releases its network connection or open log files. Call it when the client is no longer needed.

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
    cli, err := lib.New("/path/to/log/folder")
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
}
```

### Running as a TCP Server

```
gossip2 [-l addr] folder
```

- `folder` — directory for log files
- `-l addr` — listen address (default: `localhost:1337`)

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
