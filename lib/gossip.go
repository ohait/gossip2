package gossip

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ohait/gossip2/enc"
)

type Client interface {
	Signal(topic string, id string, data []byte) error
	Publish(topic string, id string, old Version, data []byte) (Version, error)
	Subscribe(topic string, h Handler) (func(), error)
	Lock(key string, ttl, timeout time.Duration) (*LockHandle, error)
	Close() error
}

type Version = enc.Version

type Handler func(topic string, id string, v Version, data []byte) error

// CASFailed is returned when a Compare-And-Swap check fails
type CASFailed struct {
	Given    Version
	Expected Version
}

func (e CASFailed) Error() string {
	return fmt.Sprintf("CAS Failed: given %v but expected %v", e.Given, e.Expected)
}

// temporary until we deploy in dev/prod
func convertV1(path string) (string, error) {
	enc.LOG("converting %q to v2...", path)
	f1, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f1.Close()
	newPath := strings.TrimSuffix(path, ".bin") + ".v2"
	f2, err := os.Create(newPath)
	if err != nil {
		return "", err
	}
	defer f2.Close()
	buf := &enc.Buffer{ReadWriter: f2}
	if err := buf.WriteFull([]byte("GSP2")); err != nil {
		return "", err
	}
	err = enc.ReadV1(f1, func(topic, id string, v Version, data []byte) error {
		raw, err := enc.Decompress(data)
		if err != nil {
			// gossip v1 is compressed only when working server, while
			// not local, meaning we can only guess here
			// failing to decompress means we assume it's uncompressed
			raw = data
		}
		compressed, err := enc.Compress(raw)
		if err != nil {
			return err
		}
		_, err = logRecord{
			topic: topic,
			id:    id,
			v:     v,
			data:  compressed,
		}.writeBuf(buf)
		return err
	})
	if err != nil {
		os.Remove(newPath)
		return "", err
	}

	enc.LOG("converted %q to %q", path, newPath)
	return newPath, os.Remove(path)
}

// Config builds a client. Zero values mean defaults.
type Config struct {
	// Folder holds the log files. Required.
	Folder string

	// LockGrace is the quarantine window applied after a lease expires. It is a
	// fixed recovery window rather than a fraction of the ttl: the window covers
	// the previous holder's in-flight work, which has nothing to do with how long
	// the lease was meant to last. A 1s lease gets the same window as a 1h one.
	// Zero means DefaultGrace.
	LockGrace time.Duration
}

func (c Config) New() (Client, error) {
	if c.Folder == "" {
		return nil, fmt.Errorf("config: folder is required")
	}
	if err := os.MkdirAll(c.Folder, 0o755); err != nil {
		return nil, err
	}
	grace := c.LockGrace
	if grace <= 0 {
		grace = DefaultGrace
	}
	cli := &idx{
		closed:    make(chan struct{}),
		logFolder: c.Folder,
		locks:     map[string]*lockEntry{},
		holds:     map[string]*LockHandle{},
		lockGrace: grace,
		now:       time.Now,
	}
	err := cli.rangeLogs(func(path string) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		var head [4]byte
		_, err = f.Read(head[:])
		if err != nil {
			return err
		}
		f.Close()
		switch string(head[:]) {
		case "GSP1":
			enc.LOG("reading V1 log %q...", path)
			var err error
			path, err = convertV1(path)
			if err != nil {
				return err
			}
		case "GSP2":
			enc.LOG("reading V2 log %q...", path)
		default:
			return fmt.Errorf("unknown log format: %q", string(head[:]))
		}

		l, err := cli.openLog(path)
		if err != nil {
			return err
		}
		cli.sealedLogs = append(cli.sealedLogs, l)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, l := range cli.sealedLogs {
		enc.LOG("log %q has %d/%d stale bytes", l.f.Name(), l.stale.Load(), int(l.total.Load()))
	}
	go func() {
		// housekeeping: compact log files, then prune dead leases. The lease
		// sweep is hygiene only — the acquire path already overwrites dead
		// entries — so it rides the compaction tick instead of its own timer.
		// A dead entry can linger at most one tick; no key is ever handed out
		// too early because of it.
		sleep := 5 * time.Second
		for {
			select {
			case <-time.After(sleep):
			case <-cli.closed:
				return
			}
			if err := cli.compact(); err != nil {
				enc.LOG("compact error: %v", err)
			}
			if n := cli.SweepLocks(); n > 0 {
				enc.LOG("swept %d expired lock(s)", n)
			}
		}
	}()
	return cli, nil
}
