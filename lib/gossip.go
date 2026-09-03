package gossip

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ohait/gossip2/enc"
	"github.com/ohait/gossip2/sync"
)

type Client interface {
	Signal(topic string, id string, data []byte) error
	Publish(topic string, id string, old Version, data []byte) (Version, error)
	Subscribe(topic string, h Handler) (func(), error)
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
		_, err = logRecord{
			topic: topic,
			id:    id,
			v:     v,
			data:  raw,
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

func New(folder string) (Client, error) {
	if err := os.MkdirAll(folder, 0o755); err != nil {
		return nil, err
	}
	cli := &idx{
		closed:    make(chan struct{}),
		logFolder: folder,
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
		// compact loop
		sleep := 5 * time.Second
		for {
			select {
			case <-time.After(sleep):
			case <-cli.closed:
				return
			}
			err := cli.compact()
			if err != nil {
				enc.LOG("compact error: %v", err)
			}
		}
	}()
	return cli, nil
}

// Reorder returns a Handler that filters old messages when out-of-order
func Reorder(h Handler) Handler {
	m := sync.Mutex{}
	last := map[string]map[string]Version{}
	return func(topic string, id string, v Version, data []byte) error {
		if v == 0 {
			return h(topic, id, v, data)
		}
		m.Lock()
		defer m.Unlock()
		if last[topic] == nil {
			last[topic] = map[string]Version{}
		}
		if v < last[topic][id] {
			m.Unlock()
			return nil
		}
		last[topic][id] = v
		return h(topic, id, v, data)
	}
}
