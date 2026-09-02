package gossip

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ohait/gossip2/enc"
	"github.com/ohait/gossip2/sync"
)

type Client interface {
	Signal(topic string, id string, data []byte) error
	Publish(topic string, id string, old enc.Version, data []byte) (enc.Version, error)
	Subscribe(topic string, h Handler) (func(), error)
	Close() error
}

type Handler func(topic string, id string, v enc.Version, data []byte) error

// CASFailed is returned when a Compare-And-Swap check fails
type CASFailed struct {
	Given    enc.Version
	Expected enc.Version
}

func (e CASFailed) Error() string {
	return fmt.Sprintf("CAS Failed: given %v but expected %v", e.Given, e.Expected)
}

func convertV1(path string) (string, error) {
	log.Printf("converting %q to v2...", path)
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
	err = enc.ReadV1(f1, func(topic, id string, v enc.Version, data []byte) error {
		_, err := logRecord{
			topic: topic,
			id:    id,
			v:     v,
			data:  data,
		}.writeBuf(buf)
		return err
	})
	if err != nil {
		os.Remove(newPath)
		return "", err
	}
	log.Printf("converted %q to %q", path, newPath)
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
		log.Printf("reading %q...", path)
		if strings.HasSuffix(path, ".bin") { // v1
			var err error
			path, err = convertV1(path)
			if err != nil {
				return err
			}
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
		log.Printf("log %q has %d/%d stale bytes", l.f.Name(), l.stale.Load(), int(l.total.Load()))
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
				log.Printf("compact error: %v", err)
			}
		}
	}()
	return cli, nil
}

// Reorder returns a Handler that filters old messages when out-of-order
func Reorder(h Handler) Handler {
	m := sync.Mutex{}
	last := map[string]map[string]enc.Version{}
	return func(topic string, id string, v enc.Version, data []byte) error {
		if v == 0 {
			return h(topic, id, v, data)
		}
		m.Lock()
		defer m.Unlock()
		if last[topic] == nil {
			last[topic] = map[string]enc.Version{}
		}
		if v < last[topic][id] {
			m.Unlock()
			return nil
		}
		last[topic][id] = v
		return h(topic, id, v, data)
	}
}
