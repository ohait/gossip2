package gossip

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/ohait/gossip2/sync"
)

var MAX_LOG_SIZE = 50 * 1024 * 1024 // 50 MB

type idx struct {
	mu sync.Mutex // used for logFiles management

	closed chan struct{} // closed when exiting

	logFolder string

	// current opened log file for writing (can be nil)
	curLog *logFile

	// previous logs opened
	sealedLogs []*logFile

	topics sync.Map[string, *topicIdx]
}

type topicIdx struct {
	mu      sync.Mutex // used for CAS write
	name    string
	records sync.Map[string, *pointer]
	subs    sync.Map[uint64, Handler]
}

type pointer struct {
	v    Version
	l    *logFile
	at   int
	size int
}

// storeCAS writes a log record for the given topic and id, returning the new version
// it locks on the topic first, then lock on the log file
func (i *idx) storeCAS(t *topicIdx, id string, old Version, data []byte) (Version, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r, _ := t.records.LoadOrStore(id, &pointer{v: old})
	if r.v != old {
		return 0, CASFailed{Given: old, Expected: r.v}
	}
	r.v = old

	// global lock for log write
	l, err := i.writeLog()
	if err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	newV := max(Version(time.Now().UnixNano()), old+1)
	at, size, err := logRecord{topic: t.name, id: id, v: newV, data: data}.write(l)
	if err != nil {
		if truncateErr := l.f.Truncate(int64(at)); truncateErr != nil {
			return 0, errors.Join(err, truncateErr)
		}
		return 0, err
	}
	if r.l != nil {
		r.l.stale.Add(int64(r.size)) // mark stale bytes in old log file
	}
	r.l = l
	r.at = at
	r.size = size
	r.v = newV

	return r.v, nil
}

func (i *idx) writeLog() (*logFile, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	l := i.curLog
	if l != nil && int(l.total.Load()) > MAX_LOG_SIZE {
		i.sealedLogs = append(i.sealedLogs, l)
		l = nil
	}
	if l == nil {
		var err error
		i.curLog, err = newLog(i.logFolder)
		if err != nil {
			return nil, err
		}
	}
	return i.curLog, nil
}

func (i *idx) Publish(topic, id string, old Version, data []byte) (Version, error) {
	t, _ := i.topics.LoadOrStore(topic, &topicIdx{name: topic})

	v, err := i.storeCAS(t, id, old, data)
	if err != nil {
		return 0, err
	}

	// no need to lock here
	t.subs.Range(func(k uint64, h Handler) bool {
		err := h(topic, id, v, data)
		if err != nil {
			t.subs.Delete(k)
		}
		return true
	})

	return v, nil
}

func (i *idx) Signal(topic, id string, data []byte) error {
	t, _ := i.topics.LoadOrStore(topic, &topicIdx{name: topic})

	// no need to lock here
	t.subs.Range(func(k uint64, h Handler) bool {
		err := h(topic, id, 0, data)
		if err != nil {
			t.subs.Delete(k)
		}
		return true
	})
	return nil
}

// Subscribe adds a handler for the given topic, returning a cleanup function and any error.
// NOTE: messages might be delivered out of order
// the client should ignore messages with older versions
func (i *idx) Subscribe(topic string, h Handler) (func(), error) {
	t, _ := i.topics.LoadOrStore(topic, &topicIdx{name: topic})
	id := rand.Uint64()

	t.subs.Store(id, h)

	// REPLAY
	var e error
	t.records.Range(func(id string, p *pointer) bool {
		// copy values in topic lock
		t.mu.Lock()
		l := p.l
		at := p.at
		t.mu.Unlock()

		// read from the log file for the full data
		r := logRecord{}
		_, err := r.readAt(l, at)
		if err != nil {
			e = err
			return false
		}

		// send to the client
		err = h(r.topic, r.id, r.v, r.data)
		if err != nil {
			e = err
			return false
		}
		return true
	})

	if e != nil {
		t.subs.Delete(id)
		return nil, e
	}

	// SUBSUB
	return func() {
		t.subs.Delete(id)
	}, nil
}

// Close stops the client and releases resources.
func (i *idx) Close() error {
	close(i.closed)

	i.mu.Lock()
	defer i.mu.Unlock()

	if i.curLog != nil {
		i.curLog.mu.Lock()
		i.curLog.f.Close()
		i.curLog.mu.Unlock()
	}

	for _, l := range i.sealedLogs {
		l.mu.Lock()
		l.f.Close()
		l.mu.Unlock()
	}

	return nil
}
