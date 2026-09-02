package gossip

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ohait/gossip2/enc"
	"github.com/ohait/gossip2/sync"
)

type logFile struct {
	mu    sync.Mutex // used for read/writes
	f     *os.File
	b     enc.Buffer
	total atomic.Int64 // total number of bytes in file
	stale atomic.Int64 // how many bytes are stale (record is no longer in the index)
}

type logRecord struct {
	// layout on the file
	topic string
	id    string
	v     enc.Version
	data  []byte
}

// write writes the record to the log file at the current position
func (r logRecord) write(l *logFile) (start int, size int, err error) {
	s, err := l.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, err
	}
	start = int(s)

	var ct int

	ct, err = l.b.WriteID(r.topic)
	if err != nil {
		return 0, 0, err
	}
	size += ct

	ct, err = l.b.WriteID(r.id)
	if err != nil {
		return 0, 0, err
	}
	size += ct

	ct, err = l.b.WriteUint64(uint64(r.v))
	if err != nil {
		return 0, 0, err
	}
	size += ct

	ct, err = l.b.WriteData(r.data)
	if err != nil {
		return 0, 0, err
	}
	size += ct

	return start, size, l.f.Sync()
}

func (r *logRecord) readAt(l *logFile, at int) (size int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err = l.f.Seek(int64(at), io.SeekStart); err != nil {
		return 0, err
	}
	return r.read(l)
}

// reads assume the file pointer is at the right position
func (r *logRecord) read(l *logFile) (size int, err error) {
	var ct int
	ct, r.topic, err = l.b.ReadID()
	size += ct
	if err != nil {
		return
	}

	ct, r.id, err = l.b.ReadID()
	size += ct
	if err != nil {
		return
	}

	var v uint64
	ct, v, err = l.b.ReadUint64()
	size += ct
	r.v = enc.Version(v)
	if err != nil {
		return
	}

	ct, r.data, err = l.b.ReadData()
	size += ct
	return
}

func newLog(folder string) (*logFile, error) {
	path := fmt.Sprintf("%s/%s.v2", folder, time.Now().Format("20060102T150405"))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	l := &logFile{
		f: f,
		b: enc.Buffer{ReadWriter: f},
	}
	_, err = f.Write([]byte("GSP2"))
	l.total.Store(4)
	return l, err
}

func (i *idx) rangeLogs(cb func(path string) error) error {
	files, err := os.ReadDir(i.logFolder)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		path := fmt.Sprintf("%s/%s", i.logFolder, f.Name())
		err := cb(path)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *logFile) tell() (int, error) {
	at, err := l.f.Seek(0, io.SeekCurrent)
	return int(at), err
}

func (l *logFile) rangeRecords(cb func(at, size int, lrec logRecord) error) error {
	for {
		var lrec logRecord
		at, err := l.tell()
		if err != nil {
			return err
		}
		size, err := lrec.read(l)
		if err != nil {
			if err == io.EOF {
				l.total.Store(int64(at))
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// file is truncated, we recover as much as we can
				log.Printf("WARN: truncated log file at %d, error: %v", at, err)
				l.total.Store(int64(at))
				return nil
			}
			return err
		}
		err = cb(at, size, lrec)
		if err != nil {
			return err
		}
	}
}

func (i *idx) openLog(path string) (*logFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	l := &logFile{
		f: f,
		b: enc.Buffer{ReadWriter: f},
	}

	// seek to end to get total size
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	l.total.Store(int64(end))

	// wind back to start
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// check header
	var head [4]byte
	_, err = io.ReadFull(f, head[:])
	if err != nil {
		return nil, err
	}
	if string(head[:]) != "GSP2" {
		return nil, fmt.Errorf("invalid log file header")
	}

	err = l.rangeRecords(func(at, size int, lrec logRecord) error {
		t, _ := i.topics.LoadOrStore(lrec.topic, &topicIdx{name: lrec.topic})
		irec, _ := t.records.LoadOrStore(lrec.id, &pointer{})
		if irec.l != nil {
			if irec.v < lrec.v {
				// we are newer, update and mark previous as stale
				irec.l.stale.Add(int64(irec.size))
				irec.l = l
				irec.at = at
				irec.size = size
				irec.v = lrec.v
			} else {
				// we are stale, skip updating
				l.stale.Add(int64(size))
			}
		} else {
			// we are first
			irec.l = l
			irec.at = at
			irec.size = size
			irec.v = lrec.v
		}
		return nil
	})
	return l, err
}

// run the compaction check
func (i *idx) compact() error {
	var todo []*logFile
	i.mu.Lock()
	for _, l := range i.sealedLogs {
		stale := int(l.stale.Load())
		actual := int(l.total.Load()) - stale
		if stale > actual*4 {
			log.Printf("mark for compact: %q", l.f.Name())
			todo = append(todo, l)
		}
	}
	i.mu.Unlock()
	if len(todo) == 0 {
		return nil
	}

	log.Printf("compacting %d logs", len(todo))
	for _, rlog := range todo {
		rlog.mu.Lock()
		rlog.f.Seek(4, io.SeekStart)
		err := rlog.rangeRecords(func(_, _ int, r logRecord) error {
			// append to the current write log
			wlog, err := i.writeLog()
			wlog.mu.Lock()
			if err != nil {
				wlog.mu.Unlock()
				return err
			}
			at, size, err := r.write(wlog)
			if err != nil {
				wlog.mu.Unlock()
				return err
			}
			wlog.mu.Unlock()

			// update index
			t, _ := i.topics.LoadOrStore(r.topic, &topicIdx{name: r.topic})
			t.mu.Lock()
			defer t.mu.Unlock()

			p, _ := t.records.Load(r.id)
			if p != nil && p.v == r.v {
				p.l = wlog
				p.at = at
				p.size = size
			}

			return nil
		})
		rlog.mu.Unlock()
		if err != nil {
			return err
		}
	}
	i.mu.Lock()
	i.sealedLogs = slices.DeleteFunc(i.sealedLogs, func(log *logFile) bool {
		if !slices.Contains(todo, log) {
			return false
		}
		log.mu.Lock()
		defer log.mu.Unlock()
		log.f.Close()
		os.Remove(log.f.Name())
		return true
	})
	i.mu.Unlock()
	return nil
}
