package gossip

import (
	"errors"
	"fmt"
	"io"
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
	v     Version
	data  []byte
}

// write appends the record after any replay reads that may have moved the
// shared file cursor.
func (r logRecord) write(l *logFile) (start int, size int, err error) {
	s, err := l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to seek to end: %w", err)
	}
	start = int(s)
	size, err = r.writeBuf(&l.b)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write buffer: %w", err)
	}
	enc.LOG("wrote record topic=%q id=%s v=%d at=%d size=%d", r.topic, r.id, r.v, start, size)
	return start, size, l.f.Sync()
}

func (r logRecord) writeBuf(b *enc.Buffer) (int, error) {
	var size int
	ct, err := b.WriteID(r.topic)
	if err != nil {
		return 0, fmt.Errorf("failed to write topic ID: %w", err)
	}
	size += ct

	ct, err = b.WriteID(r.id)
	if err != nil {
		return 0, fmt.Errorf("failed to write record ID: %w", err)
	}
	size += ct

	ct, err = b.WriteUint64(uint64(r.v))
	if err != nil {
		return 0, fmt.Errorf("failed to write version: %w", err)
	}
	size += ct

	ct, err = b.WriteData(r.data)
	if err != nil {
		return 0, fmt.Errorf("failed to write data: %w", err)
	}
	size += ct

	return size, nil
}

func (r *logRecord) readAt(l *logFile, at int) (size int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err = l.f.Seek(int64(at), io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek to position %d: %w", at, err)
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
	r.v = Version(v)
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
		return nil, fmt.Errorf("failed to create log file %q: %w", path, err)
	}
	l := &logFile{
		f: f,
		b: enc.Buffer{ReadWriter: f},
	}
	_, err = f.Write([]byte("GSP2"))
	if err != nil {
		return nil, fmt.Errorf("failed to write log header: %w", err)
	}
	l.total.Store(4)
	enc.LOG("created log: %q", path)
	return l, nil
}

func (i *idx) rangeLogs(cb func(path string) error) error {
	files, err := os.ReadDir(i.logFolder)
	if err != nil {
		return fmt.Errorf("failed to read log directory %q: %w", i.logFolder, err)
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		path := fmt.Sprintf("%s/%s", i.logFolder, f.Name())
		err := cb(path)
		if err != nil {
			return fmt.Errorf("failed to process log file %q: %w", path, err)
		}
	}
	return nil
}

func (l *logFile) tell() (int, error) {
	at, err := l.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("failed to get file position: %w", err)
	}
	return int(at), nil
}

// truncate removes a failed partial append and restores the write cursor to
// the beginning of that record. Truncate alone leaves the cursor unchanged,
// which would make the next append create a malformed gap.
func (l *logFile) truncate(at int) error {
	if err := l.f.Truncate(int64(at)); err != nil {
		return fmt.Errorf("failed to truncate file to %d: %w", at, err)
	}
	if _, err := l.f.Seek(int64(at), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek after truncate: %w", err)
	}
	l.total.Store(int64(at))
	return nil
}

func (l *logFile) rangeRecords(cb func(at, size int, lrec logRecord) error) error {
	for {
		var lrec logRecord
		at, err := l.tell()
		if err != nil {
			return fmt.Errorf("failed to get file position: %w", err)
		}
		size, err := lrec.read(l)
		if err != nil {
			if err == io.EOF {
				l.total.Store(int64(at))
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// file is truncated, we recover as much as we can
				enc.LOG("WARN: truncated log file at %d, error: %v", at, err)
				l.total.Store(int64(at))
				return nil
			}
			return fmt.Errorf("failed to read record: %w", err)
		}
		err = cb(at, size, lrec)
		if err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
	}
}

func (i *idx) openLog(path string) (*logFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %q: %w", path, err)
	}

	l := &logFile{
		f: f,
		b: enc.Buffer{ReadWriter: f},
	}

	// seek to end to get total size
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to end of file: %w", err)
	}
	l.total.Store(int64(end))

	// wind back to start
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start of file: %w", err)
	}

	// check header
	var head [4]byte
	_, err = io.ReadFull(f, head[:])
	if err != nil {
		return nil, fmt.Errorf("failed to read file header: %w", err)
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
	if err != nil {
		return nil, fmt.Errorf("failed to open log: %w", err)
	}
	return l, nil
}

// run the compaction check
func (i *idx) compact() error {
	i.mu.Lock()
	if len(i.sealedLogs) < 2 {
		i.mu.Unlock()
		return nil
	}

	var todo []*logFile
	for _, l := range i.sealedLogs {
		stale := int(l.stale.Load())
		total := int(l.total.Load())
		actual := total - stale
		if actual < MAX_LOG_SIZE/2 {
			enc.LOG("mark for compact: %q (%.2f/%.2f MB)", l.f.Name(),
				float64(actual)/1024/1024, float64(total)/1024/1024)
			todo = append(todo, l)
		}
	}
	i.mu.Unlock()
	if len(todo) == 0 {
		return nil
	}

	enc.LOG("compacting %d logs", len(todo))
	active := 0
	skip := 0
	tot := 0
	for _, rlog := range todo {
		rlog.mu.Lock()
		rlog.f.Seek(4, io.SeekStart)
		err := rlog.rangeRecords(func(_, _ int, r logRecord) error {
			t, _ := i.topics.LoadOrStore(r.topic, &topicIdx{name: r.topic})
			p, _ := t.records.Load(r.id)
			if p != nil && p.v == r.v {
				active++
			} else {
				skip++
				return nil // skip
			}

			// append to the current write log
			wlog, err := i.writeLog()
			if err != nil {
				return fmt.Errorf("failed to write during compaction: %w", err)
			}
			wlog.mu.Lock()
			at, size, err := r.write(wlog)
			wlog.mu.Unlock()
			if err != nil {
				return fmt.Errorf("failed to write record during compaction: %w", err)
			}

			// update index
			t.mu.Lock()
			defer t.mu.Unlock()

			// check again (in case it was changed outside the lock)
			p, _ = t.records.Load(r.id)
			if p != nil && p.v == r.v {
				tot += size
				p.l = wlog
				p.at = at
				p.size = size
			}
			return nil
		})
		rlog.mu.Unlock()
		if err != nil {
			return fmt.Errorf("compaction failed: %w", err)
		}
	}
	enc.LOG("compacted %d records, %.2fMB, skipped %d stale records",
		active, float64(tot)/1024/1024, skip)
	i.mu.Lock()
	i.sealedLogs = slices.DeleteFunc(i.sealedLogs, func(log *logFile) bool {
		if !slices.Contains(todo, log) {
			return false
		}
		log.mu.Lock()
		defer log.mu.Unlock()
		log.f.Close()
		os.Remove(log.f.Name())
		enc.LOG("removed log: %q", log.f.Name())
		return true
	})
	i.mu.Unlock()
	return nil
}
