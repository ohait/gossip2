package enc

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"
)

const (
	MAX_DATA = 1 << 30 // 1 GB

	ID     = 1
	DATA   = 2
	UINT64 = 3
)

var LOG = log.Printf

type Version uint64

type Buffer struct {
	sync.Mutex
	io.ReadWriter
}

func (b *Buffer) ReadFull(data []byte) error {
	_, err := io.ReadFull(b, data)
	return err
}

func (b *Buffer) WriteFull(data []byte) error {
	for ct := 0; ct < len(data); {
		n, err := b.Write(data[ct:])
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		ct += n
	}
	return nil
}

func (b *Buffer) Seek(offset int64, whence int) (int64, error) {
	switch x := b.ReadWriter.(type) {
	case io.ReadSeeker:
		return x.Seek(offset, whence)
	default:
		return 0, fmt.Errorf("unsupported seek: %T", x)
	}
}

func (b *Buffer) WriteByte(c byte) error {
	_, err := b.Write([]byte{c})
	return err
}

func (b *Buffer) ReadByte() (byte, error) {
	var buf [1]byte
	err := b.ReadFull(buf[:])
	return buf[0], err
}

func (b *Buffer) WriteID(s string) (int, error) {
	if len(s) > 256 {
		return 0, fmt.Errorf("string too long: %d", len(s))
	}
	var buf [2]byte
	buf[0] = ID
	buf[1] = byte(len(s))
	err := b.WriteFull(buf[:])
	if err != nil {
		return 0, err
	}
	return len([]byte(s)) + 2, b.WriteFull([]byte(s))
}

func (b *Buffer) ReadID() (int, string, error) {
	var buf [2]byte
	err := b.ReadFull(buf[:])
	if err != nil {
		return 0, "", err
	}
	if buf[0] != ID {
		return 2, "", fmt.Errorf("expected ID, got: %d", buf[0])
	}
	s := make([]byte, buf[1])
	err = b.ReadFull(s)
	return 2 + int(buf[1]), string(s), err
}

func (b *Buffer) WriteData(data []byte) (int, error) {
	var buf [9]byte
	buf[0] = DATA
	binary.LittleEndian.PutUint64(buf[1:], uint64(len(data)))
	err := b.WriteFull(buf[:])
	if err != nil {
		return 0, err
	}
	return len(data) + 9, b.WriteFull(data)
}

func (b *Buffer) ReadData() (int, []byte, error) {
	var buf [9]byte
	err := b.ReadFull(buf[:])
	if err != nil {
		return 0, nil, err
	}
	if buf[0] != DATA {
		return 9, nil, fmt.Errorf("invalid DATA: %d", buf[0])
	}
	len := binary.LittleEndian.Uint64(buf[1:])
	if len > MAX_DATA {
		return 9, nil, fmt.Errorf("data too long: %d", len)
	}
	data := make([]byte, len)
	err = b.ReadFull(data)
	return 9 + int(len), data, err
}

func (b *Buffer) WriteUint64(n uint64) (int, error) {
	var buf [9]byte
	buf[0] = UINT64
	binary.LittleEndian.PutUint64(buf[1:], n)
	return 9, b.WriteFull(buf[:])
}

func (b *Buffer) ReadUint64() (int, uint64, error) {
	var buf [9]byte
	err := b.ReadFull(buf[:])
	if err != nil {
		return 0, 0, err
	}
	if buf[0] != UINT64 {
		return 9, 0, fmt.Errorf("expected UINT64 got type %d", buf[0])
	}
	return 9, binary.LittleEndian.Uint64(buf[1:]), nil
}
