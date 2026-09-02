package enc

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"

	xxhash "github.com/cespare/xxhash/v2"
)

const (
	v1PayloadRaw  = byte('=')
	v1PayloadZlib = byte('z')
	v1MaxID       = 1024
	v1MaxData     = 1 << 30
)

// readV1 loads a gossip1 GSP1 log after its four-byte header has been read.
func convertV1(f io.ReadSeeker, cb func(topic, id string, v Version, data []byte) error) error {
	for {
		topic, id, v, data, err := readV1Record(f)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := cb(topic, id, v, data); err != nil {
			return err
		}
	}
}

func readV1Record(f io.ReadSeeker) (topic, id string, v Version, data []byte, err error) {
	topic, err = readV1ID(f)
	if err != nil {
		return
	}
	id, err = readV1ID(f)
	if err != nil {
		return
	}
	var ts int64
	ts, err = readV1Int64(f)
	if err != nil {
		return
	}
	if ts < 0 {
		err = fmt.Errorf("negative gossip1 timestamp %d", ts)
		return
	}
	v = Version(ts)
	hash, err := readV1Uint64(f)
	if err != nil {
		return
	}
	size, err := readV1Uint64(f)
	if err != nil {
		return
	}
	if size > v1MaxData {
		err = fmt.Errorf("data length %d exceeds maximum allowed length %d", size, v1MaxData)
		return
	}
	encoded := make([]byte, size)
	if _, err = io.ReadFull(f, encoded); err != nil {
		return
	}
	if xxhash.Sum64(encoded) != hash {
		err = fmt.Errorf("data hash mismatch for %s", id)
		return
	}
	data, err = decodeV1Payload(encoded)
	return
}

func readV1ID(r io.Reader) (string, error) {
	var size uint16
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return "", err
	}
	if size > v1MaxID {
		return "", fmt.Errorf("id length %d exceeds maximum allowed length %d", size, v1MaxID)
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func readV1Int64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.BigEndian, &n)
	return n, err
}

func readV1Uint64(r io.Reader) (uint64, error) {
	var n uint64
	err := binary.Read(r, binary.BigEndian, &n)
	return n, err
}

func decodeV1Payload(encoded []byte) ([]byte, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("missing payload encoding")
	}
	switch encoded[0] {
	case v1PayloadRaw:
		return append([]byte(nil), encoded[1:]...), nil
	case v1PayloadZlib:
		zr, err := zlib.NewReader(bytes.NewReader(encoded[1:]))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return io.ReadAll(zr)
	default:
		return nil, fmt.Errorf("unknown gossip1 payload encoding %q", encoded[0])
	}
}
