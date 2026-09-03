package enc

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
)

func Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	zlibWriter := zlib.NewWriter(&buf)
	_, err := zlibWriter.Write(data)
	if err != nil {
		return nil, err
	}
	if err := zlibWriter.Close(); err != nil {
		return nil, err
	}
	compressed := buf.Bytes()
	if len(compressed) >= len(data) {
		return append([]byte{'='}, data...), nil
	}
	return append([]byte{'Z'}, compressed...), nil
}

func Decompress(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	switch raw[0] {
	case '=':
		return raw[1:], nil
	case 'Z', 'z':
		zlibReader, err := zlib.NewReader(bytes.NewReader(raw[1:]))
		if err != nil {
			return nil, err
		}
		defer zlibReader.Close()
		data, err := io.ReadAll(zlibReader)
		if err != nil {
			return nil, err
		}
		return data, nil
	default:
		return nil, fmt.Errorf("invalid compression: %q", raw[0])
	}
}
