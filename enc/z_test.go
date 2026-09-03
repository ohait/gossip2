package enc

import (
	"bytes"
	"testing"
)

func TestCompressDecompress(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "simple string",
			data: []byte("Hello, World! This is a test string for compression."),
		},
		{
			name: "highly compressible data",
			data: bytes.Repeat([]byte("A"), 1000),
		},
		{
			name: "random data",
			data: []byte{0x00, 0xFF, 0xAB, 0xCD, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF},
		},
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "single byte",
			data: []byte("X"),
		},
		{
			name: "binary data",
			data: make([]byte, 256),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test compression
			compressed, err := Compress(tt.data)
			if err != nil {
				t.Fatalf("Compress() error = %v", err)
			}

			// Test decompression
			decompressed, err := Decompress(compressed)
			if err != nil {
				t.Fatalf("Decompress() error = %v", err)
			}

			// Verify data matches
			if !bytes.Equal(decompressed, tt.data) {
				t.Errorf("Decompress(Compress(data)) = %v, want %v", decompressed, tt.data)
			}

			// Verify compression marker
			if len(tt.data) > 0 && len(compressed) > 0 {
				if len(compressed) < len(tt.data) {
					// Should be compressed with 'Z' marker
					if compressed[0] != 'Z' {
						t.Errorf("Expected compression marker 'Z', got %q", compressed[0])
					}
				} else {
					// Should be uncompressed with '=' marker
					if compressed[0] != '=' {
						t.Errorf("Expected uncompressed marker '=', got %q", compressed[0])
					}
				}
			}
		})
	}
}

func TestCompressEmptyData(t *testing.T) {
	compressed, err := Compress([]byte{})
	if err != nil {
		t.Fatalf("Compress() error = %v", err)
	}
	if compressed != nil {
		t.Errorf("Compress([]byte{}) = %v, want nil", compressed)
	}
}

func TestDecompressEmptyData(t *testing.T) {
	decompressed, err := Decompress([]byte{})
	if err != nil {
		t.Fatalf("Decompress() error = %v", err)
	}
	if decompressed != nil {
		t.Errorf("Decompress([]byte{}) = %v, want nil", decompressed)
	}
}

func TestDecompressInvalidMarker(t *testing.T) {
	data := []byte{'X', 't', 'e', 's', 't'}
	_, err := Decompress(data)
	if err == nil {
		t.Error("Decompress() expected error for invalid marker, got nil")
	}
}

func TestDecompressUncompressedData(t *testing.T) {
	original := []byte("test data")
	// Manually create uncompressed data with '=' marker
	compressed := append([]byte{'='}, original...)

	decompressed, err := Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress() error = %v", err)
	}

	if !bytes.Equal(decompressed, original) {
		t.Errorf("Decompress() = %v, want %v", decompressed, original)
	}
}

func TestCompressDecompressRoundTrip(t *testing.T) {
	// Test with various sizes to ensure compression works correctly
	for i := range 100 {
		data := bytes.Repeat([]byte("abcdefghij"), i)
		compressed, err := Compress(data)
		if err != nil {
			t.Fatalf("Compress() error at iteration %d: %v", i, err)
		}

		decompressed, err := Decompress(compressed)
		if err != nil {
			t.Fatalf("Decompress() error at iteration %d: %v", i, err)
		}

		if !bytes.Equal(decompressed, data) {
			t.Errorf("Round trip failed at iteration %d: got %v, want %v", i, decompressed, data)
		}
	}
}
