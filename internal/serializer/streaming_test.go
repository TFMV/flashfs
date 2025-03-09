package serializer

import (
	"bytes"
	"io/fs"
	"testing"
	"time"

	"github.com/TFMV/flashfs/internal/walker"
)

func TestStreamingSerializer(t *testing.T) {
	// Create test data
	now := time.Now()
	entries := make([]walker.SnapshotEntry, 2500) // More than DefaultChunkSize
	for i := 0; i < len(entries); i++ {
		entries[i] = walker.SnapshotEntry{
			Path:        "/test/file" + string(rune(i)),
			Size:        int64(i * 1024),
			ModTime:     now.Add(time.Duration(i) * time.Minute),
			IsDir:       i%100 == 0,
			Permissions: fs.FileMode(0644),
			Hash:        "abcdef1234567890",
		}
	}

	// Create a streaming serializer with custom options
	options := StreamingOptions{
		ChunkSize:  500, // Smaller than default for testing
		BufferSize: 32 * 1024,
	}
	serializer := NewStreamingSerializer(options)

	// Create a buffer to write to
	var buf bytes.Buffer

	// Serialize the entries
	err := serializer.SerializeToWriter(entries, &buf)
	if err != nil {
		t.Fatalf("Failed to serialize entries: %v", err)
	}

	// Deserialize the entries
	var deserializedEntries []walker.SnapshotEntry
	var chunkCount int

	err = DeserializeFromReader(&buf, func(chunk SnapshotChunk) error {
		chunkCount++
		deserializedEntries = append(deserializedEntries, chunk.Entries...)

		// Verify chunk metadata
		if chunk.Index != chunkCount-1 {
			t.Errorf("Expected chunk index %d, got %d", chunkCount-1, chunk.Index)
		}

		expectedTotal := (len(entries) + options.ChunkSize - 1) / options.ChunkSize
		if chunk.Total != expectedTotal {
			t.Errorf("Expected total chunks %d, got %d", expectedTotal, chunk.Total)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to deserialize entries: %v", err)
	}

	// Verify the number of chunks
	expectedChunks := (len(entries) + options.ChunkSize - 1) / options.ChunkSize
	if chunkCount != expectedChunks {
		t.Errorf("Expected %d chunks, got %d", expectedChunks, chunkCount)
	}

	// Verify the deserialized entries
	if len(deserializedEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(deserializedEntries))
	}

	// Verify a sample of entries
	for i := 0; i < len(entries); i += 100 {
		if deserializedEntries[i].Path != entries[i].Path {
			t.Errorf("Entry %d: Expected path %s, got %s", i, entries[i].Path, deserializedEntries[i].Path)
		}
		if deserializedEntries[i].Size != entries[i].Size {
			t.Errorf("Entry %d: Expected size %d, got %d", i, entries[i].Size, deserializedEntries[i].Size)
		}
	}
}

func TestStreamingDiffSerializer(t *testing.T) {
	// Create test data
	now := time.Now()
	entries := make([]DiffEntry, 2500) // More than DefaultChunkSize
	for i := 0; i < len(entries); i++ {
		diffType := DiffAdded
		if i%3 == 1 {
			diffType = DiffModified
		} else if i%3 == 2 {
			diffType = DiffDeleted
		}

		entries[i] = DiffEntry{
			Path:           "/test/file" + string(rune(i)),
			Type:           diffType,
			OldSize:        int64(i * 512),
			NewSize:        int64(i * 1024),
			OldModTime:     now.Add(-time.Duration(i) * time.Minute),
			NewModTime:     now.Add(time.Duration(i) * time.Minute),
			OldPermissions: 0644,
			NewPermissions: 0755,
			OldHash:        "abcdef1234567890", // Valid hex string
			NewHash:        "0123456789abcdef", // Valid hex string
		}
	}

	// Create a streaming serializer with custom options
	options := StreamingOptions{
		ChunkSize:  500, // Smaller than default for testing
		BufferSize: 32 * 1024,
	}
	serializer := NewStreamingDiffSerializer(options)

	// Create a buffer to write to
	var buf bytes.Buffer

	// Serialize the entries
	err := serializer.SerializeDiffToWriter(entries, &buf)
	if err != nil {
		t.Fatalf("Failed to serialize diff entries: %v", err)
	}

	// Deserialize the entries
	var deserializedEntries []DiffEntry
	var chunkCount int

	err = DeserializeDiffFromReader(&buf, func(chunk DiffChunk) error {
		chunkCount++
		deserializedEntries = append(deserializedEntries, chunk.Entries...)

		// Verify chunk metadata
		if chunk.Index != chunkCount-1 {
			t.Errorf("Expected chunk index %d, got %d", chunkCount-1, chunk.Index)
		}

		expectedTotal := (len(entries) + options.ChunkSize - 1) / options.ChunkSize
		if chunk.Total != expectedTotal {
			t.Errorf("Expected total chunks %d, got %d", expectedTotal, chunk.Total)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to deserialize diff entries: %v", err)
	}

	// Verify the number of chunks
	expectedChunks := (len(entries) + options.ChunkSize - 1) / options.ChunkSize
	if chunkCount != expectedChunks {
		t.Errorf("Expected %d chunks, got %d", expectedChunks, chunkCount)
	}

	// Verify the deserialized entries
	if len(deserializedEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(deserializedEntries))
	}

	// Verify a sample of entries
	for i := 0; i < len(entries); i += 100 {
		if deserializedEntries[i].Path != entries[i].Path {
			t.Errorf("Entry %d: Expected path %s, got %s", i, entries[i].Path, deserializedEntries[i].Path)
		}
		if deserializedEntries[i].Type != entries[i].Type {
			t.Errorf("Entry %d: Expected type %d, got %d", i, entries[i].Type, deserializedEntries[i].Type)
		}
		if deserializedEntries[i].OldSize != entries[i].OldSize {
			t.Errorf("Entry %d: Expected old size %d, got %d", i, entries[i].OldSize, deserializedEntries[i].OldSize)
		}
		if deserializedEntries[i].NewSize != entries[i].NewSize {
			t.Errorf("Entry %d: Expected new size %d, got %d", i, entries[i].NewSize, deserializedEntries[i].NewSize)
		}
	}
}

func BenchmarkStreamingSerializer(b *testing.B) {
	// Create test data
	now := time.Now()
	entries := make([]walker.SnapshotEntry, 10000)
	for i := 0; i < len(entries); i++ {
		entries[i] = walker.SnapshotEntry{
			Path:        "/test/file" + string(rune(i)),
			Size:        int64(i * 1024),
			ModTime:     now.Add(time.Duration(i) * time.Minute),
			IsDir:       i%100 == 0,
			Permissions: fs.FileMode(0644),
			Hash:        "abcdef1234567890",
		}
	}

	// Create a streaming serializer
	serializer := NewStreamingSerializer(DefaultStreamingOptions())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		err := serializer.SerializeToWriter(entries, &buf)
		if err != nil {
			b.Fatalf("Failed to serialize entries: %v", err)
		}
	}
}

func BenchmarkStreamingDiffSerializer(b *testing.B) {
	// Create test data
	now := time.Now()
	entries := make([]DiffEntry, 10000)
	for i := 0; i < len(entries); i++ {
		diffType := DiffAdded
		if i%3 == 1 {
			diffType = DiffModified
		} else if i%3 == 2 {
			diffType = DiffDeleted
		}

		entries[i] = DiffEntry{
			Path:           "/test/file" + string(rune(i)),
			Type:           diffType,
			OldSize:        int64(i * 512),
			NewSize:        int64(i * 1024),
			OldModTime:     now.Add(-time.Duration(i) * time.Minute),
			NewModTime:     now.Add(time.Duration(i) * time.Minute),
			OldPermissions: 0644,
			NewPermissions: 0755,
			OldHash:        "abcdef1234567890",
			NewHash:        "0123456789abcdef",
		}
	}

	// Create a streaming serializer
	serializer := NewStreamingDiffSerializer(DefaultStreamingOptions())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		err := serializer.SerializeDiffToWriter(entries, &buf)
		if err != nil {
			b.Fatalf("Failed to serialize diff entries: %v", err)
		}
	}
}
