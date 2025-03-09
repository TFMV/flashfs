package serializer

import (
	"io/fs"
	"testing"
	"time"

	"github.com/TFMV/flashfs/internal/walker"
	flatbuffers "github.com/google/flatbuffers/go"
)

func TestSerializeDeserializeSnapshotFB(t *testing.T) {
	// Create test data
	now := time.Now()
	entries := []walker.SnapshotEntry{
		{
			Path:        "/test/file1.txt",
			Size:        1024,
			ModTime:     now,
			IsDir:       false,
			Permissions: 0644,
			Hash:        "abcdef1234567890",
		},
		{
			Path:        "/test/dir1",
			Size:        0,
			ModTime:     now.Add(-time.Hour),
			IsDir:       true,
			Permissions: 0755,
			Hash:        "",
		},
		{
			Path:        "/test/file2.txt",
			Size:        2048,
			ModTime:     now.Add(-2 * time.Hour),
			IsDir:       false,
			Permissions: 0644,
			Hash:        "0987654321fedcba",
		},
	}

	// Serialize the snapshot
	data, err := SerializeSnapshotFB(entries, nil)
	if err != nil {
		t.Fatalf("Failed to serialize snapshot: %v", err)
	}

	// Deserialize the snapshot
	deserializedEntries, err := DeserializeSnapshotFB(data)
	if err != nil {
		t.Fatalf("Failed to deserialize snapshot: %v", err)
	}

	// Verify the deserialized data
	if len(deserializedEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(deserializedEntries))
	}

	for i, entry := range entries {
		deserializedEntry := deserializedEntries[i]

		if deserializedEntry.Path != entry.Path {
			t.Errorf("Entry %d: Expected path %s, got %s", i, entry.Path, deserializedEntry.Path)
		}

		if deserializedEntry.Size != entry.Size {
			t.Errorf("Entry %d: Expected size %d, got %d", i, entry.Size, deserializedEntry.Size)
		}

		// Compare Unix timestamps to avoid timezone issues
		if deserializedEntry.ModTime.Unix() != entry.ModTime.Unix() {
			t.Errorf("Entry %d: Expected mod time %d, got %d", i, entry.ModTime.Unix(), deserializedEntry.ModTime.Unix())
		}

		if deserializedEntry.IsDir != entry.IsDir {
			t.Errorf("Entry %d: Expected isDir %t, got %t", i, entry.IsDir, deserializedEntry.IsDir)
		}

		if deserializedEntry.Permissions != entry.Permissions {
			t.Errorf("Entry %d: Expected permissions %o, got %o", i, entry.Permissions, deserializedEntry.Permissions)
		}

		if deserializedEntry.Hash != entry.Hash {
			t.Errorf("Entry %d: Expected hash %s, got %s", i, entry.Hash, deserializedEntry.Hash)
		}
	}
}

func TestSerializeDeserializeDiffFB(t *testing.T) {
	// Create test data
	now := time.Now()
	entries := []DiffEntry{
		{
			Path:           "/test/file1.txt",
			Type:           DiffModified,
			OldSize:        1024,
			NewSize:        2048,
			OldModTime:     now.Add(-time.Hour),
			NewModTime:     now,
			OldPermissions: 0644,
			NewPermissions: 0644,
			OldHash:        "abcdef1234567890",
			NewHash:        "0987654321fedcba",
		},
		{
			Path:           "/test/file2.txt",
			Type:           DiffAdded,
			OldSize:        0,
			NewSize:        1024,
			OldModTime:     time.Time{},
			NewModTime:     now,
			OldPermissions: 0,
			NewPermissions: 0644,
			OldHash:        "",
			NewHash:        "abcdef1234567890",
		},
		{
			Path:           "/test/file3.txt",
			Type:           DiffDeleted,
			OldSize:        1024,
			NewSize:        0,
			OldModTime:     now.Add(-2 * time.Hour),
			NewModTime:     time.Time{},
			OldPermissions: 0644,
			NewPermissions: 0,
			OldHash:        "0987654321fedcba",
			NewHash:        "",
		},
	}

	// Serialize the diff
	data, err := SerializeDiffFB(entries, nil)
	if err != nil {
		t.Fatalf("Failed to serialize diff: %v", err)
	}

	// Deserialize the diff
	deserializedEntries, err := DeserializeDiffFB(data)
	if err != nil {
		t.Fatalf("Failed to deserialize diff: %v", err)
	}

	// Verify the deserialized data
	if len(deserializedEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(deserializedEntries))
	}

	for i, entry := range entries {
		deserializedEntry := deserializedEntries[i]

		if deserializedEntry.Path != entry.Path {
			t.Errorf("Entry %d: Expected path %s, got %s", i, entry.Path, deserializedEntry.Path)
		}

		if deserializedEntry.Type != entry.Type {
			t.Errorf("Entry %d: Expected type %d, got %d", i, entry.Type, deserializedEntry.Type)
		}

		if deserializedEntry.OldSize != entry.OldSize {
			t.Errorf("Entry %d: Expected old size %d, got %d", i, entry.OldSize, deserializedEntry.OldSize)
		}

		if deserializedEntry.NewSize != entry.NewSize {
			t.Errorf("Entry %d: Expected new size %d, got %d", i, entry.NewSize, deserializedEntry.NewSize)
		}

		// Compare Unix timestamps to avoid timezone issues
		if entry.OldModTime.IsZero() {
			if !deserializedEntry.OldModTime.IsZero() {
				t.Errorf("Entry %d: Expected zero old mod time, got %v", i, deserializedEntry.OldModTime)
			}
		} else if deserializedEntry.OldModTime.Unix() != entry.OldModTime.Unix() {
			t.Errorf("Entry %d: Expected old mod time %d, got %d", i, entry.OldModTime.Unix(), deserializedEntry.OldModTime.Unix())
		}

		if entry.NewModTime.IsZero() {
			if !deserializedEntry.NewModTime.IsZero() {
				t.Errorf("Entry %d: Expected zero new mod time, got %v", i, deserializedEntry.NewModTime)
			}
		} else if deserializedEntry.NewModTime.Unix() != entry.NewModTime.Unix() {
			t.Errorf("Entry %d: Expected new mod time %d, got %d", i, entry.NewModTime.Unix(), deserializedEntry.NewModTime.Unix())
		}

		if deserializedEntry.OldPermissions != entry.OldPermissions {
			t.Errorf("Entry %d: Expected old permissions %o, got %o", i, entry.OldPermissions, deserializedEntry.OldPermissions)
		}

		if deserializedEntry.NewPermissions != entry.NewPermissions {
			t.Errorf("Entry %d: Expected new permissions %o, got %o", i, entry.NewPermissions, deserializedEntry.NewPermissions)
		}

		if deserializedEntry.OldHash != entry.OldHash {
			t.Errorf("Entry %d: Expected old hash %s, got %s", i, entry.OldHash, deserializedEntry.OldHash)
		}

		if deserializedEntry.NewHash != entry.NewHash {
			t.Errorf("Entry %d: Expected new hash %s, got %s", i, entry.NewHash, deserializedEntry.NewHash)
		}
	}
}

func TestBuilderReuse(t *testing.T) {
	// Create test data
	now := time.Now()
	entries := []walker.SnapshotEntry{
		{
			Path:        "/test/file1.txt",
			Size:        1024,
			ModTime:     now,
			IsDir:       false,
			Permissions: 0644,
			Hash:        "abcdef1234567890",
		},
	}

	// Create a builder to reuse
	builder := flatbuffers.NewBuilder(0)

	// Serialize multiple snapshots with the same builder
	for i := 0; i < 5; i++ {
		data, err := SerializeSnapshotFB(entries, builder)
		if err != nil {
			t.Fatalf("Failed to serialize snapshot with reused builder: %v", err)
		}

		// Verify the data
		deserializedEntries, err := DeserializeSnapshotFB(data)
		if err != nil {
			t.Fatalf("Failed to deserialize snapshot: %v", err)
		}

		if len(deserializedEntries) != len(entries) {
			t.Fatalf("Expected %d entries, got %d", len(entries), len(deserializedEntries))
		}
	}
}

func BenchmarkSerializeSnapshotFB(b *testing.B) {
	// Create test data
	now := time.Now()
	entries := make([]walker.SnapshotEntry, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = walker.SnapshotEntry{
			Path:        "/test/file" + string(rune(i)),
			Size:        int64(i * 1024),
			ModTime:     now.Add(time.Duration(i) * time.Minute),
			IsDir:       i%10 == 0,
			Permissions: fs.FileMode(0644),
			Hash:        "abcdef1234567890",
		}
	}

	b.Run("WithoutBuilderReuse", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := SerializeSnapshotFB(entries, nil)
			if err != nil {
				b.Fatalf("Failed to serialize snapshot: %v", err)
			}
		}
	})

	b.Run("WithBuilderReuse", func(b *testing.B) {
		builder := flatbuffers.NewBuilder(0)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := SerializeSnapshotFB(entries, builder)
			if err != nil {
				b.Fatalf("Failed to serialize snapshot: %v", err)
			}
		}
	})
}

func BenchmarkDeserializeSnapshotFB(b *testing.B) {
	// Create test data
	now := time.Now()
	entries := make([]walker.SnapshotEntry, 1000)
	for i := 0; i < 1000; i++ {
		entries[i] = walker.SnapshotEntry{
			Path:        "/test/file" + string(rune(i)),
			Size:        int64(i * 1024),
			ModTime:     now.Add(time.Duration(i) * time.Minute),
			IsDir:       i%10 == 0,
			Permissions: fs.FileMode(0644),
			Hash:        "abcdef1234567890",
		}
	}

	// Serialize the snapshot
	data, err := SerializeSnapshotFB(entries, nil)
	if err != nil {
		b.Fatalf("Failed to serialize snapshot: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DeserializeSnapshotFB(data)
		if err != nil {
			b.Fatalf("Failed to deserialize snapshot: %v", err)
		}
	}
}
