package serializer

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/TFMV/flashfs/internal/walker"
	"github.com/TFMV/flashfs/schema/flashfs"
)

func TestSerializeSnapshot(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		entries []walker.SnapshotEntry
	}{
		{
			name:    "Empty",
			entries: []walker.SnapshotEntry{},
		},
		{
			name: "SingleFile",
			entries: []walker.SnapshotEntry{
				{
					Path:        "/test/file.txt",
					Size:        1024,
					ModTime:     time.Unix(1609459200, 0), // 2021-01-01 00:00:00
					IsDir:       false,
					Permissions: 0644,
					Hash:        "01020304",
				},
			},
		},
		{
			name: "MultipleFiles",
			entries: []walker.SnapshotEntry{
				{
					Path:        "/test/file1.txt",
					Size:        1024,
					ModTime:     time.Unix(1609459200, 0), // 2021-01-01 00:00:00
					IsDir:       false,
					Permissions: 0644,
					Hash:        "01020304",
				},
				{
					Path:        "/test/file2.txt",
					Size:        2048,
					ModTime:     time.Unix(1609545600, 0), // 2021-01-02 00:00:00
					IsDir:       false,
					Permissions: 0644,
					Hash:        "05060708",
				},
				{
					Path:        "/test/dir1",
					Size:        0,
					ModTime:     time.Unix(1609632000, 0), // 2021-01-03 00:00:00
					IsDir:       true,
					Permissions: 0755,
					Hash:        "",
				},
			},
		},
		{
			name: "DirectoriesAndFiles",
			entries: []walker.SnapshotEntry{
				{
					Path:        "/test",
					Size:        0,
					ModTime:     time.Unix(1609459200, 0), // 2021-01-01 00:00:00
					IsDir:       true,
					Permissions: 0755,
					Hash:        "",
				},
				{
					Path:        "/test/dir",
					Size:        0,
					ModTime:     time.Unix(1609459200, 0), // 2021-01-01 00:00:00
					IsDir:       true,
					Permissions: 0755,
					Hash:        "",
				},
				{
					Path:        "/test/file.txt",
					Size:        1024,
					ModTime:     time.Unix(1609459200, 0), // 2021-01-01 00:00:00
					IsDir:       false,
					Permissions: 0644,
					Hash:        "01020304",
				},
				{
					Path:        "/test/dir/file.txt",
					Size:        2048,
					ModTime:     time.Unix(1609545600, 0), // 2021-01-02 00:00:00
					IsDir:       false,
					Permissions: 0644,
					Hash:        "05060708",
				},
			},
		},
		{
			name: "WithNilHash",
			entries: []walker.SnapshotEntry{
				{
					Path:        "/test/file1.txt",
					Size:        1024,
					ModTime:     time.Unix(1609459200, 0), // 2021-01-01 00:00:00
					IsDir:       false,
					Permissions: 0644,
					Hash:        "01020304",
				},
				{
					Path:        "/test/file2.txt",
					Size:        2048,
					ModTime:     time.Unix(1609545600, 0), // 2021-01-02 00:00:00
					IsDir:       false,
					Permissions: 0644,
					Hash:        "05060708",
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc // Capture range variable for parallel execution
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Serialize the snapshot
			data, err := SerializeSnapshot(tc.entries)
			if err != nil {
				t.Fatalf("SerializeSnapshot failed: %v", err)
			}

			// Verify the serialized data
			snapshot := flashfs.GetRootAsSnapshot(data, 0)
			if snapshot.EntriesLength() != len(tc.entries) {
				t.Errorf("Expected %d entries, got %d", len(tc.entries), snapshot.EntriesLength())
			}

			// Check each entry
			var entry flashfs.FileEntry
			for i := 0; i < snapshot.EntriesLength(); i++ {
				if !snapshot.Entries(&entry, i) {
					t.Errorf("Failed to get entry %d", i)
					continue
				}

				expected := tc.entries[i]
				path := string(entry.Path())
				if path != expected.Path {
					t.Errorf("Entry %d: expected path %s, got %s", i, expected.Path, path)
				}

				if entry.Size() != expected.Size {
					t.Errorf("Entry %d: expected size %d, got %d", i, expected.Size, entry.Size())
				}

				if entry.Mtime() != expected.ModTime.Unix() {
					t.Errorf("Entry %d: expected mtime %d, got %d", i, expected.ModTime.Unix(), entry.Mtime())
				}

				if entry.IsDir() != expected.IsDir {
					t.Errorf("Entry %d: expected isDir %v, got %v", i, expected.IsDir, entry.IsDir())
				}

				if entry.Permissions() != uint32(expected.Permissions) {
					t.Errorf("Entry %d: expected permissions %o, got %o", i, expected.Permissions, entry.Permissions())
				}

				// Check hash if it exists
				hashBytes := entry.HashBytes()
				if expected.Hash != "" {
					if len(hashBytes) == 0 {
						t.Errorf("Entry %d: expected hash, got none", i)
					} else {
						// Convert expected hash string to bytes for comparison
						expectedHashBytes, err := hex.DecodeString(expected.Hash)
						if err != nil {
							t.Errorf("Entry %d: failed to decode expected hash: %v", i, err)
						} else {
							// Compare hash bytes
							if len(hashBytes) != len(expectedHashBytes) {
								t.Errorf("Entry %d: hash length mismatch, expected %d, got %d",
									i, len(expectedHashBytes), len(hashBytes))
							} else {
								for j := 0; j < len(hashBytes); j++ {
									if hashBytes[j] != expectedHashBytes[j] {
										t.Errorf("Entry %d: hash mismatch at byte %d", i, j)
										break
									}
								}
							}
						}
					}
				} else {
					if len(hashBytes) > 0 {
						t.Errorf("Entry %d: expected no hash, got %d bytes", i, len(hashBytes))
					}
				}
			}
		})
	}
}

func TestSerializeSnapshotEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("VeryLargeSnapshot", func(t *testing.T) {
		t.Parallel()

		// Create a large number of entries
		const numEntries = 1000
		entries := make([]walker.SnapshotEntry, numEntries)
		for i := 0; i < numEntries; i++ {
			entries[i] = walker.SnapshotEntry{
				Path:        "/test/file" + string(rune(i)),
				Size:        int64(i * 1024),
				ModTime:     time.Unix(1609459200+int64(i), 0), // 2021-01-01 00:00:00 + i seconds
				IsDir:       false,
				Permissions: 0644,
				Hash:        "01020304",
			}
		}

		// Serialize the snapshot
		data, err := SerializeSnapshot(entries)
		if err != nil {
			t.Fatalf("SerializeSnapshot failed: %v", err)
		}

		// Verify the serialized data
		snapshot := flashfs.GetRootAsSnapshot(data, 0)
		if snapshot.EntriesLength() != numEntries {
			t.Errorf("Expected %d entries, got %d", numEntries, snapshot.EntriesLength())
		}
	})

	t.Run("LongPaths", func(t *testing.T) {
		t.Parallel()

		// Create an entry with a very long path
		longPath := "/test"
		for i := 0; i < 100; i++ {
			longPath += "/subdir" + string(rune(i))
		}
		longPath += "/file.txt"

		entries := []walker.SnapshotEntry{
			{
				Path:        longPath,
				Size:        1024,
				ModTime:     time.Unix(1609459200, 0), // 2021-01-01 00:00:00
				IsDir:       false,
				Permissions: 0644,
				Hash:        "01020304",
			},
		}

		// Serialize the snapshot
		data, err := SerializeSnapshot(entries)
		if err != nil {
			t.Fatalf("SerializeSnapshot failed: %v", err)
		}

		// Verify the serialized data
		snapshot := flashfs.GetRootAsSnapshot(data, 0)
		if snapshot.EntriesLength() != 1 {
			t.Errorf("Expected 1 entry, got %d", snapshot.EntriesLength())
		}

		var entry flashfs.FileEntry
		if !snapshot.Entries(&entry, 0) {
			t.Fatalf("Failed to get entry 0")
		}

		path := string(entry.Path())
		if path != longPath {
			t.Errorf("Expected path %s, got %s", longPath, path)
		}
	})

	t.Run("LargeHash", func(t *testing.T) {
		t.Parallel()

		// Create a large hash (1024 bytes)
		largeHash := make([]byte, 1024)
		for i := 0; i < len(largeHash); i++ {
			largeHash[i] = byte(i % 256)
		}

		// Convert to hex string
		largeHashHex := hex.EncodeToString(largeHash)

		entries := []walker.SnapshotEntry{
			{
				Path:        "/test/file.txt",
				Size:        1024,
				ModTime:     time.Unix(1609459200, 0), // 2021-01-01 00:00:00
				IsDir:       false,
				Permissions: 0644,
				Hash:        largeHashHex,
			},
		}

		// Serialize the snapshot
		data, err := SerializeSnapshot(entries)
		if err != nil {
			t.Fatalf("SerializeSnapshot failed: %v", err)
		}

		// Verify the serialized data
		snapshot := flashfs.GetRootAsSnapshot(data, 0)
		if snapshot.EntriesLength() != 1 {
			t.Errorf("Expected 1 entry, got %d", snapshot.EntriesLength())
		}

		var entry flashfs.FileEntry
		if !snapshot.Entries(&entry, 0) {
			t.Fatalf("Failed to get entry 0")
		}

		hashBytes := entry.HashBytes()
		if len(hashBytes) != len(largeHash) {
			t.Errorf("Hash length mismatch, expected %d, got %d", len(largeHash), len(hashBytes))
		} else {
			for i := 0; i < len(hashBytes); i++ {
				if hashBytes[i] != largeHash[i] {
					t.Errorf("Hash mismatch at byte %d", i)
					break
				}
			}
		}
	})
}
