package metadata

import (
	"os"
	"testing"
	"time"
)

func TestMetadataStore(t *testing.T) {
	// Create a temporary file for the database
	tmpFile, err := os.CreateTemp("", "metadata_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Create a new metadata store
	store, err := New(tmpPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Test adding and retrieving a snapshot
	snapshot := SnapshotMetadata{
		ID:          "snap1",
		CreatedAt:   time.Now(),
		Description: "Test snapshot",
		FileCount:   100,
		TotalSize:   1024 * 1024 * 10, // 10MB
		RootPath:    "/test/path",
	}

	if err := store.AddSnapshot(snapshot); err != nil {
		t.Fatalf("Failed to add snapshot: %v", err)
	}

	retrievedSnapshot, err := store.GetSnapshot("snap1")
	if err != nil {
		t.Fatalf("Failed to retrieve snapshot: %v", err)
	}

	if retrievedSnapshot.ID != snapshot.ID ||
		retrievedSnapshot.Description != snapshot.Description ||
		retrievedSnapshot.FileCount != snapshot.FileCount ||
		retrievedSnapshot.TotalSize != snapshot.TotalSize ||
		retrievedSnapshot.RootPath != snapshot.RootPath {
		t.Fatalf("Retrieved snapshot does not match original: %+v vs %+v", retrievedSnapshot, snapshot)
	}

	// Test adding and retrieving file metadata
	fileMetadata := FileMetadata{
		Size:        1024,
		ModTime:     time.Now().Unix(),
		Permissions: 0644,
		IsDir:       false,
		Hash:        "abcdef1234567890",
		DataRef:     "snap1.data:offset:1234:len:1024",
	}

	if err := store.AddFileMetadata("snap1", "/test/file.txt", fileMetadata); err != nil {
		t.Fatalf("Failed to add file metadata: %v", err)
	}

	retrievedFileMetadata, err := store.GetFileMetadata("snap1", "/test/file.txt")
	if err != nil {
		t.Fatalf("Failed to retrieve file metadata: %v", err)
	}

	if retrievedFileMetadata.Size != fileMetadata.Size ||
		retrievedFileMetadata.ModTime != fileMetadata.ModTime ||
		retrievedFileMetadata.Permissions != fileMetadata.Permissions ||
		retrievedFileMetadata.IsDir != fileMetadata.IsDir ||
		retrievedFileMetadata.Hash != fileMetadata.Hash ||
		retrievedFileMetadata.DataRef != fileMetadata.DataRef {
		t.Fatalf("Retrieved file metadata does not match original: %+v vs %+v", retrievedFileMetadata, fileMetadata)
	}

	// Test adding and retrieving diff metadata
	diffMetadata := DiffMetadata{
		Type:           "modified",
		OldSize:        1024,
		NewSize:        2048,
		OldModTime:     time.Now().Add(-time.Hour).Unix(),
		NewModTime:     time.Now().Unix(),
		OldPermissions: 0644,
		NewPermissions: 0755,
		OldHash:        "abcdef1234567890",
		NewHash:        "0987654321fedcba",
		DiffRef:        "diff_snap1_snap2.data:offset:1234:len:1024",
	}

	if err := store.AddDiffMetadata("snap1", "snap2", "/test/file.txt", diffMetadata); err != nil {
		t.Fatalf("Failed to add diff metadata: %v", err)
	}

	retrievedDiffMetadata, err := store.GetDiffMetadata("snap1", "snap2", "/test/file.txt")
	if err != nil {
		t.Fatalf("Failed to retrieve diff metadata: %v", err)
	}

	if retrievedDiffMetadata.Type != diffMetadata.Type ||
		retrievedDiffMetadata.OldSize != diffMetadata.OldSize ||
		retrievedDiffMetadata.NewSize != diffMetadata.NewSize ||
		retrievedDiffMetadata.OldModTime != diffMetadata.OldModTime ||
		retrievedDiffMetadata.NewModTime != diffMetadata.NewModTime ||
		retrievedDiffMetadata.OldPermissions != diffMetadata.OldPermissions ||
		retrievedDiffMetadata.NewPermissions != diffMetadata.NewPermissions ||
		retrievedDiffMetadata.OldHash != diffMetadata.OldHash ||
		retrievedDiffMetadata.NewHash != diffMetadata.NewHash ||
		retrievedDiffMetadata.DiffRef != diffMetadata.DiffRef {
		t.Fatalf("Retrieved diff metadata does not match original: %+v vs %+v", retrievedDiffMetadata, diffMetadata)
	}

	// Test listing snapshots
	snapshots, err := store.ListSnapshots()
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}

	if len(snapshots) != 1 || snapshots[0].ID != "snap1" {
		t.Fatalf("Expected 1 snapshot with ID 'snap1', got %d snapshots", len(snapshots))
	}

	// Test listing diffs
	diffs, err := store.ListDiffs("snap1", "snap2")
	if err != nil {
		t.Fatalf("Failed to list diffs: %v", err)
	}

	if len(diffs) != 1 || diffs["/test/file.txt"].Type != "modified" {
		t.Fatalf("Expected 1 diff for '/test/file.txt', got %d diffs", len(diffs))
	}

	// Test finding files by pattern
	files, err := store.FindFilesByPattern("snap1", "/test/*.txt")
	if err != nil {
		t.Fatalf("Failed to find files by pattern: %v", err)
	}

	if len(files) != 1 || files["/test/file.txt"].Size != 1024 {
		t.Fatalf("Expected 1 file matching pattern '/test/*.txt', got %d files", len(files))
	}

	// Test deleting a snapshot
	if err := store.DeleteSnapshot("snap1"); err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}

	_, err = store.GetSnapshot("snap1")
	if err == nil {
		t.Fatalf("Expected error when retrieving deleted snapshot, got nil")
	}

	// Test shrinking the database
	if err := store.Shrink(); err != nil {
		t.Fatalf("Failed to shrink database: %v", err)
	}
}

func TestFindFilesBySize(t *testing.T) {
	// Create a temporary file for the database
	tmpFile, err := os.CreateTemp("", "metadata_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Create a new metadata store
	store, err := New(tmpPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Add a snapshot
	snapshot := SnapshotMetadata{
		ID:          "snap1",
		CreatedAt:   time.Now(),
		Description: "Test snapshot",
		FileCount:   3,
		TotalSize:   3072,
		RootPath:    "/test/path",
	}

	if err := store.AddSnapshot(snapshot); err != nil {
		t.Fatalf("Failed to add snapshot: %v", err)
	}

	// Add file metadata with different sizes
	files := []struct {
		path string
		size int64
	}{
		{"/test/small.txt", 512},
		{"/test/medium.txt", 1024},
		{"/test/large.txt", 2048},
	}

	for _, file := range files {
		fileMetadata := FileMetadata{
			Size:        file.size,
			ModTime:     time.Now().Unix(),
			Permissions: 0644,
			IsDir:       false,
			Hash:        "hash_" + file.path,
			DataRef:     "snap1.data:offset:" + file.path,
		}

		if err := store.AddFileMetadata("snap1", file.path, fileMetadata); err != nil {
			t.Fatalf("Failed to add file metadata: %v", err)
		}
	}

	// Test finding files by size range
	testCases := []struct {
		name     string
		minSize  int64
		maxSize  int64
		expected int
	}{
		{"Small files", 0, 512, 1},
		{"Medium files", 513, 1024, 1},
		{"Large files", 1025, 3000, 1},
		{"All files", 0, 3000, 3},
		{"No files", 3000, 4000, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			files, err := store.FindFilesBySize("snap1", tc.minSize, tc.maxSize)
			if err != nil {
				t.Fatalf("Failed to find files by size: %v", err)
			}

			if len(files) != tc.expected {
				t.Fatalf("Expected %d files, got %d", tc.expected, len(files))
			}
		})
	}
}

func TestFindFilesByModTime(t *testing.T) {
	// Create a temporary file for the database
	tmpFile, err := os.CreateTemp("", "metadata_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Create a new metadata store
	store, err := New(tmpPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Add a snapshot
	snapshot := SnapshotMetadata{
		ID:          "snap1",
		CreatedAt:   time.Now(),
		Description: "Test snapshot",
		FileCount:   3,
		TotalSize:   3072,
		RootPath:    "/test/path",
	}

	if err := store.AddSnapshot(snapshot); err != nil {
		t.Fatalf("Failed to add snapshot: %v", err)
	}

	// Add file metadata with different modification times
	now := time.Now()
	files := []struct {
		path    string
		modTime time.Time
	}{
		{"/test/old.txt", now.Add(-24 * time.Hour)},
		{"/test/recent.txt", now.Add(-1 * time.Hour)},
		{"/test/new.txt", now},
	}

	for _, file := range files {
		fileMetadata := FileMetadata{
			Size:        1024,
			ModTime:     file.modTime.Unix(),
			Permissions: 0644,
			IsDir:       false,
			Hash:        "hash_" + file.path,
			DataRef:     "snap1.data:offset:" + file.path,
		}

		if err := store.AddFileMetadata("snap1", file.path, fileMetadata); err != nil {
			t.Fatalf("Failed to add file metadata: %v", err)
		}
	}

	// Test finding files by modification time range
	testCases := []struct {
		name      string
		startTime time.Time
		endTime   time.Time
		expected  int
	}{
		{"Old files", now.Add(-48 * time.Hour), now.Add(-12 * time.Hour), 1},
		{"Recent files", now.Add(-2 * time.Hour), now.Add(-30 * time.Minute), 1},
		{"New files", now.Add(-30 * time.Minute), now.Add(time.Hour), 1},
		{"All files", now.Add(-48 * time.Hour), now.Add(time.Hour), 3},
		{"No files", now.Add(time.Hour), now.Add(2 * time.Hour), 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			files, err := store.FindFilesByModTime("snap1", tc.startTime, tc.endTime)
			if err != nil {
				t.Fatalf("Failed to find files by mod time: %v", err)
			}

			if len(files) != tc.expected {
				t.Fatalf("Expected %d files, got %d", tc.expected, len(files))
			}
		})
	}
}

func TestFindFilesByHash(t *testing.T) {
	// Create a temporary file for the database
	tmpFile, err := os.CreateTemp("", "metadata_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Create a new metadata store
	store, err := New(tmpPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Add a snapshot
	snapshot := SnapshotMetadata{
		ID:          "snap1",
		CreatedAt:   time.Now(),
		Description: "Test snapshot",
		FileCount:   3,
		TotalSize:   3072,
		RootPath:    "/test/path",
	}

	if err := store.AddSnapshot(snapshot); err != nil {
		t.Fatalf("Failed to add snapshot: %v", err)
	}

	// Add file metadata with different hashes
	files := []struct {
		path string
		hash string
	}{
		{"/test/file1.txt", "hash1"},
		{"/test/file2.txt", "hash2"},
		{"/test/file3.txt", "hash1"}, // Duplicate hash
	}

	for _, file := range files {
		fileMetadata := FileMetadata{
			Size:        1024,
			ModTime:     time.Now().Unix(),
			Permissions: 0644,
			IsDir:       false,
			Hash:        file.hash,
			DataRef:     "snap1.data:offset:" + file.path,
		}

		if err := store.AddFileMetadata("snap1", file.path, fileMetadata); err != nil {
			t.Fatalf("Failed to add file metadata: %v", err)
		}
	}

	// Test finding files by hash
	testCases := []struct {
		name     string
		hash     string
		expected int
	}{
		{"Hash1", "hash1", 2},
		{"Hash2", "hash2", 1},
		{"NonexistentHash", "hash3", 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			files, err := store.FindFilesByHash("snap1", tc.hash)
			if err != nil {
				t.Fatalf("Failed to find files by hash: %v", err)
			}

			if len(files) != tc.expected {
				t.Fatalf("Expected %d files, got %d", tc.expected, len(files))
			}
		})
	}
}
