package diff

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/flashfs/internal/serializer"
	"github.com/TFMV/flashfs/internal/walker"
)

func createTestSnapshot(t *testing.T, entries []walker.SnapshotEntry) string {
	// Create a temporary file for the snapshot
	tempFile, err := os.CreateTemp("", "flashfs-diff-test-*.snap")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tempFile.Close() // Close to ensure we can write to it

	// Serialize the snapshot
	data, err := serializer.SerializeSnapshot(entries)
	if err != nil {
		t.Fatalf("Failed to serialize snapshot: %v", err)
	}

	// Write the snapshot to the file
	if err := os.WriteFile(tempFile.Name(), data, 0644); err != nil {
		t.Fatalf("Failed to write snapshot: %v", err)
	}

	return tempFile.Name()
}

func TestCompareSnapshots(t *testing.T) {
	// Create test entries for old snapshot
	oldEntries := []walker.SnapshotEntry{
		{
			Path:        "file1.txt",
			Size:        100,
			ModTime:     time.Now().Add(-24 * time.Hour).Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{1, 2, 3, 4},
		},
		{
			Path:        "dir1",
			Size:        0,
			ModTime:     time.Now().Add(-24 * time.Hour).Unix(),
			IsDir:       true,
			Permissions: 0755,
		},
		{
			Path:        "dir1/file2.txt",
			Size:        200,
			ModTime:     time.Now().Add(-24 * time.Hour).Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{5, 6, 7, 8},
		},
		{
			Path:        "file3.txt",
			Size:        300,
			ModTime:     time.Now().Add(-24 * time.Hour).Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{9, 10, 11, 12},
		},
	}

	// Create test entries for new snapshot with changes
	newEntries := []walker.SnapshotEntry{
		{
			Path:        "file1.txt",
			Size:        150, // Modified size
			ModTime:     time.Now().Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{13, 14, 15, 16}, // Modified hash
		},
		{
			Path:        "dir1",
			Size:        0,
			ModTime:     time.Now().Add(-24 * time.Hour).Unix(),
			IsDir:       true,
			Permissions: 0755,
		},
		// file2.txt is deleted
		{
			Path:        "file3.txt",
			Size:        300,
			ModTime:     time.Now().Add(-24 * time.Hour).Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{9, 10, 11, 12},
		},
		{
			Path:        "file4.txt", // New file
			Size:        400,
			ModTime:     time.Now().Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{17, 18, 19, 20},
		},
	}

	// Create test snapshots
	oldFile := createTestSnapshot(t, oldEntries)
	defer os.Remove(oldFile)
	newFile := createTestSnapshot(t, newEntries)
	defer os.Remove(newFile)

	// Test CompareSnapshots
	t.Run("BasicComparison", func(t *testing.T) {
		diffs, err := CompareSnapshots(oldFile, newFile)
		if err != nil {
			t.Fatalf("CompareSnapshots failed: %v", err)
		}

		// Verify the expected differences
		expectedCount := 3 // 1 modified, 1 new, 1 deleted
		if len(diffs) != expectedCount {
			t.Errorf("Expected %d diffs, got %d", expectedCount, len(diffs))
		}

		// Check for specific changes
		hasModified := false
		hasNew := false
		hasDeleted := false

		for _, diff := range diffs {
			if diff == "Modified: file1.txt" {
				hasModified = true
			} else if diff == "New: file4.txt" {
				hasNew = true
			} else if diff == "Deleted: dir1/file2.txt" {
				hasDeleted = true
			}
		}

		if !hasModified {
			t.Error("Missing modified file in diff results")
		}
		if !hasNew {
			t.Error("Missing new file in diff results")
		}
		if !hasDeleted {
			t.Error("Missing deleted file in diff results")
		}
	})

	// Test CompareSnapshotsWithContext
	t.Run("WithContext", func(t *testing.T) {
		ctx := context.Background()
		diffs, err := CompareSnapshotsWithContext(ctx, oldFile, newFile)
		if err != nil {
			t.Fatalf("CompareSnapshotsWithContext failed: %v", err)
		}

		// Verify the expected differences
		expectedCount := 3 // 1 modified, 1 new, 1 deleted
		if len(diffs) != expectedCount {
			t.Errorf("Expected %d diffs, got %d", expectedCount, len(diffs))
		}
	})

	// Test with canceled context
	t.Run("WithCanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := CompareSnapshotsWithContext(ctx, oldFile, newFile)
		if err == nil {
			t.Error("Expected error with canceled context, got nil")
		}
	})

	// Test with non-existent file
	t.Run("WithNonExistentFile", func(t *testing.T) {
		nonExistentFile := filepath.Join(os.TempDir(), "non-existent-file-"+time.Now().Format("20060102150405")+".snap")
		_, err := CompareSnapshots(oldFile, nonExistentFile)
		if err == nil {
			t.Error("Expected error with non-existent file, got nil")
		}
	})

	// Test with empty file
	t.Run("WithEmptyFile", func(t *testing.T) {
		emptyFile, err := os.CreateTemp("", "flashfs-diff-test-empty-*.snap")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(emptyFile.Name())
		emptyFile.Close()

		_, err = CompareSnapshots(oldFile, emptyFile.Name())
		if err == nil {
			t.Error("Expected error with empty file, got nil")
		}
	})
}

func TestCompareSnapshotsDetailed(t *testing.T) {
	// Create test entries for old snapshot
	oldEntries := []walker.SnapshotEntry{
		{
			Path:        "file1.txt",
			Size:        100,
			ModTime:     time.Now().Add(-24 * time.Hour).Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{1, 2, 3, 4},
		},
		{
			Path:        "file2.txt",
			Size:        200,
			ModTime:     time.Now().Add(-24 * time.Hour).Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{5, 6, 7, 8},
		},
	}

	// Create test entries for new snapshot with changes
	newEntries := []walker.SnapshotEntry{
		{
			Path:        "file1.txt",
			Size:        150, // Modified size
			ModTime:     time.Now().Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{13, 14, 15, 16}, // Modified hash
		},
		{
			Path:        "file3.txt", // New file
			Size:        300,
			ModTime:     time.Now().Unix(),
			IsDir:       false,
			Permissions: 0644,
			Hash:        []byte{9, 10, 11, 12},
		},
	}

	// Create test snapshots
	oldFile := createTestSnapshot(t, oldEntries)
	defer os.Remove(oldFile)
	newFile := createTestSnapshot(t, newEntries)
	defer os.Remove(newFile)

	// Test CompareSnapshotsDetailed
	t.Run("DetailedComparison", func(t *testing.T) {
		ctx := context.Background()
		diffs, err := CompareSnapshotsDetailed(ctx, oldFile, newFile)
		if err != nil {
			t.Fatalf("CompareSnapshotsDetailed failed: %v", err)
		}

		// Verify the expected differences
		expectedCount := 3 // 1 modified, 1 new, 1 deleted
		if len(diffs) != expectedCount {
			t.Errorf("Expected %d diffs, got %d", expectedCount, len(diffs))
		}

		// Check for specific changes
		hasModified := false
		hasNew := false
		hasDeleted := false

		for _, diff := range diffs {
			if diff.Type == "Modified" && diff.Path == "file1.txt" {
				hasModified = true
			} else if diff.Type == "New" && diff.Path == "file3.txt" {
				hasNew = true
			} else if diff.Type == "Deleted" && diff.Path == "file2.txt" {
				hasDeleted = true
			}
		}

		if !hasModified {
			t.Error("Missing modified file in detailed diff results")
		}
		if !hasNew {
			t.Error("Missing new file in detailed diff results")
		}
		if !hasDeleted {
			t.Error("Missing deleted file in detailed diff results")
		}
	})

	// Test with canceled context
	t.Run("WithCanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := CompareSnapshotsDetailed(ctx, oldFile, newFile)
		if err == nil {
			t.Error("Expected error with canceled context, got nil")
		}
	})
}

func TestDiffEntry(t *testing.T) {
	entry := DiffEntry{
		Type: "Modified",
		Path: "test/file.txt",
	}

	expected := "Modified: test/file.txt"
	if entry.String() != expected {
		t.Errorf("Expected %q, got %q", expected, entry.String())
	}
}
