package diff

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	tempFileName := tempFile.Name()
	tempFile.Close() // Close to ensure we can write to it

	// Serialize the snapshot
	data, err := serializer.SerializeSnapshot(entries)
	if err != nil {
		t.Fatalf("Failed to serialize snapshot: %v", err)
	}

	// Write the snapshot to the file
	if err := os.WriteFile(tempFileName, data, 0644); err != nil {
		t.Fatalf("Failed to write snapshot: %v", err)
	}

	// Verify the file exists and has content
	info, err := os.Stat(tempFileName)
	if err != nil {
		t.Fatalf("Failed to stat snapshot file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("Snapshot file is empty: %s", tempFileName)
	}

	return tempFileName
}

func TestCompareSnapshots(t *testing.T) {
	// Create test entries
	oldEntries := []walker.SnapshotEntry{
		{Path: "file1.txt", Size: 100, ModTime: time.Unix(1000, 0), IsDir: false, Permissions: 0644, Hash: "0102"},
		{Path: "file2.txt", Size: 200, ModTime: time.Unix(2000, 0), IsDir: false, Permissions: 0644, Hash: "0304"},
		{Path: "file3.txt", Size: 300, ModTime: time.Unix(3000, 0), IsDir: false, Permissions: 0644, Hash: "0506"},
	}

	newEntries := []walker.SnapshotEntry{
		{Path: "file1.txt", Size: 100, ModTime: time.Unix(1000, 0), IsDir: false, Permissions: 0644, Hash: "0102"}, // Unchanged
		{Path: "file2.txt", Size: 250, ModTime: time.Unix(2500, 0), IsDir: false, Permissions: 0644, Hash: "0708"}, // Modified
		{Path: "file4.txt", Size: 400, ModTime: time.Unix(4000, 0), IsDir: false, Permissions: 0644, Hash: "090a"}, // Added
		// file3.txt is deleted
	}

	// Create test snapshots
	oldFile := createTestSnapshot(t, oldEntries)
	defer os.Remove(oldFile)

	newFile := createTestSnapshot(t, newEntries)
	defer os.Remove(newFile)

	// Test basic comparison
	t.Run("BasicComparison", func(t *testing.T) {
		diffs, err := CompareSnapshots(oldFile, newFile)
		if err != nil {
			t.Fatalf("CompareSnapshots failed: %v", err)
		}

		// Check that we have the expected number of differences
		if len(diffs) != 3 {
			t.Errorf("Expected 3 differences, got %d", len(diffs))
		}

		// Check for specific differences
		hasModified := false
		hasNew := false
		hasDeleted := false

		for _, diff := range diffs {
			if strings.Contains(diff, "Modified: file2.txt") {
				hasModified = true
			} else if strings.Contains(diff, "New: file4.txt") {
				hasNew = true
			} else if strings.Contains(diff, "Deleted: file3.txt") {
				hasDeleted = true
			}
		}

		if !hasModified {
			t.Error("Missing modified file in diff")
		}
		if !hasNew {
			t.Error("Missing new file in diff")
		}
		if !hasDeleted {
			t.Error("Missing deleted file in diff")
		}
	})

	// Test with context
	t.Run("WithContext", func(t *testing.T) {
		ctx := context.Background()
		diffs, err := CompareSnapshotsWithContext(ctx, oldFile, newFile)
		if err != nil {
			t.Fatalf("CompareSnapshotsWithContext failed: %v", err)
		}

		if len(diffs) != 3 {
			t.Errorf("Expected 3 differences, got %d", len(diffs))
		}
	})

	// Test with canceled context
	t.Run("CanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := CompareSnapshotsWithContext(ctx, oldFile, newFile)
		if err == nil {
			t.Fatal("Expected error with canceled context, got nil")
		}
		if !strings.Contains(err.Error(), "operation canceled") {
			t.Errorf("Expected 'operation canceled' error, got: %v", err)
		}
	})

	// Test with non-existent file
	t.Run("NonExistentFile", func(t *testing.T) {
		nonExistentFile := filepath.Join(os.TempDir(), "non-existent-file-"+time.Now().Format("20060102150405")+".snap")
		_, err := CompareSnapshots(nonExistentFile, newFile)
		if err == nil {
			t.Fatal("Expected error with non-existent file, got nil")
		}
		if !strings.Contains(err.Error(), "snapshot file not found") {
			t.Errorf("Expected 'snapshot file not found' error, got: %v", err)
		}
	})

	// Test with empty file
	t.Run("EmptyFile", func(t *testing.T) {
		emptyFile, err := os.CreateTemp("", "flashfs-diff-test-empty-*.snap")
		if err != nil {
			t.Fatalf("Failed to create empty file: %v", err)
		}
		emptyFileName := emptyFile.Name()
		emptyFile.Close()
		defer os.Remove(emptyFileName)

		_, err = CompareSnapshots(emptyFileName, newFile)
		if err == nil {
			t.Fatal("Expected error with empty file, got nil")
		}
		if !strings.Contains(err.Error(), "invalid snapshot file") {
			t.Errorf("Expected 'invalid snapshot file' error, got: %v", err)
		}
	})
}

func TestCompareSnapshotsDetailed(t *testing.T) {
	// Create test entries with hashes
	oldEntries := []walker.SnapshotEntry{
		{Path: "file1.txt", Size: 100, ModTime: time.Unix(1000, 0), IsDir: false, Permissions: 0644, Hash: "010203"},
		{Path: "file2.txt", Size: 200, ModTime: time.Unix(2000, 0), IsDir: false, Permissions: 0644, Hash: "040506"},
		{Path: "file3.txt", Size: 300, ModTime: time.Unix(3000, 0), IsDir: false, Permissions: 0644, Hash: "070809"},
		{Path: "dir1/file4.txt", Size: 400, ModTime: time.Unix(4000, 0), IsDir: false, Permissions: 0644, Hash: "101112"},
	}

	newEntries := []walker.SnapshotEntry{
		{Path: "file1.txt", Size: 100, ModTime: time.Unix(1000, 0), IsDir: false, Permissions: 0644, Hash: "010203"},      // Unchanged
		{Path: "file2.txt", Size: 250, ModTime: time.Unix(2500, 0), IsDir: false, Permissions: 0644, Hash: "040507"},      // Modified size, time, hash
		{Path: "file3.txt", Size: 300, ModTime: time.Unix(3000, 0), IsDir: false, Permissions: 0644, Hash: "070810"},      // Modified hash only
		{Path: "dir1/file5.txt", Size: 500, ModTime: time.Unix(5000, 0), IsDir: false, Permissions: 0644, Hash: "131415"}, // New
		// file4.txt is deleted
	}

	// Create test snapshots
	oldFile := createTestSnapshot(t, oldEntries)
	defer os.Remove(oldFile)

	newFile := createTestSnapshot(t, newEntries)
	defer os.Remove(newFile)

	// Test detailed comparison
	t.Run("DetailedComparison", func(t *testing.T) {
		ctx := context.Background()
		diffs, err := CompareSnapshotsDetailed(ctx, oldFile, newFile)
		if err != nil {
			t.Fatalf("CompareSnapshotsDetailed failed: %v", err)
		}

		// Check that we have the expected number of differences
		if len(diffs) != 4 {
			t.Errorf("Expected 4 differences, got %d", len(diffs))
		}

		// Check for specific differences
		var modifiedSizeTime, modifiedHashOnly, newFile, deletedFile *DiffEntry
		for i := range diffs {
			diff := &diffs[i]
			if diff.Type == "Modified" && diff.Path == "file2.txt" {
				modifiedSizeTime = diff
			} else if diff.Type == "Modified" && diff.Path == "file3.txt" {
				modifiedHashOnly = diff
			} else if diff.Type == "New" && diff.Path == "dir1/file5.txt" {
				newFile = diff
			} else if diff.Type == "Deleted" && diff.Path == "dir1/file4.txt" {
				deletedFile = diff
			}
		}

		if modifiedSizeTime == nil {
			t.Error("Missing modified file (size/time) in diff")
		} else {
			if modifiedSizeTime.OldSize != 200 || modifiedSizeTime.NewSize != 250 {
				t.Errorf("Incorrect size in modified file: old=%d, new=%d", modifiedSizeTime.OldSize, modifiedSizeTime.NewSize)
			}
			if modifiedSizeTime.OldModTime != 2000 || modifiedSizeTime.NewModTime != 2500 {
				t.Errorf("Incorrect modTime in modified file: old=%d, new=%d", modifiedSizeTime.OldModTime, modifiedSizeTime.NewModTime)
			}
		}

		if modifiedHashOnly == nil {
			t.Error("Missing modified file (hash only) in diff")
		} else {
			if !modifiedHashOnly.HashDiff {
				t.Error("HashDiff flag not set for hash-only modified file")
			}
			if modifiedHashOnly.OldSize != modifiedHashOnly.NewSize {
				t.Error("Size should be the same for hash-only modified file")
			}
		}

		if newFile == nil {
			t.Error("Missing new file in diff")
		} else {
			if newFile.NewSize != 500 {
				t.Errorf("Incorrect size in new file: %d", newFile.NewSize)
			}
		}

		if deletedFile == nil {
			t.Error("Missing deleted file in diff")
		} else {
			if deletedFile.OldSize != 400 {
				t.Errorf("Incorrect size in deleted file: %d", deletedFile.OldSize)
			}
		}
	})

	// Test with options
	t.Run("WithOptions", func(t *testing.T) {
		ctx := context.Background()
		options := DiffOptions{
			PathPrefix:     "dir1/",
			UseHashDiff:    true,
			Workers:        2,
			UseBloomFilter: false,
		}

		diffs, err := CompareSnapshotsDetailedWithOptions(ctx, oldFile, newFile, options)
		if err != nil {
			t.Fatalf("CompareSnapshotsDetailedWithOptions failed: %v", err)
		}

		// Should only include files with the prefix "dir1/"
		if len(diffs) != 2 {
			t.Errorf("Expected 2 differences with path prefix, got %d", len(diffs))
		}

		for _, diff := range diffs {
			if !strings.HasPrefix(diff.Path, "dir1/") {
				t.Errorf("Found diff with incorrect path prefix: %s", diff.Path)
			}
		}
	})

	// Test with multiple workers
	t.Run("ParallelDiffing", func(t *testing.T) {
		ctx := context.Background()
		options := DiffOptions{
			Workers:     runtime.NumCPU(), // Use all available CPUs
			UseHashDiff: true,             // Explicitly enable hash diffing
		}

		diffs, err := CompareSnapshotsDetailedWithOptions(ctx, oldFile, newFile, options)
		if err != nil {
			t.Fatalf("Parallel diffing failed: %v", err)
		}

		// Should include the hash-only modified file
		if len(diffs) != 4 {
			t.Errorf("Expected 4 differences with parallel diffing, got %d", len(diffs))
			for _, diff := range diffs {
				t.Logf("Found diff: %s", diff.DetailedString())
			}
		}

		// Verify we have all expected diff types
		hasModifiedSize := false
		hasModifiedHash := false
		hasNew := false
		hasDeleted := false

		for _, diff := range diffs {
			if diff.Type == "Modified" && diff.Path == "file2.txt" {
				hasModifiedSize = true
			} else if diff.Type == "Modified" && diff.Path == "file3.txt" {
				hasModifiedHash = true
			} else if diff.Type == "New" && diff.Path == "dir1/file5.txt" {
				hasNew = true
			} else if diff.Type == "Deleted" && diff.Path == "dir1/file4.txt" {
				hasDeleted = true
			}
		}

		if !hasModifiedSize {
			t.Error("Missing modified file (size/time) in parallel diff")
		}
		if !hasModifiedHash {
			t.Error("Missing modified file (hash only) in parallel diff")
		}
		if !hasNew {
			t.Error("Missing new file in parallel diff")
		}
		if !hasDeleted {
			t.Error("Missing deleted file in parallel diff")
		}
	})

	// Test without hash diffing
	t.Run("WithoutHashDiffing", func(t *testing.T) {
		ctx := context.Background()
		options := DiffOptions{
			UseHashDiff: false,
		}

		diffs, err := CompareSnapshotsDetailedWithOptions(ctx, oldFile, newFile, options)
		if err != nil {
			t.Fatalf("CompareSnapshotsDetailedWithOptions failed: %v", err)
		}

		// Should not include the hash-only modified file
		if len(diffs) != 3 {
			t.Errorf("Expected 3 differences without hash diffing, got %d", len(diffs))
		}

		for _, diff := range diffs {
			if diff.Type == "Modified" && diff.Path == "file3.txt" {
				t.Error("Found hash-only modified file when UseHashDiff is false")
			}
		}
	})
}

func TestDiffEntry(t *testing.T) {
	// Test String() method
	t.Run("String", func(t *testing.T) {
		entry := DiffEntry{
			Type: "Modified",
			Path: "test/file.txt",
		}
		expected := "Modified: test/file.txt"
		if entry.String() != expected {
			t.Errorf("Expected %q, got %q", expected, entry.String())
		}
	})

	// Test DetailedString() method
	t.Run("DetailedString", func(t *testing.T) {
		// Test modified file with size and time changes
		entry := DiffEntry{
			Type:           "Modified",
			Path:           "test/file.txt",
			OldSize:        100,
			NewSize:        200,
			OldModTime:     1000,
			NewModTime:     2000,
			OldPermissions: 0644,
			NewPermissions: 0644,
		}

		detailed := entry.DetailedString()
		if !strings.Contains(detailed, "Modified: test/file.txt") {
			t.Errorf("DetailedString missing basic info: %s", detailed)
		}
		if !strings.Contains(detailed, "size: 100 → 200") {
			t.Errorf("DetailedString missing size change: %s", detailed)
		}
		if !strings.Contains(detailed, "mtime: 1000 → 2000") {
			t.Errorf("DetailedString missing mtime change: %s", detailed)
		}

		// Test hash-only change
		hashEntry := DiffEntry{
			Type:     "Modified",
			Path:     "test/file.txt",
			OldSize:  100,
			NewSize:  100,
			HashDiff: true,
		}

		hashDetailed := hashEntry.DetailedString()
		if !strings.Contains(hashDetailed, "hash changed") {
			t.Errorf("DetailedString missing hash change: %s", hashDetailed)
		}
	})
}

func TestBloomFilter(t *testing.T) {
	// Create a bloom filter
	bf := &BloomFilter{
		bits:    make([]byte, 128), // 1024 bits
		numHash: 3,
	}

	// Add some items
	testItems := []string{
		"file1.txt",
		"file2.txt",
		"dir1/file3.txt",
	}

	for _, item := range testItems {
		bf.Add([]byte(item))
	}

	// Test items that should be in the filter
	for _, item := range testItems {
		if !bf.Contains([]byte(item)) {
			t.Errorf("Item %q should be in the bloom filter", item)
		}
	}

	// Test items that should not be in the filter
	notInFilter := []string{
		"file4.txt",
		"dir2/file5.txt",
		"completely_different.txt",
	}

	falsePositives := 0
	for _, item := range notInFilter {
		if bf.Contains([]byte(item)) {
			falsePositives++
		}
	}

	// Allow some false positives (bloom filters can have them)
	if falsePositives > 1 {
		t.Errorf("Too many false positives: %d out of %d", falsePositives, len(notInFilter))
	}
}

func TestCreateBloomFilterSimple(t *testing.T) {
	// Create a simple snapshot
	snapshot := make(map[string]walker.SnapshotEntry)
	for i := 0; i < 100; i++ {
		path := fmt.Sprintf("file%d.txt", i)
		snapshot[path] = walker.SnapshotEntry{
			Path: path,
			Size: int64(i * 100),
		}
	}

	// Create bloom filter with a reasonable false positive rate
	bf := createBloomFilter(snapshot, 0.01)

	// Check that all items are in the filter
	for path := range snapshot {
		if !bf.Contains([]byte(path)) {
			t.Errorf("Item %q should be in the bloom filter", path)
		}
	}
}

func TestCreateBloomFilterEdgeCases(t *testing.T) {
	// Test with empty snapshot
	emptySnapshot := make(map[string]walker.SnapshotEntry)
	bf1 := createBloomFilter(emptySnapshot, 0.01)
	if bf1 == nil {
		t.Fatal("Bloom filter should not be nil for empty snapshot")
	}

	// Test with very small false positive rate
	smallSnapshot := make(map[string]walker.SnapshotEntry)
	smallSnapshot["test.txt"] = walker.SnapshotEntry{Path: "test.txt"}
	bf2 := createBloomFilter(smallSnapshot, 0.0000001)
	if bf2 == nil {
		t.Fatal("Bloom filter should not be nil for very small false positive rate")
	}

	// Test with very large false positive rate
	bf3 := createBloomFilter(smallSnapshot, 0.99999)
	if bf3 == nil {
		t.Fatal("Bloom filter should not be nil for very large false positive rate")
	}

	// Test with negative false positive rate (should use default)
	bf4 := createBloomFilter(smallSnapshot, -0.5)
	if bf4 == nil {
		t.Fatal("Bloom filter should not be nil for negative false positive rate")
	}
}
