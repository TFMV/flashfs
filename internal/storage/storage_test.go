package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/flashfs/internal/walker"
	"github.com/TFMV/flashfs/schema/flashfs"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotStore(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a new snapshot store
	store, err := NewSnapshotStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	t.Run("WriteAndReadSnapshot", func(t *testing.T) {
		t.Parallel()

		// Create a separate directory for this test to avoid parallel test issues
		testDir, err := os.MkdirTemp("", "flashfs-write-read-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		testStore, err := NewSnapshotStore(testDir)
		require.NoError(t, err)
		defer testStore.Close()

		// Create a test snapshot
		builder := flatbuffers.NewBuilder(1024)

		// Create a file entry
		pathOffset := builder.CreateString("/test/file.txt")
		hashBytes := []byte{1, 2, 3, 4}
		hashOffset := builder.CreateByteVector(hashBytes)

		flashfs.FileEntryStart(builder)
		flashfs.FileEntryAddPath(builder, pathOffset)
		flashfs.FileEntryAddSize(builder, 1024)
		flashfs.FileEntryAddMtime(builder, time.Now().Unix())
		flashfs.FileEntryAddIsDir(builder, false)
		flashfs.FileEntryAddPermissions(builder, 0644)
		flashfs.FileEntryAddHash(builder, hashOffset)
		fileEntryOffset := flashfs.FileEntryEnd(builder)

		// Create a snapshot with the file entry
		flashfs.SnapshotStartEntriesVector(builder, 1)
		builder.PrependUOffsetT(fileEntryOffset)
		entriesVector := builder.EndVector(1)

		flashfs.SnapshotStart(builder)
		flashfs.SnapshotAddEntries(builder, entriesVector)
		snapshot := flashfs.SnapshotEnd(builder)

		builder.Finish(snapshot)
		snapshotData := builder.FinishedBytes()

		// Write the snapshot
		err = testStore.WriteSnapshot("test-snapshot", snapshotData)
		require.NoError(t, err)

		// Verify the file exists
		snapPath := filepath.Join(testDir, "test-snapshot"+SnapshotFileExt)
		_, err = os.Stat(snapPath)
		require.NoError(t, err, "Snapshot file should exist")

		// Read the snapshot
		readData, err := testStore.ReadSnapshot("test-snapshot")
		require.NoError(t, err)

		// Verify the data
		assert.Equal(t, snapshotData, readData)
	})

	t.Run("ListSnapshots", func(t *testing.T) {
		t.Parallel()

		// Create a test snapshot store in a separate directory
		listDir, err := os.MkdirTemp("", "flashfs-list-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(listDir)

		listStore, err := NewSnapshotStore(listDir)
		require.NoError(t, err)
		defer listStore.Close()

		// Create some test snapshots
		testData := []byte("test data")
		require.NoError(t, listStore.WriteSnapshot("snapshot1", testData))
		require.NoError(t, listStore.WriteSnapshot("snapshot2", testData))
		require.NoError(t, listStore.WriteSnapshot("snapshot3", testData))

		// List the snapshots
		snapshots, err := listStore.ListSnapshots()
		require.NoError(t, err)

		// Verify the list
		assert.Len(t, snapshots, 3)
		assert.Contains(t, snapshots, "snapshot1")
		assert.Contains(t, snapshots, "snapshot2")
		assert.Contains(t, snapshots, "snapshot3")
	})

	t.Run("DeleteSnapshot", func(t *testing.T) {
		t.Parallel()

		// Create a test snapshot store in a separate directory
		deleteDir, err := os.MkdirTemp("", "flashfs-delete-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(deleteDir)

		deleteStore, err := NewSnapshotStore(deleteDir)
		require.NoError(t, err)
		defer deleteStore.Close()

		// Create a test snapshot
		testData := []byte("test data")
		require.NoError(t, deleteStore.WriteSnapshot("to-delete", testData))

		// Verify it exists
		_, err = deleteStore.ReadSnapshot("to-delete")
		require.NoError(t, err)

		// Delete the snapshot
		err = deleteStore.DeleteSnapshot("to-delete")
		require.NoError(t, err)

		// Verify it's gone
		_, err = deleteStore.ReadSnapshot("to-delete")
		require.Error(t, err)
	})

	t.Run("CacheSnapshot", func(t *testing.T) {
		t.Parallel()

		// Create a test snapshot store with a small cache
		cacheDir, err := os.MkdirTemp("", "flashfs-cache-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(cacheDir)

		cacheStore, err := NewSnapshotStore(cacheDir)
		require.NoError(t, err)
		defer cacheStore.Close()

		// Set a small cache size
		cacheStore.SetCacheSize(2)

		// Create some test snapshots
		testData1 := []byte("test data 1")
		testData2 := []byte("test data 2")
		testData3 := []byte("test data 3")

		require.NoError(t, cacheStore.WriteSnapshot("cache1", testData1))
		require.NoError(t, cacheStore.WriteSnapshot("cache2", testData2))

		// These should be in cache now
		cacheStore.cacheMutex.RLock()
		assert.Len(t, cacheStore.cacheKeys, 2)
		assert.Contains(t, cacheStore.snapshotCache, "cache1")
		assert.Contains(t, cacheStore.snapshotCache, "cache2")
		cacheStore.cacheMutex.RUnlock()

		// Add a third one, which should evict the first
		require.NoError(t, cacheStore.WriteSnapshot("cache3", testData3))

		cacheStore.cacheMutex.RLock()
		assert.Len(t, cacheStore.cacheKeys, 2)
		assert.NotContains(t, cacheStore.snapshotCache, "cache1")
		assert.Contains(t, cacheStore.snapshotCache, "cache2")
		assert.Contains(t, cacheStore.snapshotCache, "cache3")
		cacheStore.cacheMutex.RUnlock()
	})
}

func TestDiffOperations(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-diff-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a new snapshot store
	store, err := NewSnapshotStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	// Create two test snapshots with different content
	snapshot1 := createTestSnapshot(t, []walker.SnapshotEntry{
		{Path: "/test/file1.txt", Size: 100, ModTime: 1000, IsDir: false, Permissions: 0644, Hash: []byte{1, 2, 3, 4}},
		{Path: "/test/file2.txt", Size: 200, ModTime: 2000, IsDir: false, Permissions: 0644, Hash: []byte{5, 6, 7, 8}},
	})

	snapshot2 := createTestSnapshot(t, []walker.SnapshotEntry{
		{Path: "/test/file1.txt", Size: 100, ModTime: 1000, IsDir: false, Permissions: 0644, Hash: []byte{1, 2, 3, 4}},     // Unchanged
		{Path: "/test/file2.txt", Size: 300, ModTime: 3000, IsDir: false, Permissions: 0644, Hash: []byte{9, 10, 11, 12}},  // Modified
		{Path: "/test/file3.txt", Size: 400, ModTime: 4000, IsDir: false, Permissions: 0644, Hash: []byte{13, 14, 15, 16}}, // Added
	})

	// Write the snapshots
	require.NoError(t, store.WriteSnapshot("snapshot1", snapshot1))
	require.NoError(t, store.WriteSnapshot("snapshot2", snapshot2))

	t.Run("ComputeDiff", func(t *testing.T) {
		// Compute the diff
		diff, err := store.ComputeDiff("snapshot1", "snapshot2")
		require.NoError(t, err)

		// Verify the diff contains the expected changes
		diffSnapshot := flashfs.GetRootAsSnapshot(diff, 0)
		assert.Equal(t, 2, diffSnapshot.EntriesLength()) // Modified + Added

		// Check each entry in the diff
		var entry flashfs.FileEntry
		foundModified := false
		foundAdded := false

		for i := 0; i < diffSnapshot.EntriesLength(); i++ {
			diffSnapshot.Entries(&entry, i)
			path := string(entry.Path())

			if path == "/test/file2.txt" {
				foundModified = true
				assert.Equal(t, int64(300), entry.Size())
				assert.Equal(t, int64(3000), entry.Mtime())
			} else if path == "/test/file3.txt" {
				foundAdded = true
				assert.Equal(t, int64(400), entry.Size())
				assert.Equal(t, int64(4000), entry.Mtime())
			}
		}

		assert.True(t, foundModified, "Modified file should be in diff")
		assert.True(t, foundAdded, "Added file should be in diff")
	})

	t.Run("StoreDiff", func(t *testing.T) {
		// Store the diff
		err := store.StoreDiff("snapshot1", "snapshot2")
		require.NoError(t, err)

		// Verify the diff file exists
		diffPath := filepath.Join(tempDir, "snapshot1_snapshot2"+DiffFileExt)
		_, err = os.Stat(diffPath)
		require.NoError(t, err)
	})

	t.Run("ApplyDiff", func(t *testing.T) {
		// First store the diff
		require.NoError(t, store.StoreDiff("snapshot1", "snapshot2"))

		// Apply the diff
		result, err := store.ApplyDiff("snapshot1", "snapshot1_snapshot2")
		require.NoError(t, err)

		// Verify the result matches snapshot2
		snapshot2Data, err := store.ReadSnapshot("snapshot2")
		require.NoError(t, err)

		// Compare the snapshots by checking their entries
		resultSnapshot := flashfs.GetRootAsSnapshot(result, 0)
		expectedSnapshot := flashfs.GetRootAsSnapshot(snapshot2Data, 0)

		assert.Equal(t, expectedSnapshot.EntriesLength(), resultSnapshot.EntriesLength())

		// Create maps of entries for easier comparison
		resultEntries := make(map[string]*flashfs.FileEntry)
		expectedEntries := make(map[string]*flashfs.FileEntry)

		var entry flashfs.FileEntry
		for i := 0; i < resultSnapshot.EntriesLength(); i++ {
			resultSnapshot.Entries(&entry, i)
			entryCopy := new(flashfs.FileEntry)
			*entryCopy = entry
			resultEntries[string(entry.Path())] = entryCopy
		}

		for i := 0; i < expectedSnapshot.EntriesLength(); i++ {
			expectedSnapshot.Entries(&entry, i)
			entryCopy := new(flashfs.FileEntry)
			*entryCopy = entry
			expectedEntries[string(entry.Path())] = entryCopy
		}

		// Verify all expected entries are in the result
		for path, expectedEntry := range expectedEntries {
			resultEntry, exists := resultEntries[path]
			assert.True(t, exists, "Path %s should exist in result", path)
			if exists {
				assert.Equal(t, expectedEntry.Size(), resultEntry.Size())
				assert.Equal(t, expectedEntry.Mtime(), resultEntry.Mtime())
				assert.Equal(t, expectedEntry.IsDir(), resultEntry.IsDir())
				assert.Equal(t, expectedEntry.Permissions(), resultEntry.Permissions())
			}
		}
	})
}

func TestBloomFilter(t *testing.T) {
	t.Parallel()

	t.Run("BasicOperations", func(t *testing.T) {
		filter := NewBloomFilter(100, 4)

		// Add some items
		filter.Add([]byte("item1"))
		filter.Add([]byte("item2"))
		filter.Add([]byte("item3"))

		// Check for existence
		assert.True(t, filter.Contains([]byte("item1")))
		assert.True(t, filter.Contains([]byte("item2")))
		assert.True(t, filter.Contains([]byte("item3")))
		assert.False(t, filter.Contains([]byte("item4")))
	})

	t.Run("CreateFromSnapshot", func(t *testing.T) {
		// Create a temporary directory for testing
		tempDir, err := os.MkdirTemp("", "flashfs-bloom-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Create a new snapshot store
		store, err := NewSnapshotStore(tempDir)
		require.NoError(t, err)
		defer store.Close()

		// Create a test snapshot
		snapshot := createTestSnapshot(t, []walker.SnapshotEntry{
			{Path: "/test/file1.txt", Size: 100, ModTime: 1000, IsDir: false, Permissions: 0644, Hash: []byte{1, 2, 3, 4}},
			{Path: "/test/file2.txt", Size: 200, ModTime: 2000, IsDir: false, Permissions: 0644, Hash: []byte{5, 6, 7, 8}},
		})

		// Write the snapshot
		require.NoError(t, store.WriteSnapshot("bloom-test", snapshot))

		// Create a bloom filter from the snapshot
		filter, err := store.CreateBloomFilterFromSnapshot("bloom-test")
		require.NoError(t, err)

		// Check for existence
		assert.True(t, filter.Contains([]byte("/test/file1.txt")))
		assert.True(t, filter.Contains([]byte("/test/file2.txt")))
		assert.False(t, filter.Contains([]byte("/test/file3.txt")))
	})
}

func TestQuerySnapshot(t *testing.T) {
	t.Parallel()

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-query-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a new snapshot store
	store, err := NewSnapshotStore(tempDir)
	require.NoError(t, err)
	defer store.Close()

	// Create a test snapshot with various entries
	snapshot := createTestSnapshot(t, []walker.SnapshotEntry{
		{Path: "/test/file1.txt", Size: 100, ModTime: 1000, IsDir: false, Permissions: 0644, Hash: []byte{1, 2, 3, 4}},
		{Path: "/test/file2.txt", Size: 200, ModTime: 2000, IsDir: false, Permissions: 0644, Hash: []byte{5, 6, 7, 8}},
		{Path: "/test/dir1", Size: 0, ModTime: 3000, IsDir: true, Permissions: 0755, Hash: nil},
		{Path: "/test/dir1/file3.txt", Size: 300, ModTime: 4000, IsDir: false, Permissions: 0644, Hash: []byte{9, 10, 11, 12}},
	})

	// Write the snapshot
	require.NoError(t, store.WriteSnapshot("query-test", snapshot))

	t.Run("QueryAll", func(t *testing.T) {
		// Query all entries
		results, err := store.QuerySnapshot("query-test", nil)
		require.NoError(t, err)
		assert.Len(t, results, 4)
	})

	t.Run("QueryByPath", func(t *testing.T) {
		// Query by path prefix
		results, err := store.QuerySnapshot("query-test", func(entry *flashfs.FileEntry) bool {
			return bytes.HasPrefix(entry.Path(), []byte("/test/dir1"))
		})
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("QueryBySize", func(t *testing.T) {
		// Query by size
		results, err := store.QuerySnapshot("query-test", func(entry *flashfs.FileEntry) bool {
			return entry.Size() > 100
		})
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("QueryByIsDir", func(t *testing.T) {
		// Query directories
		results, err := store.QuerySnapshot("query-test", func(entry *flashfs.FileEntry) bool {
			return entry.IsDir()
		})
		require.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, "/test/dir1", string(results[0].Path()))
	})

	t.Run("QueryByModTime", func(t *testing.T) {
		// Query by modification time
		results, err := store.QuerySnapshot("query-test", func(entry *flashfs.FileEntry) bool {
			return entry.Mtime() > 2000
		})
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})
}

// Helper function to create a test snapshot
func createTestSnapshot(t *testing.T, entries []walker.SnapshotEntry) []byte {
	builder := flatbuffers.NewBuilder(1024)
	fileEntryOffsets := make([]flatbuffers.UOffsetT, len(entries))

	for i, entry := range entries {
		pathOffset := builder.CreateString(entry.Path)

		var hashOffset flatbuffers.UOffsetT
		if len(entry.Hash) > 0 {
			hashOffset = builder.CreateByteVector(entry.Hash)
		}

		flashfs.FileEntryStart(builder)
		flashfs.FileEntryAddPath(builder, pathOffset)
		flashfs.FileEntryAddSize(builder, entry.Size)
		flashfs.FileEntryAddMtime(builder, entry.ModTime)
		flashfs.FileEntryAddIsDir(builder, entry.IsDir)
		flashfs.FileEntryAddPermissions(builder, entry.Permissions)
		if len(entry.Hash) > 0 {
			flashfs.FileEntryAddHash(builder, hashOffset)
		}
		fileEntryOffsets[i] = flashfs.FileEntryEnd(builder)
	}

	// Create vector of file entries
	flashfs.SnapshotStartEntriesVector(builder, len(fileEntryOffsets))
	for i := len(fileEntryOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(fileEntryOffsets[i])
	}
	entriesVector := builder.EndVector(len(fileEntryOffsets))

	flashfs.SnapshotStart(builder)
	flashfs.SnapshotAddEntries(builder, entriesVector)
	snapshot := flashfs.SnapshotEnd(builder)

	builder.Finish(snapshot)
	return builder.FinishedBytes()
}
