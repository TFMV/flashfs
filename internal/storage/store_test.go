package storage

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/flashfs/internal/metadata"
	"github.com/TFMV/flashfs/internal/serializer"
	"github.com/TFMV/flashfs/internal/walker"
)

// MockStore is a simplified store implementation for testing
type MockStore struct {
	snapshots map[string][]walker.SnapshotEntry
	diffs     map[string]map[string][]walker.SnapshotEntry
}

// NewMockStore creates a new mock store
func NewMockStore() *MockStore {
	return &MockStore{
		snapshots: make(map[string][]walker.SnapshotEntry),
		diffs:     make(map[string]map[string][]walker.SnapshotEntry),
	}
}

// CreateSnapshot creates a new snapshot in the mock store
func (m *MockStore) CreateSnapshot(id string, description string, rootPath string, entries []walker.SnapshotEntry) error {
	m.snapshots[id] = entries
	return nil
}

// GetSnapshot retrieves a snapshot from the mock store
func (m *MockStore) GetSnapshot(id string) ([]walker.SnapshotEntry, error) {
	entries, ok := m.snapshots[id]
	if !ok {
		return nil, fmt.Errorf("snapshot not found: %s", id)
	}
	return entries, nil
}

// DeleteSnapshot deletes a snapshot from the mock store
func (m *MockStore) DeleteSnapshot(id string) error {
	delete(m.snapshots, id)
	return nil
}

// ComputeDiff computes the difference between two snapshots
func (m *MockStore) ComputeDiff(oldID, newID string) ([]serializer.DiffEntry, error) {
	// This is a simplified implementation for testing
	return []serializer.DiffEntry{}, nil
}

// StoreDiff stores a diff between two snapshots
func (m *MockStore) StoreDiff(oldID, newID string) error {
	return nil
}

// GetDiff retrieves a diff between two snapshots
func (m *MockStore) GetDiff(oldID, newID string) ([]serializer.DiffEntry, error) {
	return []serializer.DiffEntry{}, nil
}

// DeleteDiff deletes a diff between two snapshots
func (m *MockStore) DeleteDiff(oldID, newID string) error {
	return nil
}

// FindFilesByPattern finds files in a snapshot matching a pattern
func (m *MockStore) FindFilesByPattern(snapshotID, pattern string) (map[string]metadata.FileMetadata, error) {
	return make(map[string]metadata.FileMetadata), nil
}

// FindFilesBySize finds files in a snapshot within a size range
func (m *MockStore) FindFilesBySize(snapshotID string, minSize, maxSize int64) (map[string]metadata.FileMetadata, error) {
	return make(map[string]metadata.FileMetadata), nil
}

// FindFilesByHash finds files in a snapshot with a specific hash
func (m *MockStore) FindFilesByHash(snapshotID, hash string) (map[string]metadata.FileMetadata, error) {
	return make(map[string]metadata.FileMetadata), nil
}

func TestMockStore(t *testing.T) {
	// Create a new mock store
	store := NewMockStore()

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

	// Test creating a snapshot
	snapshotID := "snapshot1"
	description := "Test snapshot"
	rootPath := "/test"
	if err := store.CreateSnapshot(snapshotID, description, rootPath, entries); err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Test retrieving a snapshot
	retrievedEntries, err := store.GetSnapshot(snapshotID)
	if err != nil {
		t.Fatalf("Failed to retrieve snapshot: %v", err)
	}

	// Verify the retrieved entries
	if len(retrievedEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(retrievedEntries))
	}

	// Create a second snapshot with modifications
	entries2 := []walker.SnapshotEntry{
		{
			Path:        "/test/file1.txt",
			Size:        2048,               // Modified size
			ModTime:     now.Add(time.Hour), // Modified time
			IsDir:       false,
			Permissions: 0644,
			Hash:        "modified1234567890", // Modified hash
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
			Path:        "/test/file3.txt", // New file
			Size:        4096,
			ModTime:     now.Add(2 * time.Hour),
			IsDir:       false,
			Permissions: 0644,
			Hash:        "newfile1234567890",
		},
		// file2.txt is deleted
	}

	// Test creating a second snapshot
	snapshotID2 := "snapshot2"
	description2 := "Test snapshot 2"
	if err := store.CreateSnapshot(snapshotID2, description2, rootPath, entries2); err != nil {
		t.Fatalf("Failed to create second snapshot: %v", err)
	}

	// Test deleting a snapshot
	if err := store.DeleteSnapshot(snapshotID); err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}

	// Verify the snapshot was deleted
	_, err = store.GetSnapshot(snapshotID)
	if err == nil {
		t.Fatalf("Expected error when retrieving deleted snapshot, got nil")
	}
}

func TestMockStoreWithLargeData(t *testing.T) {
	// Skip this test if running in short mode
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Create a new mock store
	store := NewMockStore()

	// Create test data with many entries
	now := time.Now()
	entries := make([]walker.SnapshotEntry, 1000)
	for i := 0; i < 1000; i++ {
		// Create a valid hex hash string
		hash := fmt.Sprintf("%016x", i)

		entries[i] = walker.SnapshotEntry{
			Path:        filepath.Join("/test", fmt.Sprintf("file%d.txt", i)),
			Size:        int64(i * 1024),
			ModTime:     now.Add(time.Duration(i) * time.Minute),
			IsDir:       i%100 == 0,
			Permissions: fs.FileMode(0644),
			Hash:        hash,
		}
	}

	// Test creating a snapshot with many entries
	snapshotID := "large_snapshot"
	description := "Large test snapshot"
	rootPath := "/test"
	if err := store.CreateSnapshot(snapshotID, description, rootPath, entries); err != nil {
		t.Fatalf("Failed to create large snapshot: %v", err)
	}

	// Test retrieving a snapshot with many entries
	retrievedEntries, err := store.GetSnapshot(snapshotID)
	if err != nil {
		t.Fatalf("Failed to retrieve large snapshot: %v", err)
	}

	// Verify the retrieved entries
	if len(retrievedEntries) != len(entries) {
		t.Fatalf("Expected %d entries, got %d", len(entries), len(retrievedEntries))
	}

	// Test deleting a snapshot with many entries
	if err := store.DeleteSnapshot(snapshotID); err != nil {
		t.Fatalf("Failed to delete large snapshot: %v", err)
	}

	// Verify the snapshot was deleted
	_, err = store.GetSnapshot(snapshotID)
	if err == nil {
		t.Fatalf("Expected error when retrieving deleted snapshot, got nil")
	}
}
