package query

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

// --- Fake types and constants for testing ---

// Diff types used in ComputeDiff.
const (
	DiffAdded    = 1
	DiffModified = 2
)

// FakeFile simulates a file record in the hot layer.
type FakeFile struct {
	Size          int64
	ModTime       int64 // Unix timestamp
	IsDir         bool
	Hash          string
	Permissions   uint32
	DataRef       string
	SymlinkTarget string
}

// FakeSnapshotEntry simulates an entry in the cold layer.
type FakeSnapshotEntry struct {
	Path          string
	Size          int64
	ModTime       time.Time
	IsDir         bool
	Hash          string
	Permissions   int
	SymlinkTarget string
}

// FakeSnapshot simulates snapshot metadata for ListSnapshots.
type FakeSnapshot struct {
	ID string
}

// FakeDiffEntry simulates a diff record.
type FakeDiffEntry struct {
	Path           string
	Type           int
	NewSize        int64
	NewModTime     time.Time
	IsDir          bool
	NewHash        string
	NewPermissions uint32
}

// FakeStore is a fake implementation of the storage.Store interface.
type FakeStore struct {
	hotLayerEnabled bool

	// hotSnapshots indicates whether a snapshot exists in the hot layer.
	hotSnapshots map[string]bool

	// hotFiles maps snapshotID to a set of files (keyed by path).
	hotFiles map[string]map[string]FakeFile

	// coldSnapshots maps snapshotID to a list of entries.
	coldSnapshots map[string][]FakeSnapshotEntry

	// snapshots holds all snapshots for ListSnapshots.
	snapshots []FakeSnapshot

	// diffEntries maps oldID -> newID -> list of diff entries.
	diffEntries map[string]map[string][]FakeDiffEntry
}

func (fs *FakeStore) IsHotLayerEnabled() bool {
	return fs.hotLayerEnabled
}

func (fs *FakeStore) GetSnapshotMetadata(snapshotID string) (interface{}, error) {
	if exists, ok := fs.hotSnapshots[snapshotID]; ok && exists {
		return struct{}{}, nil
	}
	return nil, fmt.Errorf("snapshot not found in hot layer")
}

func (fs *FakeStore) FindFilesByPattern(snapshotID, pattern string) (map[string]FakeFile, error) {
	files, ok := fs.hotFiles[snapshotID]
	if !ok {
		return nil, fmt.Errorf("no hot files for snapshot %s", snapshotID)
	}
	result := make(map[string]FakeFile)
	for path, file := range files {
		matched, err := filepath.Match(pattern, path)
		if err != nil {
			return nil, err
		}
		if matched {
			result[path] = file
		}
	}
	return result, nil
}

func (fs *FakeStore) GetSnapshot(snapshotID string) ([]FakeSnapshotEntry, error) {
	entries, ok := fs.coldSnapshots[snapshotID]
	if !ok {
		return nil, fmt.Errorf("cold snapshot %s not found", snapshotID)
	}
	return entries, nil
}

func (fs *FakeStore) ListSnapshots() ([]FakeSnapshot, error) {
	return fs.snapshots, nil
}

func (fs *FakeStore) ComputeDiff(oldID, newID string) ([]FakeDiffEntry, error) {
	if diffsByNew, ok := fs.diffEntries[oldID]; ok {
		if diffs, ok := diffsByNew[newID]; ok {
			return diffs, nil
		}
	}
	return nil, fmt.Errorf("no diff found between %s and %s", oldID, newID)
}

// --- Tests ---

func TestQueryEngine(t *testing.T) {
	// Set up a fake store with data for hot and cold layers.
	now := time.Now()
	fakeStore := &FakeStore{
		hotLayerEnabled: true,
		hotSnapshots: map[string]bool{
			"snap1": true,
			"snap2": true,
		},
		hotFiles: map[string]map[string]FakeFile{
			"snap1": {
				"file1.txt": {
					Size:          100,
					ModTime:       now.Unix(),
					IsDir:         false,
					Hash:          "hash1",
					Permissions:   0644,
					DataRef:       "ref1",
					SymlinkTarget: "",
				},
			},
			"snap2": {
				"file3.txt": {
					Size:          150,
					ModTime:       now.Add(-1 * time.Hour).Unix(),
					IsDir:         false,
					Hash:          "hash3",
					Permissions:   0600,
					DataRef:       "ref3",
					SymlinkTarget: "",
				},
			},
		},
		coldSnapshots: map[string][]FakeSnapshotEntry{
			"snap1": {
				{
					Path:          "file2.txt",
					Size:          200,
					ModTime:       now.Add(-2 * time.Hour),
					IsDir:         false,
					Hash:          "hash2",
					Permissions:   0755,
					SymlinkTarget: "",
				},
			},
			"snap2": {
				{
					Path:          "file4.txt",
					Size:          250,
					ModTime:       now.Add(-3 * time.Hour),
					IsDir:         false,
					Hash:          "hash4",
					Permissions:   0755,
					SymlinkTarget: "",
				},
			},
		},
		snapshots: []FakeSnapshot{
			{ID: "snap1"},
			{ID: "snap2"},
		},
		diffEntries: map[string]map[string][]FakeDiffEntry{
			"snap1": {
				"snap2": {
					{
						Path:           "file5.txt",
						Type:           DiffAdded,
						NewSize:        300,
						NewModTime:     now.Add(-30 * time.Minute),
						IsDir:          false,
						NewHash:        "hash5",
						NewPermissions: 0644,
					},
				},
			},
		},
	}

	// Wrap fakeStore in a minimal adapter so that QueryEngine can call its methods.
	// For testing we assume FakeStore satisfies the minimal interface required by QueryEngine.
	adapter := &fakeStoreAdapter{FakeStore: fakeStore}
	engine := NewQueryEngine(adapter)

	// Use default options (pattern "*", no size/time filtering, both layers included).
	opts := DefaultQueryOptions()

	t.Run("QuerySnapshot includes both hot and cold", func(t *testing.T) {
		results, err := engine.QuerySnapshot("snap1", opts)
		if err != nil {
			t.Fatalf("QuerySnapshot failed: %v", err)
		}
		// Expect two results: one from hot ("file1.txt") and one from cold ("file2.txt").
		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
		// Check that at least one result comes from the hot layer.
		foundHot := false
		for _, r := range results {
			if r.FromHotLayer {
				foundHot = true
			}
		}
		if !foundHot {
			t.Error("expected at least one hot layer result")
		}
	})

	t.Run("QueryMultipleSnapshots", func(t *testing.T) {
		ids := []string{"snap1", "snap2"}
		results, err := engine.QueryMultipleSnapshots(ids, opts)
		if err != nil {
			t.Fatalf("QueryMultipleSnapshots failed: %v", err)
		}
		// Expect hot+cold from each snapshot.
		// snap1: file1.txt and file2.txt, snap2: file3.txt and file4.txt => total 4.
		if len(results) != 4 {
			t.Errorf("expected 4 results, got %d", len(results))
		}
	})

	t.Run("QueryAllSnapshots", func(t *testing.T) {
		results, err := engine.QueryAllSnapshots(opts)
		if err != nil {
			t.Fatalf("QueryAllSnapshots failed: %v", err)
		}
		// Should be same as QueryMultipleSnapshots with snap1 and snap2.
		if len(results) != 4 {
			t.Errorf("expected 4 results, got %d", len(results))
		}
	})

	t.Run("FindDuplicateFiles", func(t *testing.T) {
		// Duplicate file: add a file with same hash to snap2 in cold layer.
		fakeStore.coldSnapshots["snap2"] = append(fakeStore.coldSnapshots["snap2"], FakeSnapshotEntry{
			Path:          "duplicate.txt",
			Size:          250,
			ModTime:       now.Add(-4 * time.Hour),
			IsDir:         false,
			Hash:          "hash4", // same as file4.txt in snap2
			Permissions:   0755,
			SymlinkTarget: "",
		})
		ids := []string{"snap2"}
		dups, err := engine.FindDuplicateFiles(ids, opts)
		if err != nil {
			t.Fatalf("FindDuplicateFiles failed: %v", err)
		}
		// Expect one duplicate group with hash "hash4".
		if len(dups) != 1 {
			t.Errorf("expected 1 duplicate group, got %d", len(dups))
		}
		for hash, files := range dups {
			if hash != "hash4" {
				t.Errorf("unexpected duplicate hash %s", hash)
			}
			if len(files) < 2 {
				t.Error("expected at least 2 files in duplicate group")
			}
		}
	})

	t.Run("FindFilesChangedBetweenSnapshots", func(t *testing.T) {
		results, err := engine.FindFilesChangedBetweenSnapshots("snap1", "snap2", opts)
		if err != nil {
			t.Fatalf("FindFilesChangedBetweenSnapshots failed: %v", err)
		}
		// We expect the diff entry for "file5.txt" from snap1->snap2.
		if len(results) != 1 {
			t.Errorf("expected 1 diff result, got %d", len(results))
		}
		if results[0].Path != "file5.txt" {
			t.Errorf("expected diff for file5.txt, got %s", results[0].Path)
		}
	})

	t.Run("FindLargest/Newest/Oldest Files", func(t *testing.T) {
		// For snapshot "snap1", we have two files:
		// hot: file1.txt (size 100, modTime now)
		// cold: file2.txt (size 200, modTime now-2h)
		largest, err := engine.FindLargestFiles("snap1", 1, opts)
		if err != nil {
			t.Fatalf("FindLargestFiles failed: %v", err)
		}
		if len(largest) == 0 || largest[0].Size != 200 {
			t.Errorf("expected largest file to be size 200, got %v", largest)
		}

		newest, err := engine.FindNewestFiles("snap1", 1, opts)
		if err != nil {
			t.Fatalf("FindNewestFiles failed: %v", err)
		}
		// file1.txt should be the newest (modTime now).
		if len(newest) == 0 || newest[0].Path != "file1.txt" {
			t.Errorf("expected newest file to be file1.txt, got %v", newest)
		}

		oldest, err := engine.FindOldestFiles("snap1", 1, opts)
		if err != nil {
			t.Fatalf("FindOldestFiles failed: %v", err)
		}
		// file2.txt should be the oldest.
		if len(oldest) == 0 || oldest[0].Path != "file2.txt" {
			t.Errorf("expected oldest file to be file2.txt, got %v", oldest)
		}
	})
}

// --- fakeStoreAdapter ---
//
// The QueryEngine expects a store with certain methods. In production this is likely
// implemented by *storage.Store. For testing we wrap our FakeStore so that it satisfies
// the minimal interface required by QueryEngine.
type fakeStoreAdapter struct {
	*FakeStore
}

func (f fakeStoreAdapter) IsHotLayerEnabled() bool {
	return f.FakeStore.IsHotLayerEnabled()
}

func (f fakeStoreAdapter) GetSnapshotMetadata(snapshotID string) (interface{}, error) {
	return f.FakeStore.GetSnapshotMetadata(snapshotID)
}

func (f fakeStoreAdapter) FindFilesByPattern(snapshotID, pattern string) (map[string]metadata.FileMetadata, error) {
	files, err := f.FakeStore.FindFilesByPattern(snapshotID, pattern)
	if err != nil {
		return nil, err
	}

	result := make(map[string]metadata.FileMetadata)
	for path, file := range files {
		result[path] = metadata.FileMetadata{
			Size:          file.Size,
			ModTime:       file.ModTime,
			IsDir:         file.IsDir,
			Hash:          file.Hash,
			Permissions:   file.Permissions,
			DataRef:       file.DataRef,
			SymlinkTarget: file.SymlinkTarget,
		}
	}
	return result, nil
}

func (f fakeStoreAdapter) GetSnapshot(snapshotID string) ([]walker.SnapshotEntry, error) {
	entries, err := f.FakeStore.GetSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	result := make([]walker.SnapshotEntry, len(entries))
	for i, entry := range entries {
		result[i] = walker.SnapshotEntry{
			Path:          entry.Path,
			Size:          entry.Size,
			ModTime:       entry.ModTime,
			IsDir:         entry.IsDir,
			Hash:          entry.Hash,
			Permissions:   fs.FileMode(entry.Permissions),
			SymlinkTarget: entry.SymlinkTarget,
		}
	}
	return result, nil
}

func (f fakeStoreAdapter) ListSnapshots() ([]metadata.SnapshotMetadata, error) {
	snapshots, err := f.FakeStore.ListSnapshots()
	if err != nil {
		return nil, err
	}

	result := make([]metadata.SnapshotMetadata, len(snapshots))
	for i, snapshot := range snapshots {
		result[i] = metadata.SnapshotMetadata{
			ID: snapshot.ID,
		}
	}
	return result, nil
}

func (fs *fakeStoreAdapter) ComputeDiff(oldID, newID string) ([]serializer.DiffEntry, error) {
	// Convert FakeDiffEntry to serializer.DiffEntry
	var result []serializer.DiffEntry
	if fs.diffEntries == nil {
		return result, nil
	}

	if oldMap, ok := fs.diffEntries[oldID]; ok {
		if entries, ok := oldMap[newID]; ok {
			for _, entry := range entries {
				var diffType serializer.DiffType
				switch entry.Type {
				case DiffAdded:
					diffType = serializer.DiffAdded
				case DiffModified:
					diffType = serializer.DiffModified
				default:
					diffType = serializer.DiffDeleted
				}

				result = append(result, serializer.DiffEntry{
					Path:           entry.Path,
					Type:           diffType,
					NewSize:        entry.NewSize,
					NewModTime:     entry.NewModTime,
					IsDir:          entry.IsDir,
					NewHash:        entry.NewHash,
					NewPermissions: entry.NewPermissions,
				})
			}
		}
	}
	return result, nil
}

// Close implements the Store interface
func (f *fakeStoreAdapter) Close() error {
	return nil
}

// FindFilesBySize implements the Store interface
func (f *fakeStoreAdapter) FindFilesBySize(snapshotID string, minSize, maxSize int64) (map[string]metadata.FileMetadata, error) {
	// For testing, we'll just return an empty map
	return map[string]metadata.FileMetadata{}, nil
}

// FindFilesByModTime implements the Store interface
func (f *fakeStoreAdapter) FindFilesByModTime(snapshotID string, startTime, endTime time.Time) (map[string]metadata.FileMetadata, error) {
	// For testing, we'll just return an empty map
	return map[string]metadata.FileMetadata{}, nil
}

// FindFilesByHash implements the Store interface
func (f *fakeStoreAdapter) FindFilesByHash(snapshotID, hash string) (map[string]metadata.FileMetadata, error) {
	// For testing, we'll just return an empty map
	return map[string]metadata.FileMetadata{}, nil
}
