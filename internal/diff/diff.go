package diff

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/TFMV/flashfs/internal/walker"
	flashfs "github.com/TFMV/flashfs/schema/flashfs"
)

// Common errors
var (
	// ErrInvalidSnapshot is returned when a snapshot file is invalid
	ErrInvalidSnapshot = errors.New("invalid snapshot file")
	// ErrSnapshotNotFound is returned when a snapshot file is not found
	ErrSnapshotNotFound = errors.New("snapshot file not found")
	// ErrOperationCanceled is returned when an operation is canceled
	ErrOperationCanceled = errors.New("operation canceled")
)

// DiffEntry represents a single difference between snapshots
type DiffEntry struct {
	Type string // "New", "Modified", "Deleted"
	Path string
}

// String returns a string representation of a DiffEntry
func (d DiffEntry) String() string {
	return fmt.Sprintf("%s: %s", d.Type, d.Path)
}

// loadSnapshot reads and deserializes a snapshot file into a map keyed by file path.
func loadSnapshot(ctx context.Context, filename string) (map[string]walker.SnapshotEntry, error) {
	// Validate file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, filename)
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
	default:
	}

	// Read file with absolute path
	absPath, err := filepath.Abs(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot file: %w", err)
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
	default:
	}

	// Validate snapshot data
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty file", ErrInvalidSnapshot)
	}

	// Safely access the FlatBuffers data
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: FlatBuffers parsing error: %v", ErrInvalidSnapshot, r)
		}
	}()

	snapshot := flashfs.GetRootAsSnapshot(data, 0)
	if snapshot == nil {
		return nil, fmt.Errorf("%w: could not parse snapshot", ErrInvalidSnapshot)
	}

	result := make(map[string]walker.SnapshotEntry)

	// Safely get entries length
	var entriesLen int
	func() {
		defer func() {
			if r := recover(); r != nil {
				entriesLen = 0
			}
		}()
		entriesLen = snapshot.EntriesLength()
	}()

	if entriesLen == 0 {
		// Empty snapshot is valid, just return empty map
		return result, nil
	}

	// Pre-allocate entry for reuse
	var fe flashfs.FileEntry

	for i := 0; i < entriesLen; i++ {
		// Check for context cancellation periodically
		if i > 0 && i%1000 == 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
			default:
			}
		}

		// Safely access entry
		var entryValid bool
		func() {
			defer func() {
				if r := recover(); r != nil {
					entryValid = false
				}
			}()
			entryValid = snapshot.Entries(&fe, i)
		}()

		if !entryValid {
			continue // Skip invalid entries
		}

		// Safely get path
		var path string
		func() {
			defer func() {
				if r := recover(); r != nil {
					path = ""
				}
			}()
			path = string(fe.Path())
		}()

		if path == "" {
			continue // Skip entries with empty paths
		}

		// Safely get other fields
		var size, modTime int64
		var isDir bool
		var permissions uint32
		var hashBytes []byte

		func() {
			defer func() {
				if r := recover(); r != nil {
					// Use defaults on error
					size = 0
					modTime = 0
					isDir = false
					permissions = 0
					hashBytes = nil
				}
			}()
			size = fe.Size()
			modTime = fe.Mtime()
			isDir = fe.IsDir()
			permissions = fe.Permissions()
			hashBytes = fe.HashBytes()
		}()

		entry := walker.SnapshotEntry{
			Path:        path,
			Size:        size,
			ModTime:     modTime,
			IsDir:       isDir,
			Permissions: permissions,
		}

		if len(hashBytes) > 0 {
			entry.Hash = hashBytes
		}

		result[entry.Path] = entry
	}

	return result, nil
}

// CompareSnapshots returns a list of differences between two snapshots.
func CompareSnapshots(oldFile, newFile string) ([]string, error) {
	return CompareSnapshotsWithContext(context.Background(), oldFile, newFile)
}

// CompareSnapshotsWithContext returns a list of differences between two snapshots with context support.
func CompareSnapshotsWithContext(ctx context.Context, oldFile, newFile string) ([]string, error) {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
	default:
	}

	// Load snapshots concurrently
	var wg sync.WaitGroup
	var oldSnapshot, newSnapshot map[string]walker.SnapshotEntry
	var oldErr, newErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		oldSnapshot, oldErr = loadSnapshot(ctx, oldFile)
	}()

	go func() {
		defer wg.Done()
		newSnapshot, newErr = loadSnapshot(ctx, newFile)
	}()

	wg.Wait()

	// Check for errors
	if oldErr != nil {
		return nil, fmt.Errorf("error loading old snapshot: %w", oldErr)
	}
	if newErr != nil {
		return nil, fmt.Errorf("error loading new snapshot: %w", newErr)
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
	default:
	}

	// Pre-allocate diffs slice with a reasonable capacity
	estimatedDiffCount := max(len(oldSnapshot)/10, 10) // Assume ~10% of files changed
	diffs := make([]string, 0, estimatedDiffCount)

	// Check for new or modified files.
	for path, newEntry := range newSnapshot {
		// Check for context cancellation periodically
		if len(diffs) > 0 && len(diffs)%1000 == 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
			default:
			}
		}

		if oldEntry, exists := oldSnapshot[path]; exists {
			if newEntry.Size != oldEntry.Size ||
				newEntry.ModTime != oldEntry.ModTime ||
				newEntry.Permissions != oldEntry.Permissions {
				diffs = append(diffs, fmt.Sprintf("Modified: %s", path))
			}
		} else {
			diffs = append(diffs, fmt.Sprintf("New: %s", path))
		}
	}

	// Check for deleted files.
	for path := range oldSnapshot {
		// Check for context cancellation periodically
		if len(diffs) > 0 && len(diffs)%1000 == 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
			default:
			}
		}

		if _, exists := newSnapshot[path]; !exists {
			diffs = append(diffs, fmt.Sprintf("Deleted: %s", path))
		}
	}

	return diffs, nil
}

// CompareSnapshotsDetailed returns detailed differences between two snapshots.
func CompareSnapshotsDetailed(ctx context.Context, oldFile, newFile string) ([]DiffEntry, error) {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
	default:
	}

	// Load snapshots concurrently
	var wg sync.WaitGroup
	var oldSnapshot, newSnapshot map[string]walker.SnapshotEntry
	var oldErr, newErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		oldSnapshot, oldErr = loadSnapshot(ctx, oldFile)
	}()

	go func() {
		defer wg.Done()
		newSnapshot, newErr = loadSnapshot(ctx, newFile)
	}()

	wg.Wait()

	// Check for errors
	if oldErr != nil {
		return nil, fmt.Errorf("error loading old snapshot: %w", oldErr)
	}
	if newErr != nil {
		return nil, fmt.Errorf("error loading new snapshot: %w", newErr)
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
	default:
	}

	// Pre-allocate diffs slice with a reasonable capacity
	estimatedDiffCount := max(len(oldSnapshot)/10, 10) // Assume ~10% of files changed
	diffs := make([]DiffEntry, 0, estimatedDiffCount)

	// Check for new or modified files.
	for path, newEntry := range newSnapshot {
		// Check for context cancellation periodically
		if len(diffs) > 0 && len(diffs)%1000 == 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
			default:
			}
		}

		if oldEntry, exists := oldSnapshot[path]; exists {
			if newEntry.Size != oldEntry.Size ||
				newEntry.ModTime != oldEntry.ModTime ||
				newEntry.Permissions != oldEntry.Permissions {
				diffs = append(diffs, DiffEntry{Type: "Modified", Path: path})
			}
		} else {
			diffs = append(diffs, DiffEntry{Type: "New", Path: path})
		}
	}

	// Check for deleted files.
	for path := range oldSnapshot {
		// Check for context cancellation periodically
		if len(diffs) > 0 && len(diffs)%1000 == 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
			default:
			}
		}

		if _, exists := newSnapshot[path]; !exists {
			diffs = append(diffs, DiffEntry{Type: "Deleted", Path: path})
		}
	}

	return diffs, nil
}

// Helper function for Go versions before 1.21
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
