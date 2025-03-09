package query

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/TFMV/flashfs/internal/metadata"
	"github.com/TFMV/flashfs/internal/serializer"
	"github.com/TFMV/flashfs/internal/walker"
)

// Store represents the interface required by the query engine.
type Store interface {
	// Core methods needed by the query engine
	GetSnapshot(id string) ([]walker.SnapshotEntry, error)
	ListSnapshots() ([]metadata.SnapshotMetadata, error)
	ComputeDiff(oldID, newID string) ([]serializer.DiffEntry, error)
	FindFilesByPattern(snapshotID, pattern string) (map[string]metadata.FileMetadata, error)
	FindFilesBySize(snapshotID string, minSize, maxSize int64) (map[string]metadata.FileMetadata, error)
	FindFilesByModTime(snapshotID string, startTime, endTime time.Time) (map[string]metadata.FileMetadata, error)
	FindFilesByHash(snapshotID, hash string) (map[string]metadata.FileMetadata, error)
	Close() error
}

// QueryOptions represents options for querying snapshots.
type QueryOptions struct {
	// Pattern is a filepath.Match pattern to filter files by path.
	Pattern string
	// MinSize is the minimum file size in bytes.
	MinSize int64
	// MaxSize is the maximum file size in bytes (0 means no maximum).
	MaxSize int64
	// StartTime is the start of the modification time range.
	StartTime time.Time
	// EndTime is the end of the modification time range.
	EndTime time.Time
	// Hash is a specific hash to match.
	Hash string
	// IsDir filters for directories (true) or files (false), nil means both.
	IsDir *bool
	// IncludeHot determines whether to include results from the hot layer.
	IncludeHot bool
	// IncludeCold determines whether to include results from the cold layer.
	IncludeCold bool
}

// DefaultQueryOptions returns the default query options.
func DefaultQueryOptions() QueryOptions {
	return QueryOptions{
		Pattern:     "*",
		MinSize:     0,
		MaxSize:     0,
		IncludeHot:  true,
		IncludeCold: true,
	}
}

// FileResult represents a file found in a query.
type FileResult struct {
	Path          string
	Size          int64
	ModTime       time.Time
	IsDir         bool
	Permissions   uint32
	Hash          string
	SnapshotID    string
	FromHotLayer  bool
	DataRef       string // For hot layer data location
	SymlinkTarget string
}

// QueryEngine provides a unified interface for querying both hot and cold layers.
type QueryEngine struct {
	store Store
}

// NewQueryEngine creates a new query engine.
func NewQueryEngine(store Store) *QueryEngine {
	return &QueryEngine{
		store: store,
	}
}

// QuerySnapshot queries a snapshot using the specified options.  It parallelizes
// queries to the hot and cold layers.
func (q *QueryEngine) QuerySnapshot(snapshotID string, options QueryOptions) ([]FileResult, error) {
	var wg sync.WaitGroup
	var hotResults, coldResults []FileResult
	var hotErr, coldErr error

	// For testing purposes, we'll assume hot layer is enabled
	// In a real implementation, this would be determined by the store
	hotLayerEnabled := true

	// Query hot layer if enabled and requested
	if options.IncludeHot && hotLayerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hotResults, hotErr = q.queryHotLayer(snapshotID, options)
		}()
	}

	// Query cold layer if requested
	if options.IncludeCold {
		wg.Add(1)
		go func() {
			defer wg.Done()
			coldResults, coldErr = q.queryColdLayer(snapshotID, options)
		}()
	}

	// Wait for both queries to complete
	wg.Wait()

	// Check for errors
	if hotErr != nil {
		return nil, fmt.Errorf("error querying hot layer: %w", hotErr)
	}
	if coldErr != nil {
		return nil, fmt.Errorf("error querying cold layer: %w", coldErr)
	}

	// Combine results
	results := append(hotResults, coldResults...)
	return results, nil
}

// queryHotLayer queries the hot layer for files matching the options.
func (q *QueryEngine) queryHotLayer(snapshotID string, options QueryOptions) ([]FileResult, error) {
	results := make([]FileResult, 0)

	// Check if snapshot exists
	// We'll try to get the snapshot directly
	_, err := q.store.GetSnapshot(snapshotID)
	if err != nil {
		return nil, fmt.Errorf("snapshot not found: %w", err)
	}

	files, err := q.store.FindFilesByPattern(snapshotID, options.Pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to find files by pattern: %w", err)
	}

	for path, file := range files {
		// Convert to FileResult for consistent filtering
		fileResult := FileResult{
			Path:    path,
			Size:    file.Size,
			ModTime: time.Unix(file.ModTime, 0),
			IsDir:   file.IsDir,
			Hash:    file.Hash,
		}

		if !q.matchFile(fileResult, options) {
			continue
		}

		results = append(results, FileResult{
			Path:          path,
			Size:          file.Size,
			ModTime:       time.Unix(file.ModTime, 0),
			IsDir:         file.IsDir,
			Permissions:   file.Permissions,
			Hash:          file.Hash,
			SnapshotID:    snapshotID,
			FromHotLayer:  true,
			DataRef:       file.DataRef,
			SymlinkTarget: file.SymlinkTarget,
		})
	}
	return results, nil
}

// queryColdLayer queries the cold layer (FlatBuffers) for files matching the options.
func (q *QueryEngine) queryColdLayer(snapshotID string, options QueryOptions) ([]FileResult, error) {
	entries, err := q.store.GetSnapshot(snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err) // More context
	}

	results := make([]FileResult, 0) //, len(entries)) // Potential Optimization: pre-allocate based on number of entries.
	for _, entry := range entries {
		matched, err := filepath.Match(options.Pattern, entry.Path)
		if err != nil {
			return nil, fmt.Errorf("invalid pattern: %w", err) // Fail fast on bad pattern.
		}
		if !matched {
			continue
		}

		if !q.matchFile(FileResult{ // Convert to FileResult to use common matching logic.
			Path:    entry.Path,
			Size:    entry.Size,
			ModTime: entry.ModTime,
			IsDir:   entry.IsDir,
			Hash:    entry.Hash,
		}, options) {
			continue
		}

		results = append(results, FileResult{
			Path:          entry.Path,
			Size:          entry.Size,
			ModTime:       entry.ModTime,
			IsDir:         entry.IsDir,
			Permissions:   uint32(entry.Permissions),
			Hash:          entry.Hash,
			SnapshotID:    snapshotID,
			FromHotLayer:  false,
			SymlinkTarget: entry.SymlinkTarget,
		})
	}

	return results, nil
}

// matchFile checks if a FileResult matches the QueryOptions.  This centralizes
// the filtering logic for both hot and cold layers.
func (q *QueryEngine) matchFile(file FileResult, options QueryOptions) bool {
	if file.Size < options.MinSize || (options.MaxSize > 0 && file.Size > options.MaxSize) {
		return false
	}
	if !options.StartTime.IsZero() && file.ModTime.Before(options.StartTime) {
		return false
	}
	if !options.EndTime.IsZero() && file.ModTime.After(options.EndTime) {
		return false
	}
	if options.Hash != "" && file.Hash != options.Hash {
		return false
	}
	if options.IsDir != nil && file.IsDir != *options.IsDir {
		return false
	}
	return true
}

// QueryMultipleSnapshots queries multiple snapshots. It parallelizes the queries
// for each snapshot.
func (q *QueryEngine) QueryMultipleSnapshots(snapshotIDs []string, options QueryOptions) ([]FileResult, error) {
	resultChannel := make(chan []FileResult)
	errorChannel := make(chan error)
	var wg sync.WaitGroup

	for _, id := range snapshotIDs {
		wg.Add(1)
		go func(snapshotID string) {
			defer wg.Done()
			results, err := q.QuerySnapshot(snapshotID, options)

			if err != nil {
				errorChannel <- fmt.Errorf("failed to query snapshot %s: %w", snapshotID, err)
				return
			}
			resultChannel <- results

		}(id)
	}

	go func() {
		wg.Wait()
		close(resultChannel)
		close(errorChannel)
	}()

	var allResults []FileResult
	var firstError error

	// Collect the results. Using two loops for cleaner error handling
	for results := range resultChannel {
		allResults = append(allResults, results...)
	}

	for err := range errorChannel {
		if firstError == nil {
			firstError = err
		}
	}

	return allResults, firstError // Return first error encountered, if any.

}

// QueryAllSnapshots queries all snapshots using the specified options.
func (q *QueryEngine) QueryAllSnapshots(options QueryOptions) ([]FileResult, error) {
	snapshots, err := q.store.ListSnapshots()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	snapshotIDs := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotIDs = append(snapshotIDs, snapshot.ID)
	}

	return q.QueryMultipleSnapshots(snapshotIDs, options)
}

// FindDuplicateFiles finds files with the same hash across snapshots.
func (q *QueryEngine) FindDuplicateFiles(snapshotIDs []string, options QueryOptions) (map[string][]FileResult, error) {
	results, err := q.QueryMultipleSnapshots(snapshotIDs, options)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}

	filesByHash := make(map[string][]FileResult)
	for _, result := range results {
		if result.Hash == "" {
			continue // Skip files without a hash
		}
		filesByHash[result.Hash] = append(filesByHash[result.Hash], result)
	}

	duplicates := make(map[string][]FileResult)
	for hash, files := range filesByHash {
		if len(files) > 1 {
			duplicates[hash] = files
		}
	}

	return duplicates, nil
}

// FindFilesChangedBetweenSnapshots finds files that changed between two snapshots.
func (q *QueryEngine) FindFilesChangedBetweenSnapshots(oldID, newID string, options QueryOptions) ([]FileResult, error) {
	diffEntries, err := q.store.ComputeDiff(oldID, newID)
	if err != nil {
		return nil, fmt.Errorf("failed to compute diff: %w", err)
	}

	results := make([]FileResult, 0, len(diffEntries)) // Pre-allocate
	for _, diff := range diffEntries {
		matched, err := filepath.Match(options.Pattern, diff.Path)
		if err != nil {
			return nil, fmt.Errorf("invalid pattern: %w", err)
		}
		if !matched {
			continue
		}

		// For modified and added files, use the new file properties.
		if diff.Type == serializer.DiffAdded || diff.Type == serializer.DiffModified {
			if !q.matchFile(FileResult{
				Path:    diff.Path,
				Size:    diff.NewSize,
				ModTime: diff.NewModTime,
				IsDir:   diff.IsDir,
				Hash:    diff.NewHash,
			}, options) {
				continue
			}
		}

		//Always add if it matches
		results = append(results, FileResult{
			Path:         diff.Path,
			Size:         diff.NewSize,
			ModTime:      diff.NewModTime,
			IsDir:        diff.IsDir,
			Permissions:  diff.NewPermissions,
			Hash:         diff.NewHash,
			SnapshotID:   newID,
			FromHotLayer: false, // Diff is always from the result of the diff which is inherently cold data
		})

	}

	return results, nil
}

// FindLargestFiles finds the N largest files in a snapshot.
func (q *QueryEngine) FindLargestFiles(snapshotID string, n int, options QueryOptions) ([]FileResult, error) {
	results, err := q.QuerySnapshot(snapshotID, options)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot: %w", err)
	}

	// Sort by size (descending) using Go's efficient sort package.
	sort.Slice(results, func(i, j int) bool {
		return results[i].Size > results[j].Size // Largest first
	})

	if n > 0 && n < len(results) {
		return results[:n], nil
	}
	return results, nil
}

// FindNewestFiles finds the N newest files in a snapshot.
func (q *QueryEngine) FindNewestFiles(snapshotID string, n int, options QueryOptions) ([]FileResult, error) {
	results, err := q.QuerySnapshot(snapshotID, options)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot: %w", err)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ModTime.After(results[j].ModTime) // Newest first
	})

	if n > 0 && n < len(results) {
		return results[:n], nil
	}
	return results, nil
}

// FindOldestFiles finds the N oldest files in a snapshot.
func (q *QueryEngine) FindOldestFiles(snapshotID string, n int, options QueryOptions) ([]FileResult, error) {
	results, err := q.QuerySnapshot(snapshotID, options)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot: %w", err)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ModTime.Before(results[j].ModTime) // Oldest first
	})

	if n > 0 && n < len(results) {
		return results[:n], nil
	}
	return results, nil
}
