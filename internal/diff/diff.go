package diff

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/flashfs/internal/hash"
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
	// Additional fields for detailed diff information
	OldSize        int64
	NewSize        int64
	OldModTime     int64
	NewModTime     int64
	OldPermissions uint32
	NewPermissions uint32
	HashDiff       bool // True if the hash differs but size and mtime are the same
}

// String returns a string representation of a DiffEntry
func (d DiffEntry) String() string {
	return fmt.Sprintf("%s: %s", d.Type, d.Path)
}

// DetailedString returns a detailed string representation of a DiffEntry
func (d DiffEntry) DetailedString() string {
	switch d.Type {
	case "New":
		return fmt.Sprintf("New: %s (size: %d, mtime: %d)", d.Path, d.NewSize, d.NewModTime)
	case "Deleted":
		return fmt.Sprintf("Deleted: %s (size: %d, mtime: %d)", d.Path, d.OldSize, d.OldModTime)
	case "Modified":
		details := []string{}
		if d.OldSize != d.NewSize {
			details = append(details, fmt.Sprintf("size: %d → %d", d.OldSize, d.NewSize))
		}
		if d.OldModTime != d.NewModTime {
			details = append(details, fmt.Sprintf("mtime: %d → %d", d.OldModTime, d.NewModTime))
		}
		if d.OldPermissions != d.NewPermissions {
			details = append(details, fmt.Sprintf("permissions: %o → %o", d.OldPermissions, d.NewPermissions))
		}
		if d.HashDiff {
			details = append(details, "hash changed")
		}
		return fmt.Sprintf("Modified: %s (%s)", d.Path, strings.Join(details, ", "))
	default:
		return d.String()
	}
}

// DiffOptions contains options for controlling the diff operation
type DiffOptions struct {
	// Path prefix to filter entries (for partial diffs)
	PathPrefix string
	// Whether to use hash-based diffing
	UseHashDiff bool
	// Number of worker goroutines for parallel diffing (0 = auto)
	Workers int
	// Whether to use bloom filters for quick detection of changes
	UseBloomFilter bool
}

// DefaultDiffOptions returns the default diff options
func DefaultDiffOptions() DiffOptions {
	return DiffOptions{
		PathPrefix:     "",
		UseHashDiff:    true,
		Workers:        0, // Auto
		UseBloomFilter: true,
	}
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
		var size int64
		var modTime time.Time
		var isDir bool
		var permissions fs.FileMode
		var hashBytes []byte

		func() {
			defer func() {
				if r := recover(); r != nil {
					// Handle any panics from flatbuffers
					size = 0
					modTime = time.Time{}
					isDir = false
					permissions = 0
					hashBytes = nil
				}
			}()
			size = fe.Size()
			// Convert Unix timestamp to time.Time
			modTime = time.Unix(fe.Mtime(), 0)
			isDir = fe.IsDir()
			// Convert uint32 to fs.FileMode
			permissions = fs.FileMode(fe.Permissions())
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
			// Convert byte slice to hex string
			entry.Hash = fmt.Sprintf("%x", hashBytes)
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
	// Use default options
	options := DefaultDiffOptions()

	// Get detailed diffs
	diffs, err := CompareSnapshotsDetailedWithOptions(ctx, oldFile, newFile, options)
	if err != nil {
		return nil, err
	}

	// Convert to string format
	result := make([]string, len(diffs))
	for i, diff := range diffs {
		result[i] = diff.String()
	}

	return result, nil
}

// CompareSnapshotsDetailed returns detailed differences between two snapshots.
func CompareSnapshotsDetailed(ctx context.Context, oldFile, newFile string) ([]DiffEntry, error) {
	// Use default options
	options := DefaultDiffOptions()
	return CompareSnapshotsDetailedWithOptions(ctx, oldFile, newFile, options)
}

// CompareSnapshotsDetailedWithOptions returns detailed differences between two snapshots with custom options.
func CompareSnapshotsDetailedWithOptions(ctx context.Context, oldFile, newFile string, options DiffOptions) ([]DiffEntry, error) {
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

	// Apply path prefix filter if specified
	if options.PathPrefix != "" {
		filteredOldSnapshot := make(map[string]walker.SnapshotEntry)
		filteredNewSnapshot := make(map[string]walker.SnapshotEntry)

		for path, entry := range oldSnapshot {
			if strings.HasPrefix(path, options.PathPrefix) {
				filteredOldSnapshot[path] = entry
			}
		}

		for path, entry := range newSnapshot {
			if strings.HasPrefix(path, options.PathPrefix) {
				filteredNewSnapshot[path] = entry
			}
		}

		oldSnapshot = filteredOldSnapshot
		newSnapshot = filteredNewSnapshot
	}

	// Use bloom filter for quick detection if enabled
	if options.UseBloomFilter && len(oldSnapshot) > 1000 && len(newSnapshot) > 1000 {
		// Create bloom filters for both snapshots
		oldBloom := createBloomFilter(oldSnapshot, 0.01) // 1% false positive rate
		newBloom := createBloomFilter(newSnapshot, 0.01)

		// Quick check for unchanged files
		unchangedPaths := make(map[string]bool)
		for path := range oldSnapshot {
			if newBloom.Contains([]byte(path)) {
				unchangedPaths[path] = true
			}
		}

		for path := range newSnapshot {
			if !oldBloom.Contains([]byte(path)) {
				// Definitely new
				delete(unchangedPaths, path)
			}
		}

		// Remove definitely unchanged files from comparison
		for path := range unchangedPaths {
			// Only if size and mtime match (to avoid false positives)
			oldEntry := oldSnapshot[path]
			if newEntry, exists := newSnapshot[path]; exists {
				if oldEntry.Size == newEntry.Size &&
					oldEntry.ModTime == newEntry.ModTime &&
					oldEntry.Permissions == newEntry.Permissions &&
					!options.UseHashDiff {
					// Skip this file in detailed comparison
					delete(oldSnapshot, path)
					delete(newSnapshot, path)
				}
			}
		}
	}

	// Determine number of workers
	workers := options.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	// Pre-allocate diffs slice with a reasonable capacity
	estimatedDiffCount := max(len(oldSnapshot)/10, 10) // Assume ~10% of files changed
	var diffs []DiffEntry
	var diffsMutex sync.Mutex

	// Parallel processing for modified files
	if workers > 1 && (len(oldSnapshot) > 100 || len(newSnapshot) > 100) {
		// Create a channel for paths to process
		pathChan := make(chan string, min(1000, max(len(oldSnapshot), len(newSnapshot))))

		// Start worker goroutines
		var workerWg sync.WaitGroup
		for i := 0; i < workers; i++ {
			workerWg.Add(1)
			go func() {
				defer workerWg.Done()

				// Process paths from the channel
				for path := range pathChan {
					// Check for context cancellation
					select {
					case <-ctx.Done():
						return
					default:
					}

					// Check if the path exists in both snapshots
					oldEntry, oldExists := oldSnapshot[path]
					newEntry, newExists := newSnapshot[path]

					var diff DiffEntry

					if oldExists && newExists {
						// Check if the file was modified
						if oldEntry.Size != newEntry.Size ||
							oldEntry.ModTime != newEntry.ModTime ||
							oldEntry.Permissions != newEntry.Permissions ||
							(options.UseHashDiff && !hashesEqual(oldEntry.Hash, newEntry.Hash)) {
							diff = DiffEntry{
								Type:           "Modified",
								Path:           path,
								OldSize:        oldEntry.Size,
								NewSize:        newEntry.Size,
								OldModTime:     oldEntry.ModTime.Unix(),
								NewModTime:     newEntry.ModTime.Unix(),
								OldPermissions: uint32(oldEntry.Permissions),
								NewPermissions: uint32(newEntry.Permissions),
								HashDiff:       !hashesEqual(oldEntry.Hash, newEntry.Hash),
							}
						}
					} else if oldExists {
						// File was deleted
						diff = DiffEntry{
							Type:           "Deleted",
							Path:           path,
							OldSize:        oldEntry.Size,
							OldModTime:     oldEntry.ModTime.Unix(),
							OldPermissions: uint32(oldEntry.Permissions),
						}
					} else if newExists {
						// File is new
						diff = DiffEntry{
							Type:           "New",
							Path:           path,
							NewSize:        newEntry.Size,
							NewModTime:     newEntry.ModTime.Unix(),
							NewPermissions: uint32(newEntry.Permissions),
						}
					}

					// Add the diff if it's valid
					if diff.Type != "" {
						diffsMutex.Lock()
						diffs = append(diffs, diff)
						diffsMutex.Unlock()
					}
				}
			}()
		}

		// Collect all paths from both snapshots
		allPaths := make(map[string]bool)
		for path := range oldSnapshot {
			allPaths[path] = true
		}
		for path := range newSnapshot {
			allPaths[path] = true
		}

		// Send paths to workers
		for path := range allPaths {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				close(pathChan)
				return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
			default:
				pathChan <- path
			}
		}

		// Close the channel and wait for workers to finish
		close(pathChan)
		workerWg.Wait()

	} else {
		// Sequential processing for smaller snapshots
		diffs = make([]DiffEntry, 0, estimatedDiffCount)

		// Check for new or modified files
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
				// Check if the file was modified
				if oldEntry.Size != newEntry.Size ||
					oldEntry.ModTime != newEntry.ModTime ||
					oldEntry.Permissions != newEntry.Permissions ||
					(options.UseHashDiff && !hashesEqual(oldEntry.Hash, newEntry.Hash)) {
					diffs = append(diffs, DiffEntry{
						Type:           "Modified",
						Path:           path,
						OldSize:        oldEntry.Size,
						NewSize:        newEntry.Size,
						OldModTime:     oldEntry.ModTime.Unix(),
						NewModTime:     newEntry.ModTime.Unix(),
						OldPermissions: uint32(oldEntry.Permissions),
						NewPermissions: uint32(newEntry.Permissions),
						HashDiff:       !hashesEqual(oldEntry.Hash, newEntry.Hash),
					})
				}
			} else {
				// File is new
				diffs = append(diffs, DiffEntry{
					Type:           "New",
					Path:           path,
					NewSize:        newEntry.Size,
					NewModTime:     newEntry.ModTime.Unix(),
					NewPermissions: uint32(newEntry.Permissions),
				})
			}
		}

		// Check for deleted files
		for path, oldEntry := range oldSnapshot {
			// Check for context cancellation periodically
			if len(diffs) > 0 && len(diffs)%1000 == 0 {
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("%w: %v", ErrOperationCanceled, ctx.Err())
				default:
				}
			}

			if _, exists := newSnapshot[path]; !exists {
				// File was deleted
				diffs = append(diffs, DiffEntry{
					Type:           "Deleted",
					Path:           path,
					OldSize:        oldEntry.Size,
					OldModTime:     oldEntry.ModTime.Unix(),
					OldPermissions: uint32(oldEntry.Permissions),
				})
			}
		}
	}

	return diffs, nil
}

// bytesEqual compares two byte slices for equality
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// hashesEqual compares two hash strings for equality
func hashesEqual(a, b string) bool {
	return hash.Equal(a, b)
}

// BloomFilter is a simple bloom filter implementation
type BloomFilter struct {
	bits    []byte
	numHash uint
}

// createBloomFilter creates a bloom filter from a snapshot
func createBloomFilter(snapshot map[string]walker.SnapshotEntry, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal size and number of hash functions
	n := len(snapshot)
	if n == 0 {
		n = 1 // Avoid division by zero
	}

	// Ensure falsePositiveRate is reasonable
	if falsePositiveRate <= 0 {
		falsePositiveRate = 0.01 // Default to 1% false positive rate
	}
	if falsePositiveRate >= 1 {
		falsePositiveRate = 0.99 // Cap at 99% false positive rate
	}

	// Calculate optimal size: m = -n*ln(p)/(ln(2)^2)
	// Use a safer calculation method
	m := int(math.Ceil(-float64(n) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2))))

	// Apply reasonable bounds
	if m < 1024 {
		m = 1024 // Minimum size
	}
	if m > 100000000 { // 100MB max
		m = 100000000
	}

	// Calculate optimal number of hash functions: k = m/n * ln(2)
	k := uint(math.Ceil(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30 // Maximum number of hash functions
	}

	// Create the bloom filter with a safe size calculation
	bitsLen := (m + 7) / 8 // Round up to nearest byte
	if bitsLen <= 0 {
		bitsLen = 1 // Ensure at least 1 byte
	}

	bf := &BloomFilter{
		bits:    make([]byte, bitsLen),
		numHash: k,
	}

	// Add all paths to the bloom filter
	for path := range snapshot {
		bf.Add([]byte(path))
	}

	return bf
}

// Add adds an item to the bloom filter
func (bf *BloomFilter) Add(data []byte) {
	// Use double hashing to simulate multiple hash functions
	h1 := hash1(data)
	h2 := hash2(data)

	for i := uint(0); i < bf.numHash; i++ {
		// Combine the two hash functions to create k hash functions
		// h_i(x) = h1(x) + i*h2(x)
		h := (h1 + i*h2) % uint(len(bf.bits)*8)
		bf.bits[h/8] |= 1 << (h % 8)
	}
}

// Contains checks if an item might be in the bloom filter
func (bf *BloomFilter) Contains(data []byte) bool {
	h1 := hash1(data)
	h2 := hash2(data)

	for i := uint(0); i < bf.numHash; i++ {
		h := (h1 + i*h2) % uint(len(bf.bits)*8)
		if bf.bits[h/8]&(1<<(h%8)) == 0 {
			return false
		}
	}

	return true
}

// Simple hash functions for the bloom filter
func hash1(data []byte) uint {
	h := uint(0)
	for _, b := range data {
		h = h*31 + uint(b)
	}
	return h
}

func hash2(data []byte) uint {
	h := uint(0)
	for _, b := range data {
		h = h*37 + uint(b)
	}
	if h == 0 {
		return 1 // h2 must be non-zero
	}
	return h
}

// Helper functions for Go versions before 1.21
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
