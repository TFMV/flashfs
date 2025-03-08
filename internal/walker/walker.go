package walker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	hasher "github.com/TFMV/flashfs/internal/hash"
)

// WalkOptions contains options for the Walk function.
type WalkOptions struct {
	// ComputeHashes determines whether file hashes should be computed.
	ComputeHashes bool
	// FollowSymlinks determines whether symbolic links should be followed.
	FollowSymlinks bool
	// MaxDepth is the maximum directory depth to traverse (0 means no limit).
	MaxDepth int
	// NumWorkers specifies the number of worker goroutines for parallel processing.
	NumWorkers int
	// HashAlgorithm specifies the algorithm to be used like "BLAKE3", "MD5" etc
	HashAlgorithm string
	// SkipErrors specifies that errors should not cancel operations
	SkipErrors bool
	// ExcludePatterns is a list of filepath.Match patterns to exclude from the walk.
	ExcludePatterns []string
	// UsePartialHashing enables partial hashing for large files.
	UsePartialHashing bool
	// PartialHashingThreshold is the file size threshold in bytes above which
	// partial hashing will be used (if enabled). Default is 10MB.
	PartialHashingThreshold int64
}

// DefaultWalkOptions returns the default options for Walk.
func DefaultWalkOptions() WalkOptions {
	return WalkOptions{
		ComputeHashes:           true,
		FollowSymlinks:          false,
		MaxDepth:                0,
		NumWorkers:              runtime.NumCPU(),
		HashAlgorithm:           "BLAKE3",
		SkipErrors:              false,
		ExcludePatterns:         nil,
		UsePartialHashing:       true,
		PartialHashingThreshold: 10 * 1024 * 1024, // 10MB
	}
}

// SnapshotEntry represents file metadata to be recorded.
type SnapshotEntry struct {
	Path          string      // Relative path from the root
	Size          int64       // Size in bytes
	ModTime       time.Time   // Modification time
	IsDir         bool        // True if it's a directory
	Permissions   fs.FileMode // File permissions
	Hash          string      // Hash of the file content (if computed)
	SymlinkTarget string      // Target of the symlink (if it's a symlink)
	Error         error
}

// ErrMaxDepthReached is returned when the maximum depth is reached.
var ErrMaxDepthReached = errors.New("maximum depth reached")

// computeHash calculates the hash of a file using the specified algorithm.
func computeHash(path string, algorithm string, usePartial bool, threshold int64) (string, error) {
	var alg hasher.Algorithm
	switch algorithm {
	case "BLAKE3":
		alg = hasher.BLAKE3
	case "MD5":
		alg = hasher.MD5
	case "SHA1":
		alg = hasher.SHA1
	case "SHA256":
		alg = hasher.SHA256
	default:
		return "", fmt.Errorf("unsupported hash algorithm: %s", algorithm)
	}

	opts := hasher.DefaultOptions()
	opts.Algorithm = alg
	opts.UsePartialHashing = usePartial
	if threshold > 0 {
		opts.PartialHashingThreshold = threshold
	}

	result := hasher.File(path, opts)
	if result.Error != nil {
		return "", result.Error
	}

	return result.Hash, nil
}

// validateRoot checks if the root exists and is a directory.
func validateRoot(root string) (string, fs.FileInfo, error) {
	info, err := os.Stat(root)
	if err != nil {
		return "", nil, err
	}
	if !info.IsDir() {
		return "", nil, fmt.Errorf("%s is not a directory", root)
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", nil, err
	}
	return absRoot, info, nil
}

// processEntry handles a single file or directory entry.
func processEntry(ctx context.Context, absRoot, path string, d fs.DirEntry, options WalkOptions, visited *sync.Map, entryCount *int64) (*SnapshotEntry, error) {
	// Check for context cancellation.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Check against exclude patterns.
	relPath, _ := filepath.Rel(absRoot, path) // Error handled in caller
	for _, pattern := range options.ExcludePatterns {
		matched, _ := filepath.Match(pattern, relPath) // Ignore error, treat as not matched
		if matched {
			if d.IsDir() {
				return nil, filepath.SkipDir // Skip entire directory if it matches
			}
			return nil, nil // Skip individual file
		}
		matched, _ = filepath.Match(pattern, filepath.Base(relPath)) // Ignore error and handle
		if matched {
			if d.IsDir() {
				return nil, filepath.SkipDir // Skip entire directory if it matches
			}
			return nil, nil // Skip individual file
		}
	}

	info, err := d.Info()
	if err != nil {
		return nil, err // Return the actual error
	}

	// Handle symlinks.
	var symlinkTarget string
	if info.Mode()&os.ModeSymlink != 0 {
		if !options.FollowSymlinks {
			symlinkTarget, _ = os.Readlink(path) // Capture the symlink target
			// We *don't* skip here; we want to record the symlink itself
		} else {
			realPath, err := filepath.EvalSymlinks(path)
			if err != nil {
				return &SnapshotEntry{Error: err}, err // Keep the entry in the results
			}
			info, err = os.Stat(realPath) // Get info of the *target*
			if err != nil {
				return nil, err
			}
			if info.IsDir() {

				absRealPath, err := filepath.Abs(realPath)
				if err != nil {
					return nil, err
				}

				// Cycle detection. Use sync.Map for concurrent access.
				if _, loaded := visited.LoadOrStore(absRealPath, true); loaded {
					return nil, filepath.SkipDir // Skip this directory
				}
			}

			path = realPath                          // Continue processing with the *real* path
			relPath, _ = filepath.Rel(absRoot, path) // Recalculate relPath
		}
	}

	if d.IsDir() && !options.FollowSymlinks { // add to visited *only if its a real directory *
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		visited.Store(absPath, true)
	}

	// Compute hash if needed.
	var hash string
	if options.ComputeHashes && !info.IsDir() {
		hash, err = computeHash(path, options.HashAlgorithm, options.UsePartialHashing, options.PartialHashingThreshold)
		if err != nil && options.SkipErrors {
			return &SnapshotEntry{Error: err}, err // Keep the entry in the results
		} else if err != nil {
			return nil, err
		}
	}

	atomic.AddInt64(entryCount, 1)

	return &SnapshotEntry{
		Path:          relPath,
		Size:          info.Size(),
		ModTime:       info.ModTime(),
		IsDir:         info.IsDir(),
		Permissions:   info.Mode().Perm(),
		Hash:          hash,
		SymlinkTarget: symlinkTarget, // Will be "" if not a symlink
	}, nil
}

// WalkWithOptions performs a directory traversal with configurable options and parallelism.
func WalkWithOptions(ctx context.Context, root string, options WalkOptions) ([]SnapshotEntry, error) {
	absRoot, rootInfo, err := validateRoot(root)
	if err != nil {
		return nil, err
	}

	// Set default number of workers if not specified
	if options.NumWorkers <= 0 {
		options.NumWorkers = runtime.NumCPU()
	}

	// Channels for communication.
	entryChan := make(chan entryInfo, options.NumWorkers*2)      // Buffered channel
	resultChan := make(chan SnapshotEntry, options.NumWorkers*2) // Buffered channel
	errorChan := make(chan error, options.NumWorkers)            // Buffered channel for collecting errors
	done := make(chan struct{})

	// Use sync.Map for visited directories (concurrent-safe).
	var visited sync.Map
	visited.Store(absRoot, true) // Mark root as visited

	var entryCount int64 = 0 // Counter for statistics

	// Initialize root entry.
	rootEntry := SnapshotEntry{
		Path:        ".",
		Size:        rootInfo.Size(),
		ModTime:     rootInfo.ModTime(),
		IsDir:       true,
		Permissions: rootInfo.Mode().Perm(),
	}

	// Add the root directory to result.
	results := make([]SnapshotEntry, 0)
	results = append(results, rootEntry)

	// Start worker pool.
	var wg sync.WaitGroup
	for i := 0; i < options.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range entryChan {
				relPath, _ := filepath.Rel(absRoot, entry.path)
				if options.MaxDepth > 0 {
					depth := 0
					for _, c := range relPath {
						if c == filepath.Separator {
							depth++
						}
					}
					if depth > options.MaxDepth { //check depth here too
						if entry.d.IsDir() { //Return if max depth directory to skip
							entry.err = filepath.SkipDir
						} else {
							continue
						}
					}
				}

				snapshotEntry, err := processEntry(ctx, absRoot, entry.path, entry.d, options, &visited, &entryCount)
				if err != nil {
					if errors.Is(err, filepath.SkipDir) {
						if entry.err == nil { //Only set if processentry did not report another error first
							entry.err = err
						}
					} else if !options.SkipErrors {
						select { //Non blocking send
						case errorChan <- err:
						default:
						}
						continue // Skip this entry and continue processing
					} else { //if options.SkipErrors

						resultChan <- *snapshotEntry //Send even with error
					}
				}

				if snapshotEntry != nil && snapshotEntry.Error == nil { //Only send real entries
					resultChan <- *snapshotEntry
				}

				if entry.err != nil { //return directory SkipDir back up the chain
					entry.done <- entry.err
				}
			}
		}()
	}

	// WalkDir closure.
	go func() {
		defer close(entryChan)
		defer close(done)

		err := filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, err error) error {
			if path == absRoot { //skip root
				return nil
			}
			//If there are errors, pass to entry and if skip errors is enabled keep going
			if err != nil && options.SkipErrors {
				entryChan <- entryInfo{path, d, err, make(chan error, 1)} // make sure we submit errors too
				return nil
			} else if err != nil { //If there are errors and not skipping errors, return error
				return err
			}
			// Create a channel to receive any SkipDir signal from the worker
			doneChan := make(chan error, 1)

			select {
			case <-ctx.Done():
				return ctx.Err() // Check context here too.
			case entryChan <- entryInfo{path, d, nil, doneChan}: // Send entry to worker.
				select { //check for donChan signal which is SkipDir set from either depth of procEntry
				case skipErr := <-doneChan:
					return skipErr //report back up chain
				default:
					return nil
				}
			}

		})
		if err != nil { //Catch and report errors
			select { // Non-blocking send on errorChan
			case errorChan <- fmt.Errorf("WalkDir error: %w", err):
			default:
			}

		}
	}()

	go func() {
		wg.Wait()         // Wait for all workers to finish.
		close(resultChan) // Close resultChan after all workers are done.
		close(errorChan)  // Close errorChan after all workers are done.

	}()

	// Collect results.  Append to pre-allocated slice in main goroutine.
	for entry := range resultChan {
		results = append(results, entry)
	}

	// Check for any errors.
	for err := range errorChan {
		if err != nil {
			return results, err // Return the first error encountered.
		}
	}

	<-done // Wait for WalkDir to complete.

	return results, nil
}

// Walk performs a directory traversal with default options.
func Walk(root string) ([]SnapshotEntry, error) {
	return WalkWithOptions(context.Background(), root, DefaultWalkOptions())
}

// WalkWithContext performs a directory traversal with context support and default options.
func WalkWithContext(ctx context.Context, root string) ([]SnapshotEntry, error) {
	return WalkWithOptions(ctx, root, DefaultWalkOptions())
}

type entryInfo struct {
	path string
	d    fs.DirEntry
	err  error
	done chan error
}
