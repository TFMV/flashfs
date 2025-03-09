package walker

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

// EntryCallback is a function that processes a SnapshotEntry.
// If it returns an error, the walk will be aborted.
type EntryCallback func(entry SnapshotEntry) error

// streamSnapshotEntry extends SnapshotEntry with fileInfo for internal use
type streamSnapshotEntry struct {
	SnapshotEntry
	fileInfo fs.FileInfo
}

// WalkStreamWithCallback walks the file tree rooted at root and calls the callback
// function for each file or directory in the tree, including root.
// It's similar to WalkWithOptions but streams entries as they're discovered
// rather than collecting them all before returning.
func WalkStreamWithCallback(ctx context.Context, root string, options WalkOptions, callback EntryCallback) error {
	// Validate and normalize the root path
	absRoot, rootInfo, err := validateRoot(root)
	if err != nil {
		return err
	}

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create an error group for managing goroutines
	eg, ctx := errgroup.WithContext(ctx)

	// Create a channel to receive entries from the walker
	entryChan := make(chan *streamSnapshotEntry, options.NumWorkers*2)

	// Create a map to track visited directories (to avoid cycles)
	visited := &sync.Map{}

	// Start a goroutine to close the channel when done
	eg.Go(func() error {
		defer close(entryChan)

		// Process the root directory
		rootEntry := &streamSnapshotEntry{
			SnapshotEntry: SnapshotEntry{
				Path:        ".",
				IsDir:       rootInfo.IsDir(),
				Size:        rootInfo.Size(),
				ModTime:     rootInfo.ModTime(),
				Permissions: rootInfo.Mode() & os.ModePerm,
			},
			fileInfo: rootInfo,
		}

		// Send the root entry to the channel
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entryChan <- rootEntry:
			// Entry was sent
		}

		// Walk the directory tree
		return filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, err error) error {
			// Skip the root directory as it's already processed
			if path == absRoot {
				return nil
			}

			// Check if the context is canceled
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Continue processing
			}

			// Check if we should skip this entry
			if err != nil {
				if options.SkipErrors {
					return nil
				}
				return err
			}

			// Get the relative path
			relPath, err := filepath.Rel(absRoot, path)
			if err != nil {
				if options.SkipErrors {
					return nil
				}
				return err
			}

			// Check if this path matches any exclude patterns
			for _, pattern := range options.ExcludePatterns {
				matched, err := filepath.Match(pattern, relPath)
				if err != nil {
					if options.SkipErrors {
						continue
					}
					return err
				}
				if matched {
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}

			// Check if we've reached the maximum depth
			if options.MaxDepth > 0 {
				depth := strings.Count(relPath, string(filepath.Separator)) + 1
				if depth > options.MaxDepth {
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}

			// Get file info
			info, err := d.Info()
			if err != nil {
				if options.SkipErrors {
					return nil
				}
				return err
			}

			// Create a snapshot entry
			entry := &streamSnapshotEntry{
				SnapshotEntry: SnapshotEntry{
					Path:        relPath,
					Size:        info.Size(),
					ModTime:     info.ModTime(),
					IsDir:       info.IsDir(),
					Permissions: info.Mode() & os.ModePerm,
				},
				fileInfo: info,
			}

			// Handle symlinks if requested
			if options.FollowSymlinks && (info.Mode()&os.ModeSymlink) != 0 {
				target, err := os.Readlink(path)
				if err != nil {
					if options.SkipErrors {
						return nil
					}
					return err
				}
				entry.SnapshotEntry.SymlinkTarget = target

				// Check if we've already visited this target to avoid cycles
				if _, visited := visited.LoadOrStore(target, true); visited {
					return nil
				}
			}

			// Compute hash for files if requested
			if options.ComputeHashes && !info.IsDir() {
				hash, err := computeHash(path, options.HashAlgorithm, options.UsePartialHashing, options.PartialHashingThreshold)
				if err != nil {
					if options.SkipErrors {
						entry.SnapshotEntry.Error = err
					} else {
						return err
					}
				}
				entry.SnapshotEntry.Hash = hash
			}

			// Send the entry to the channel
			select {
			case <-ctx.Done():
				return ctx.Err()
			case entryChan <- entry:
				// Entry was sent
			}

			return nil
		})
	})

	// Start a goroutine to process entries
	eg.Go(func() error {
		for entry := range entryChan {
			// Call the callback function with the entry
			if err := callback(entry.SnapshotEntry); err != nil {
				cancel() // Cancel the context to stop the walker
				return err
			}
		}
		return nil
	})

	// Wait for all goroutines to complete
	return eg.Wait()
}

// WalkWithCallback is a deprecated alias for WalkStreamWithCallback.
// It will be removed in a future version.
//
// Deprecated: Use WalkStreamWithCallback instead.
func WalkWithCallback(ctx context.Context, root string, options WalkOptions, callback EntryCallback) error {
	return WalkStreamWithCallback(ctx, root, options, callback)
}

// WalkStreamWithOptions walks the file tree rooted at root and returns a channel
// that will receive entries as they're discovered. This allows processing to begin
// before the entire filesystem is traversed.
func WalkStreamWithOptions(ctx context.Context, root string, options WalkOptions) (<-chan SnapshotEntry, <-chan error) {
	// Create channels for entries and errors
	entryChan := make(chan SnapshotEntry, options.NumWorkers*2)
	errChan := make(chan error, 1)

	// Start the walk in a goroutine
	go func() {
		defer close(entryChan)
		defer close(errChan)

		err := WalkStreamWithCallback(ctx, root, options, func(entry SnapshotEntry) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case entryChan <- entry:
				return nil
			}
		})

		if err != nil {
			select {
			case errChan <- err:
				// Error sent
			default:
				// Error channel is full or closed
			}
		}
	}()

	return entryChan, errChan
}

// WalkStream walks the file tree rooted at root and returns a channel
// that will receive entries as they're discovered. It uses default options.
func WalkStream(ctx context.Context, root string) (<-chan SnapshotEntry, <-chan error) {
	return WalkStreamWithOptions(ctx, root, DefaultWalkOptions())
}
