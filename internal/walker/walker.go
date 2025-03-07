package walker

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/zeebo/blake3"
)

// WalkOptions contains options for the Walk function
type WalkOptions struct {
	// ComputeHashes determines whether file hashes should be computed
	ComputeHashes bool
	// FollowSymlinks determines whether symbolic links should be followed
	FollowSymlinks bool
	// MaxDepth is the maximum directory depth to traverse (0 means no limit)
	MaxDepth int
}

// DefaultWalkOptions returns the default options for Walk
func DefaultWalkOptions() WalkOptions {
	return WalkOptions{
		ComputeHashes:  true,
		FollowSymlinks: false,
		MaxDepth:       100,
	}
}

// SnapshotEntry represents file metadata to be recorded.
type SnapshotEntry struct {
	Path        string
	Size        int64
	ModTime     int64
	IsDir       bool
	Permissions uint32
	Hash        []byte
}

// computeHash calculates the BLAKE3 hash of a file.
func computeHash(path string) []byte {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	h := blake3.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return nil
	}
	return h.Sum(nil)
}

// Walk performs a directory traversal starting at root with default options.
func Walk(root string) ([]SnapshotEntry, error) {
	return WalkWithOptions(context.Background(), root, DefaultWalkOptions())
}

// WalkWithContext performs a directory traversal starting at root with context support and default options.
func WalkWithContext(ctx context.Context, root string) ([]SnapshotEntry, error) {
	return WalkWithOptions(ctx, root, DefaultWalkOptions())
}

// WalkWithOptions performs a directory traversal starting at root with the specified options.
func WalkWithOptions(ctx context.Context, root string, options WalkOptions) ([]SnapshotEntry, error) {
	// Check if root exists and is a directory
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", root)
	}

	// Ensure root path is absolute
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	// Use a slice with pre-allocated capacity for better performance
	// A typical directory might have hundreds or thousands of files
	result := make([]SnapshotEntry, 0, 1000)

	// Add the root directory itself to the result
	rootEntry := SnapshotEntry{
		Path:        ".",
		Size:        info.Size(),
		ModTime:     info.ModTime().Unix(),
		IsDir:       true,
		Permissions: uint32(info.Mode().Perm()),
		Hash:        nil,
	}
	result = append(result, rootEntry)

	// Use a map for tracking visited directories - faster than sync.Map for this use case
	// since we don't need concurrent access in the non-concurrent version
	visitedDirs := make(map[string]bool)

	// Use a counter for statistics
	var entryCount int64 = 1 // Start at 1 for the root directory

	// Use filepath.WalkDir for the traversal
	err = filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, err error) error {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return nil // Skip errors and continue
		}

		// Skip the root directory itself since we already added it
		if path == absRoot {
			return nil
		}

		// Check directory depth if MaxDepth is set
		if options.MaxDepth > 0 {
			relPath, err := filepath.Rel(absRoot, path)
			if err != nil {
				return nil
			}

			// Count path separators to determine depth
			depth := 0
			for _, c := range relPath {
				if c == filepath.Separator {
					depth++
				}
			}

			if depth >= options.MaxDepth {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}

		// Handle symlinks
		isSymlink := false
		info, err := d.Info()
		if err != nil {
			return nil // Skip files we can't stat
		}

		// Check if it's a symlink
		if info.Mode()&os.ModeSymlink != 0 {
			isSymlink = true

			// Skip symlinks to directories if not following them
			if d.IsDir() && !options.FollowSymlinks {
				return filepath.SkipDir
			}

			// For symlinks we want to follow, we need to get the real path
			if options.FollowSymlinks {
				realPath, err := filepath.EvalSymlinks(path)
				if err != nil {
					return nil
				}

				// Check if we've already visited this directory to avoid cycles
				if d.IsDir() {
					absRealPath, err := filepath.Abs(realPath)
					if err != nil {
						return nil
					}

					if visitedDirs[absRealPath] {
						return filepath.SkipDir
					}
					visitedDirs[absRealPath] = true
				}
			}
		}

		// For directories, mark as visited to avoid cycles from symlinks
		if d.IsDir() && !isSymlink {
			absPath, err := filepath.Abs(path)
			if err != nil {
				return nil
			}
			visitedDirs[absPath] = true
		}

		// Compute hash if needed
		var hash []byte
		if options.ComputeHashes && !d.IsDir() && !isSymlink {
			hash = computeHash(path)
		}

		// Calculate path relative to root
		relPath, err := filepath.Rel(absRoot, path)
		if err != nil {
			return nil
		}

		// Create the entry
		entry := SnapshotEntry{
			Path:        relPath,
			Size:        info.Size(),
			ModTime:     info.ModTime().Unix(),
			IsDir:       d.IsDir(),
			Permissions: uint32(info.Mode().Perm()),
			Hash:        hash,
		}

		// Append to result - no mutex needed in non-concurrent version
		result = append(result, entry)

		// Increment counter atomically
		atomic.AddInt64(&entryCount, 1)

		return nil
	})

	if err != nil {
		return result, err
	}

	return result, nil
}
