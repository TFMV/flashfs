package walker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/karrick/godirwalk"
	"github.com/zeebo/blake3"
)

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

// Walk performs a concurrent directory traversal starting at root.
func Walk(root string) ([]SnapshotEntry, error) {
	return WalkWithContext(context.Background(), root)
}

// WalkWithContext performs a concurrent directory traversal starting at root with context support.
func WalkWithContext(ctx context.Context, root string) ([]SnapshotEntry, error) {
	// Check if root exists and is a directory
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", root)
	}

	var entries []SnapshotEntry
	var mu sync.Mutex

	// Ensure root path is absolute
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	// Channel for directories to process.
	ch := make(chan string, 100)
	var wg sync.WaitGroup

	// Create a context for cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Error channel to collect errors from workers
	errCh := make(chan error, 1)

	// Done channel to signal completion
	done := make(chan struct{})

	// Number of concurrent workers (can be tuned/adapted).
	workers := runtime.NumCPU()

	// Start worker goroutines.
	for i := 0; i < workers; i++ {
		go func() {
			for dir := range ch {
				// Check for context cancellation
				select {
				case <-ctx.Done():
					wg.Done()
					continue
				default:
				}

				files, err := godirwalk.ReadDirents(dir, nil)
				if err != nil {
					// Send the first error encountered
					select {
					case errCh <- err:
						cancel() // Cancel other workers
					default:
					}
					wg.Done()
					continue
				}

				for _, file := range files {
					// Check for context cancellation
					select {
					case <-ctx.Done():
						break
					default:
					}

					fullPath := filepath.Join(dir, file.Name())
					info, err := os.Stat(fullPath)
					if err != nil {
						continue
					}

					var hash []byte
					if !info.IsDir() {
						// Compute content hash (can be made optional)
						hash = computeHash(fullPath)
					}

					// Calculate path relative to root
					relPath, err := filepath.Rel(absRoot, fullPath)
					if err != nil {
						continue
					}

					entry := SnapshotEntry{
						Path:        relPath,
						Size:        info.Size(),
						ModTime:     info.ModTime().Unix(),
						IsDir:       info.IsDir(),
						Permissions: uint32(info.Mode().Perm()),
						Hash:        hash,
					}

					mu.Lock()
					entries = append(entries, entry)
					mu.Unlock()

					if info.IsDir() {
						// Check for context cancellation before adding more work
						select {
						case <-ctx.Done():
							break
						default:
							wg.Add(1)
							ch <- fullPath
						}
					}
				}
				wg.Done()
			}
		}()
	}

	// Enqueue the root directory.
	wg.Add(1)
	ch <- absRoot

	// Wait for completion in a separate goroutine
	go func() {
		wg.Wait()
		close(ch)
		close(done)
	}()

	// Wait for either completion, error, or context cancellation
	select {
	case <-done:
		// All work completed successfully
		return entries, nil
	case err := <-errCh:
		// An error occurred
		return entries, err
	case <-ctx.Done():
		// Context was canceled
		return entries, ctx.Err()
	}
}
