package walker

import (
	"io"
	"os"
	"path/filepath"
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
	var entries []SnapshotEntry
	var mu sync.Mutex

	// Channel for directories to process.
	ch := make(chan string, 100)
	var wg sync.WaitGroup

	// Number of concurrent workers (can be tuned/adapted).
	workers := 4

	// Start worker goroutines.
	for i := 0; i < workers; i++ {
		go func() {
			for dir := range ch {
				files, err := godirwalk.ReadDirents(dir, nil)
				if err != nil {
					continue
				}

				for _, file := range files {
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

					entry := SnapshotEntry{
						Path:        fullPath,
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
						wg.Add(1)
						ch <- fullPath
					}
				}
				wg.Done()
			}
		}()
	}

	// Enqueue the root directory.
	wg.Add(1)
	ch <- root

	// Wait until all directories have been processed.
	wg.Wait()
	close(ch)

	return entries, nil
}
