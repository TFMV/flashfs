package diff

import (
	"fmt"
	"os"

	"github.com/TFMV/flashfs/internal/walker"
	flashfs "github.com/TFMV/flashfs/schema/flashfs"
)

// loadSnapshot reads and deserializes a snapshot file into a map keyed by file path.
func loadSnapshot(filename string) (map[string]walker.SnapshotEntry, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	snapshot := flashfs.GetRootAsSnapshot(data, 0)
	result := make(map[string]walker.SnapshotEntry)
	entriesLen := snapshot.EntriesLength()
	var fe flashfs.FileEntry
	for i := 0; i < entriesLen; i++ {
		if snapshot.Entries(&fe, i) {
			entry := walker.SnapshotEntry{
				Path:        string(fe.Path()),
				Size:        fe.Size(),
				ModTime:     fe.Mtime(),
				IsDir:       fe.IsDir(),
				Permissions: fe.Permissions(),
			}
			// Retrieve hash bytes if available.
			hashBytes := fe.HashBytes()
			if len(hashBytes) > 0 {
				entry.Hash = hashBytes
			}
			result[entry.Path] = entry
		}
	}
	return result, nil
}

// CompareSnapshots returns a list of differences between two snapshots.
func CompareSnapshots(oldFile, newFile string) ([]string, error) {
	oldSnapshot, err := loadSnapshot(oldFile)
	if err != nil {
		return nil, err
	}
	newSnapshot, err := loadSnapshot(newFile)
	if err != nil {
		return nil, err
	}

	var diffs []string
	// Check for new or modified files.
	for path, newEntry := range newSnapshot {
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
		if _, exists := newSnapshot[path]; !exists {
			diffs = append(diffs, fmt.Sprintf("Deleted: %s", path))
		}
	}
	return diffs, nil
}
