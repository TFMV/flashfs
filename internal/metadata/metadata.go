package metadata

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/buntdb"
)

// Store represents a metadata store backed by BuntDB
type Store struct {
	db         *buntdb.DB
	path       string
	mutex      sync.RWMutex
	autoCommit bool
}

// FileMetadata represents metadata for a file in a snapshot
type FileMetadata struct {
	Size          int64  `json:"size"`
	ModTime       int64  `json:"mtime"`
	Permissions   uint32 `json:"mode"`
	IsDir         bool   `json:"isDir"`
	Hash          string `json:"hash,omitempty"`
	DataRef       string `json:"dataRef,omitempty"` // Reference to FlatBuffer data
	SymlinkTarget string `json:"symlinkTarget,omitempty"`
}

// DiffMetadata represents metadata for a file diff between snapshots
type DiffMetadata struct {
	Type           string `json:"type"` // "added", "modified", "deleted"
	OldSize        int64  `json:"oldSize,omitempty"`
	NewSize        int64  `json:"newSize,omitempty"`
	OldModTime     int64  `json:"oldMtime,omitempty"`
	NewModTime     int64  `json:"newMtime,omitempty"`
	OldPermissions uint32 `json:"oldMode,omitempty"`
	NewPermissions uint32 `json:"newMode,omitempty"`
	OldHash        string `json:"oldHash,omitempty"`
	NewHash        string `json:"newHash,omitempty"`
	DiffRef        string `json:"diffRef,omitempty"` // Reference to diff data
}

// SnapshotMetadata represents metadata for a snapshot
type SnapshotMetadata struct {
	ID          string    `json:"id"`
	CreatedAt   time.Time `json:"createdAt"`
	Description string    `json:"description,omitempty"`
	FileCount   int       `json:"fileCount"`
	TotalSize   int64     `json:"totalSize"`
	RootPath    string    `json:"rootPath"`
}

// Options for configuring the metadata store
type Options struct {
	AutoCommit bool
}

// DefaultOptions returns the default options for the metadata store
func DefaultOptions() Options {
	return Options{
		AutoCommit: true,
	}
}

// New creates a new metadata store
func New(path string, options Options) (*Store, error) {
	db, err := buntdb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata store: %w", err)
	}

	// Create indexes
	if err := createIndexes(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}

	return &Store{
		db:         db,
		path:       path,
		autoCommit: options.AutoCommit,
	}, nil
}

// createIndexes creates the necessary indexes for efficient querying
func createIndexes(db *buntdb.DB) error {
	// Create index for file paths
	if err := db.CreateIndex("path", "file:*:*", buntdb.IndexString); err != nil {
		return err
	}

	// Create index for file modification times
	if err := db.CreateIndex("mtime", "file:*:*", buntdb.IndexJSON("mtime")); err != nil {
		return err
	}

	// Create index for file sizes
	if err := db.CreateIndex("size", "file:*:*", buntdb.IndexJSON("size")); err != nil {
		return err
	}

	// Create index for file hashes
	if err := db.CreateIndex("hash", "file:*:*", buntdb.IndexJSON("hash")); err != nil {
		return err
	}

	// Create index for snapshots by time
	if err := db.CreateIndex("snapshot_time", "snapshot:*", buntdb.IndexJSON("createdAt")); err != nil {
		return err
	}

	// Create index for diffs
	if err := db.CreateIndex("diff", "diff:*:*:*", buntdb.IndexString); err != nil {
		return err
	}

	return nil
}

// Close closes the metadata store
func (s *Store) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.db.Close()
}

// AddSnapshot adds a new snapshot to the metadata store
func (s *Store) AddSnapshot(snapshot SnapshotMetadata) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}

	err = s.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set("snapshot:"+snapshot.ID, string(data), nil)
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to add snapshot: %w", err)
	}

	return nil
}

// GetSnapshot retrieves a snapshot from the metadata store
func (s *Store) GetSnapshot(id string) (SnapshotMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var snapshot SnapshotMetadata

	err := s.db.View(func(tx *buntdb.Tx) error {
		data, err := tx.Get("snapshot:" + id)
		if err != nil {
			return err
		}

		return json.Unmarshal([]byte(data), &snapshot)
	})

	if err != nil {
		return SnapshotMetadata{}, fmt.Errorf("failed to get snapshot: %w", err)
	}

	return snapshot, nil
}

// ListSnapshots lists all snapshots in the metadata store
func (s *Store) ListSnapshots() ([]SnapshotMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var snapshots []SnapshotMetadata

	err := s.db.View(func(tx *buntdb.Tx) error {
		return tx.Ascend("snapshot_time", func(key, value string) bool {
			var snapshot SnapshotMetadata
			if err := json.Unmarshal([]byte(value), &snapshot); err != nil {
				return false
			}
			snapshots = append(snapshots, snapshot)
			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	return snapshots, nil
}

// DeleteSnapshot deletes a snapshot and all its file metadata
func (s *Store) DeleteSnapshot(id string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.db.Update(func(tx *buntdb.Tx) error {
		// Delete snapshot metadata
		if _, err := tx.Delete("snapshot:" + id); err != nil && err != buntdb.ErrNotFound {
			return err
		}

		// Delete all file metadata for this snapshot
		var filesToDelete []string
		err := tx.AscendKeys("file:"+id+":*", func(key, _ string) bool {
			filesToDelete = append(filesToDelete, key)
			return true
		})
		if err != nil {
			return err
		}

		for _, key := range filesToDelete {
			if _, err := tx.Delete(key); err != nil && err != buntdb.ErrNotFound {
				return err
			}
		}

		return nil
	})
}

// AddFileMetadata adds file metadata to a snapshot
func (s *Store) AddFileMetadata(snapshotID, path string, metadata FileMetadata) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal file metadata: %w", err)
	}

	err = s.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(fmt.Sprintf("file:%s:%s", snapshotID, path), string(data), nil)
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to add file metadata: %w", err)
	}

	return nil
}

// GetFileMetadata retrieves file metadata from a snapshot
func (s *Store) GetFileMetadata(snapshotID, path string) (FileMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var metadata FileMetadata

	err := s.db.View(func(tx *buntdb.Tx) error {
		data, err := tx.Get(fmt.Sprintf("file:%s:%s", snapshotID, path))
		if err != nil {
			return err
		}

		return json.Unmarshal([]byte(data), &metadata)
	})

	if err != nil {
		return FileMetadata{}, fmt.Errorf("failed to get file metadata: %w", err)
	}

	return metadata, nil
}

// AddDiffMetadata adds diff metadata between two snapshots
func (s *Store) AddDiffMetadata(baseSnapshotID, targetSnapshotID, path string, metadata DiffMetadata) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal diff metadata: %w", err)
	}

	err = s.db.Update(func(tx *buntdb.Tx) error {
		key := fmt.Sprintf("diff:%s:%s:%s", baseSnapshotID, targetSnapshotID, path)
		_, _, err := tx.Set(key, string(data), nil)
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to add diff metadata: %w", err)
	}

	return nil
}

// GetDiffMetadata retrieves diff metadata between two snapshots
func (s *Store) GetDiffMetadata(baseSnapshotID, targetSnapshotID, path string) (DiffMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var metadata DiffMetadata

	err := s.db.View(func(tx *buntdb.Tx) error {
		key := fmt.Sprintf("diff:%s:%s:%s", baseSnapshotID, targetSnapshotID, path)
		data, err := tx.Get(key)
		if err != nil {
			return err
		}

		return json.Unmarshal([]byte(data), &metadata)
	})

	if err != nil {
		return DiffMetadata{}, fmt.Errorf("failed to get diff metadata: %w", err)
	}

	return metadata, nil
}

// ListDiffs lists all diffs between two snapshots
func (s *Store) ListDiffs(baseSnapshotID, targetSnapshotID string) (map[string]DiffMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	diffs := make(map[string]DiffMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("diff:%s:%s:", baseSnapshotID, targetSnapshotID)
		return tx.AscendKeys(prefix+"*", func(key, value string) bool {
			path := strings.TrimPrefix(key, prefix)
			var diff DiffMetadata
			if err := json.Unmarshal([]byte(value), &diff); err != nil {
				return false
			}
			diffs[path] = diff
			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list diffs: %w", err)
	}

	return diffs, nil
}

// FindFilesByPattern finds files in a snapshot matching a pattern
func (s *Store) FindFilesByPattern(snapshotID, pattern string) (map[string]FileMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	files := make(map[string]FileMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("file:%s:", snapshotID)
		return tx.AscendKeys(prefix+"*", func(key, value string) bool {
			path := strings.TrimPrefix(key, prefix)
			match, err := filepath.Match(pattern, path)
			if err != nil || !match {
				return true // Continue to next item
			}

			var metadata FileMetadata
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				return false
			}
			files[path] = metadata
			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to find files: %w", err)
	}

	return files, nil
}

// FindFilesBySize finds files in a snapshot within a size range
func (s *Store) FindFilesBySize(snapshotID string, minSize, maxSize int64) (map[string]FileMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	files := make(map[string]FileMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("file:%s:", snapshotID)

		// Use the size index to find files within the size range
		return tx.AscendGreaterOrEqual("size", fmt.Sprintf(`{"size":%d}`, minSize), func(key, value string) bool {
			if !strings.HasPrefix(key, prefix) {
				return true // Continue to next item
			}

			var metadata FileMetadata
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				return false
			}

			if metadata.Size > maxSize {
				return false // Stop iteration
			}

			path := strings.TrimPrefix(key, prefix)
			files[path] = metadata
			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to find files by size: %w", err)
	}

	return files, nil
}

// FindFilesByModTime finds files in a snapshot modified within a time range
func (s *Store) FindFilesByModTime(snapshotID string, startTime, endTime time.Time) (map[string]FileMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	files := make(map[string]FileMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("file:%s:", snapshotID)
		startUnix := startTime.Unix()
		endUnix := endTime.Unix()

		// Use the mtime index to find files within the time range
		return tx.AscendGreaterOrEqual("mtime", fmt.Sprintf(`{"mtime":%d}`, startUnix), func(key, value string) bool {
			if !strings.HasPrefix(key, prefix) {
				return true // Continue to next item
			}

			var metadata FileMetadata
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				return false
			}

			if metadata.ModTime > endUnix {
				return false // Stop iteration
			}

			path := strings.TrimPrefix(key, prefix)
			files[path] = metadata
			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to find files by mod time: %w", err)
	}

	return files, nil
}

// FindFilesByHash finds files in a snapshot with a specific hash
func (s *Store) FindFilesByHash(snapshotID, hash string) (map[string]FileMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	files := make(map[string]FileMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("file:%s:", snapshotID)

		// Use the hash index to find files with the specified hash
		return tx.AscendEqual("hash", fmt.Sprintf(`{"hash":"%s"}`, hash), func(key, value string) bool {
			if !strings.HasPrefix(key, prefix) {
				return true // Continue to next item
			}

			var metadata FileMetadata
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				return false
			}

			path := strings.TrimPrefix(key, prefix)
			files[path] = metadata
			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to find files by hash: %w", err)
	}

	return files, nil
}

// Shrink shrinks the database file
func (s *Store) Shrink() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.db.Shrink()
}
