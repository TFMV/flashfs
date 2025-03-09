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

// StoreType represents the type of metadata store
type StoreType int

const (
	// NoStore means no metadata store is used (cold storage only)
	NoStore StoreType = iota
	// HotStore means a BuntDB store is used for fast querying
	HotStore
)

// Store represents a metadata store backed by BuntDB
type Store struct {
	db         *buntdb.DB
	path       string
	mutex      sync.RWMutex
	autoCommit bool
	storeType  StoreType
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
	IsHot       bool      `json:"isHot"` // Whether this snapshot is in the hot layer
}

// Options for configuring the metadata store
type Options struct {
	AutoCommit bool
	StoreType  StoreType
}

// DefaultOptions returns the default options for the metadata store
func DefaultOptions() Options {
	return Options{
		AutoCommit: true,
		StoreType:  HotStore,
	}
}

// New creates a new metadata store
func New(path string, options Options) (*Store, error) {
	store := &Store{
		path:       path,
		autoCommit: options.AutoCommit,
		storeType:  options.StoreType,
	}

	// If no store is requested, return early
	if options.StoreType == NoStore {
		return store, nil
	}

	// Otherwise, create a BuntDB store
	db, err := buntdb.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata store: %w", err)
	}

	// Create indexes
	if err := createIndexes(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create indexes: %w", err)
	}

	store.db = db
	return store, nil
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

	if s.storeType == NoStore || s.db == nil {
		return nil
	}

	return s.db.Close()
}

// IsHotLayerEnabled returns whether the hot layer is enabled
func (s *Store) IsHotLayerEnabled() bool {
	return s.storeType == HotStore && s.db != nil
}

// AddSnapshot adds a new snapshot to the metadata store
func (s *Store) AddSnapshot(snapshot SnapshotMetadata) error {
	if s.storeType == NoStore || s.db == nil {
		// No-op if hot layer is disabled
		return nil
	}

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
	if s.storeType == NoStore || s.db == nil {
		return SnapshotMetadata{}, fmt.Errorf("hot layer is disabled")
	}

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
	if s.storeType == NoStore || s.db == nil {
		return []SnapshotMetadata{}, nil
	}

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
	if s.storeType == NoStore || s.db == nil {
		return nil
	}

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
	if s.storeType == NoStore || s.db == nil {
		return nil
	}

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
	if s.storeType == NoStore || s.db == nil {
		return FileMetadata{}, fmt.Errorf("hot layer is disabled")
	}

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
	if s.storeType == NoStore || s.db == nil {
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal diff metadata: %w", err)
	}

	err = s.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(fmt.Sprintf("diff:%s:%s:%s", baseSnapshotID, targetSnapshotID, path), string(data), nil)
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to add diff metadata: %w", err)
	}

	return nil
}

// GetDiffMetadata retrieves diff metadata between two snapshots
func (s *Store) GetDiffMetadata(baseSnapshotID, targetSnapshotID, path string) (DiffMetadata, error) {
	if s.storeType == NoStore || s.db == nil {
		return DiffMetadata{}, fmt.Errorf("hot layer is disabled")
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	var metadata DiffMetadata

	err := s.db.View(func(tx *buntdb.Tx) error {
		data, err := tx.Get(fmt.Sprintf("diff:%s:%s:%s", baseSnapshotID, targetSnapshotID, path))
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
	if s.storeType == NoStore || s.db == nil {
		return map[string]DiffMetadata{}, nil
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	diffs := make(map[string]DiffMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("diff:%s:%s:", baseSnapshotID, targetSnapshotID)
		return tx.AscendKeys(prefix+"*", func(key, value string) bool {
			var metadata DiffMetadata
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				return false
			}

			// Extract the path from the key
			path := strings.TrimPrefix(key, prefix)
			diffs[path] = metadata
			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list diffs: %w", err)
	}

	return diffs, nil
}

// FindFilesByPattern finds files in a snapshot by pattern
func (s *Store) FindFilesByPattern(snapshotID, pattern string) (map[string]FileMetadata, error) {
	if s.storeType == NoStore || s.db == nil {
		return map[string]FileMetadata{}, nil
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	files := make(map[string]FileMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("file:%s:", snapshotID)
		return tx.AscendKeys(prefix+"*", func(key, value string) bool {
			// Extract the path from the key
			path := strings.TrimPrefix(key, prefix)

			// Check if the path matches the pattern
			matched, err := filepath.Match(pattern, path)
			if err != nil || !matched {
				return true
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
		return nil, fmt.Errorf("failed to find files by pattern: %w", err)
	}

	return files, nil
}

// FindFilesBySize finds files in a snapshot by size range
func (s *Store) FindFilesBySize(snapshotID string, minSize, maxSize int64) (map[string]FileMetadata, error) {
	if s.storeType == NoStore || s.db == nil {
		return map[string]FileMetadata{}, nil
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	files := make(map[string]FileMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("file:%s:", snapshotID)
		return tx.AscendKeys(prefix+"*", func(key, value string) bool {
			var metadata FileMetadata
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				return false
			}

			// Check if the file size is within the range
			if metadata.Size >= minSize && (maxSize == 0 || metadata.Size <= maxSize) {
				// Extract the path from the key
				path := strings.TrimPrefix(key, prefix)
				files[path] = metadata
			}

			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to find files by size: %w", err)
	}

	return files, nil
}

// FindFilesByModTime finds files in a snapshot by modification time range
func (s *Store) FindFilesByModTime(snapshotID string, startTime, endTime time.Time) (map[string]FileMetadata, error) {
	if s.storeType == NoStore || s.db == nil {
		return map[string]FileMetadata{}, nil
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	files := make(map[string]FileMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("file:%s:", snapshotID)
		return tx.AscendKeys(prefix+"*", func(key, value string) bool {
			var metadata FileMetadata
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				return false
			}

			// Check if the modification time is within the range
			modTime := time.Unix(metadata.ModTime, 0)
			if (startTime.IsZero() || !modTime.Before(startTime)) &&
				(endTime.IsZero() || !modTime.After(endTime)) {
				// Extract the path from the key
				path := strings.TrimPrefix(key, prefix)
				files[path] = metadata
			}

			return true
		})
	})

	if err != nil {
		return nil, fmt.Errorf("failed to find files by modification time: %w", err)
	}

	return files, nil
}

// FindFilesByHash finds files in a snapshot by hash
func (s *Store) FindFilesByHash(snapshotID, hash string) (map[string]FileMetadata, error) {
	if s.storeType == NoStore || s.db == nil {
		return map[string]FileMetadata{}, nil
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	files := make(map[string]FileMetadata)

	err := s.db.View(func(tx *buntdb.Tx) error {
		prefix := fmt.Sprintf("file:%s:", snapshotID)
		return tx.AscendKeys(prefix+"*", func(key, value string) bool {
			var metadata FileMetadata
			if err := json.Unmarshal([]byte(value), &metadata); err != nil {
				return false
			}

			// Check if the hash matches
			if metadata.Hash == hash {
				// Extract the path from the key
				path := strings.TrimPrefix(key, prefix)
				files[path] = metadata
			}

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
	if s.storeType == NoStore || s.db == nil {
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.db.Shrink()
}
