package storage

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/TFMV/flashfs/internal/metadata"
	"github.com/TFMV/flashfs/internal/serializer"
	"github.com/TFMV/flashfs/internal/walker"
)

// Store represents a storage system that combines metadata and data storage
type Store struct {
	metadataStore *metadata.Store
	dataStore     *DataStore
	baseDir       string
}

// StoreOptions represents options for configuring the store
type StoreOptions struct {
	MetadataOptions metadata.Options
}

// DefaultStoreOptions returns the default options for the store
func DefaultStoreOptions() StoreOptions {
	return StoreOptions{
		MetadataOptions: metadata.DefaultOptions(),
	}
}

// NewStore creates a new store
func NewStore(baseDir string, options StoreOptions) (*Store, error) {
	// Create the metadata store
	metadataStore, err := metadata.New(filepath.Join(baseDir, "metadata.db"), options.MetadataOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	// Create the data store
	dataStore, err := NewDataStore(filepath.Join(baseDir, "data"))
	if err != nil {
		metadataStore.Close()
		return nil, fmt.Errorf("failed to create data store: %w", err)
	}

	return &Store{
		metadataStore: metadataStore,
		dataStore:     dataStore,
		baseDir:       baseDir,
	}, nil
}

// Close closes the store
func (s *Store) Close() error {
	metadataErr := s.metadataStore.Close()
	dataErr := s.dataStore.Close()

	if metadataErr != nil {
		return metadataErr
	}

	return dataErr
}

// CreateSnapshot creates a new snapshot
func (s *Store) CreateSnapshot(id string, description string, rootPath string, entries []walker.SnapshotEntry) error {
	// Serialize the snapshot data using FlatBuffers
	data, err := serializer.SerializeSnapshotFB(entries, nil)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot: %w", err)
	}

	// Write the snapshot data to the data store
	dataRef, err := s.dataStore.WriteSnapshotData(id, data)
	if err != nil {
		return fmt.Errorf("failed to write snapshot data: %w", err)
	}

	// Create the snapshot metadata
	snapshot := metadata.SnapshotMetadata{
		ID:          id,
		CreatedAt:   time.Now(),
		Description: description,
		FileCount:   len(entries),
		TotalSize:   calculateTotalSize(entries),
		RootPath:    rootPath,
	}

	// Add the snapshot metadata to the metadata store
	if err := s.metadataStore.AddSnapshot(snapshot); err != nil {
		return fmt.Errorf("failed to add snapshot metadata: %w", err)
	}

	// Add file metadata for each entry
	for _, entry := range entries {
		// Skip entries with errors
		if entry.Error != nil {
			continue
		}

		fileMetadata := metadata.FileMetadata{
			Size:          entry.Size,
			ModTime:       entry.ModTime.Unix(),
			Permissions:   uint32(entry.Permissions),
			IsDir:         entry.IsDir,
			Hash:          entry.Hash,
			DataRef:       dataRef,
			SymlinkTarget: entry.SymlinkTarget,
		}

		if err := s.metadataStore.AddFileMetadata(id, entry.Path, fileMetadata); err != nil {
			return fmt.Errorf("failed to add file metadata: %w", err)
		}
	}

	return nil
}

// GetSnapshot retrieves a snapshot
func (s *Store) GetSnapshot(id string) ([]walker.SnapshotEntry, error) {
	// Get the snapshot metadata
	_, err := s.metadataStore.GetSnapshot(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot metadata: %w", err)
	}

	// Get the file metadata for the snapshot
	files, err := s.metadataStore.FindFilesByPattern(id, "*")
	if err != nil {
		return nil, fmt.Errorf("failed to get file metadata: %w", err)
	}

	// Get a sample file to extract the data reference
	var dataRef string
	for _, file := range files {
		if file.DataRef != "" {
			dataRef = file.DataRef
			break
		}
	}

	if dataRef == "" {
		return nil, fmt.Errorf("no data reference found for snapshot %s", id)
	}

	// Read the snapshot data from the data store
	data, err := s.dataStore.ReadSnapshotData(dataRef)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot data: %w", err)
	}

	// Deserialize the snapshot data
	entries, err := serializer.DeserializeSnapshotFB(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize snapshot: %w", err)
	}

	return entries, nil
}

// ListSnapshots lists all snapshots
func (s *Store) ListSnapshots() ([]metadata.SnapshotMetadata, error) {
	return s.metadataStore.ListSnapshots()
}

// DeleteSnapshot deletes a snapshot
func (s *Store) DeleteSnapshot(id string) error {
	// Delete the snapshot data
	if err := s.dataStore.DeleteSnapshotData(id); err != nil {
		return fmt.Errorf("failed to delete snapshot data: %w", err)
	}

	// Delete the snapshot metadata
	if err := s.metadataStore.DeleteSnapshot(id); err != nil {
		return fmt.Errorf("failed to delete snapshot metadata: %w", err)
	}

	return nil
}

// ComputeDiff computes the difference between two snapshots
func (s *Store) ComputeDiff(oldID, newID string) ([]serializer.DiffEntry, error) {
	// Get the old snapshot
	oldEntries, err := s.GetSnapshot(oldID)
	if err != nil {
		return nil, fmt.Errorf("failed to get old snapshot: %w", err)
	}

	// Get the new snapshot
	newEntries, err := s.GetSnapshot(newID)
	if err != nil {
		return nil, fmt.Errorf("failed to get new snapshot: %w", err)
	}

	// Create a map of old entries by path
	oldEntriesMap := make(map[string]walker.SnapshotEntry)
	for _, entry := range oldEntries {
		oldEntriesMap[entry.Path] = entry
	}

	// Create a map of new entries by path
	newEntriesMap := make(map[string]walker.SnapshotEntry)
	for _, entry := range newEntries {
		newEntriesMap[entry.Path] = entry
	}

	// Compute the diff
	var diff []serializer.DiffEntry

	// Find added and modified files
	for path, newEntry := range newEntriesMap {
		oldEntry, exists := oldEntriesMap[path]
		if !exists {
			// File was added
			diff = append(diff, serializer.DiffEntry{
				Path:           path,
				Type:           serializer.DiffAdded,
				NewSize:        newEntry.Size,
				NewModTime:     newEntry.ModTime,
				NewPermissions: uint32(newEntry.Permissions),
				NewHash:        newEntry.Hash,
			})
		} else {
			// Check if file was modified
			if newEntry.Size != oldEntry.Size ||
				newEntry.ModTime.Unix() != oldEntry.ModTime.Unix() ||
				newEntry.Permissions != oldEntry.Permissions ||
				newEntry.Hash != oldEntry.Hash {
				// File was modified
				diff = append(diff, serializer.DiffEntry{
					Path:           path,
					Type:           serializer.DiffModified,
					OldSize:        oldEntry.Size,
					NewSize:        newEntry.Size,
					OldModTime:     oldEntry.ModTime,
					NewModTime:     newEntry.ModTime,
					OldPermissions: uint32(oldEntry.Permissions),
					NewPermissions: uint32(newEntry.Permissions),
					OldHash:        oldEntry.Hash,
					NewHash:        newEntry.Hash,
				})
			}
		}
	}

	// Find deleted files
	for path, oldEntry := range oldEntriesMap {
		_, exists := newEntriesMap[path]
		if !exists {
			// File was deleted
			diff = append(diff, serializer.DiffEntry{
				Path:           path,
				Type:           serializer.DiffDeleted,
				OldSize:        oldEntry.Size,
				OldModTime:     oldEntry.ModTime,
				OldPermissions: uint32(oldEntry.Permissions),
				OldHash:        oldEntry.Hash,
			})
		}
	}

	return diff, nil
}

// StoreDiff stores a diff between two snapshots
func (s *Store) StoreDiff(oldID, newID string) error {
	// Compute the diff
	diff, err := s.ComputeDiff(oldID, newID)
	if err != nil {
		return fmt.Errorf("failed to compute diff: %w", err)
	}

	// Serialize the diff data using FlatBuffers
	data, err := serializer.SerializeDiffFB(diff, nil)
	if err != nil {
		return fmt.Errorf("failed to serialize diff: %w", err)
	}

	// Write the diff data to the data store
	dataRef, err := s.dataStore.WriteDiffData(oldID, newID, data)
	if err != nil {
		return fmt.Errorf("failed to write diff data: %w", err)
	}

	// Add diff metadata for each entry
	for _, entry := range diff {
		// Convert DiffType to string representation
		var typeStr string
		switch entry.Type {
		case serializer.DiffAdded:
			typeStr = "added"
		case serializer.DiffModified:
			typeStr = "modified"
		case serializer.DiffDeleted:
			typeStr = "deleted"
		default:
			typeStr = "unknown"
		}

		diffMetadata := metadata.DiffMetadata{
			Type:           typeStr,
			OldSize:        entry.OldSize,
			NewSize:        entry.NewSize,
			OldModTime:     entry.OldModTime.Unix(),
			NewModTime:     entry.NewModTime.Unix(),
			OldPermissions: entry.OldPermissions,
			NewPermissions: entry.NewPermissions,
			OldHash:        entry.OldHash,
			NewHash:        entry.NewHash,
			DiffRef:        dataRef,
		}

		if err := s.metadataStore.AddDiffMetadata(oldID, newID, entry.Path, diffMetadata); err != nil {
			return fmt.Errorf("failed to add diff metadata: %w", err)
		}
	}

	return nil
}

// GetDiff retrieves a diff between two snapshots
func (s *Store) GetDiff(oldID, newID string) ([]serializer.DiffEntry, error) {
	// Get the diff metadata
	diffMetadata, err := s.metadataStore.ListDiffs(oldID, newID)
	if err != nil {
		return nil, fmt.Errorf("failed to get diff metadata: %w", err)
	}

	// Get a sample diff to extract the data reference
	var dataRef string
	for _, diff := range diffMetadata {
		if diff.DiffRef != "" {
			dataRef = diff.DiffRef
			break
		}
	}

	if dataRef == "" {
		return nil, fmt.Errorf("no data reference found for diff between %s and %s", oldID, newID)
	}

	// Read the diff data from the data store
	data, err := s.dataStore.ReadDiffData(dataRef)
	if err != nil {
		return nil, fmt.Errorf("failed to read diff data: %w", err)
	}

	// Deserialize the diff data
	diff, err := serializer.DeserializeDiffFB(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize diff: %w", err)
	}

	return diff, nil
}

// DeleteDiff deletes a diff between two snapshots
func (s *Store) DeleteDiff(oldID, newID string) error {
	// Delete the diff data
	if err := s.dataStore.DeleteDiffData(oldID, newID); err != nil {
		return fmt.Errorf("failed to delete diff data: %w", err)
	}

	// TODO: Delete the diff metadata

	return nil
}

// FindFilesByPattern finds files in a snapshot matching a pattern
func (s *Store) FindFilesByPattern(snapshotID, pattern string) (map[string]metadata.FileMetadata, error) {
	return s.metadataStore.FindFilesByPattern(snapshotID, pattern)
}

// FindFilesBySize finds files in a snapshot within a size range
func (s *Store) FindFilesBySize(snapshotID string, minSize, maxSize int64) (map[string]metadata.FileMetadata, error) {
	return s.metadataStore.FindFilesBySize(snapshotID, minSize, maxSize)
}

// FindFilesByModTime finds files in a snapshot modified within a time range
func (s *Store) FindFilesByModTime(snapshotID string, startTime, endTime time.Time) (map[string]metadata.FileMetadata, error) {
	return s.metadataStore.FindFilesByModTime(snapshotID, startTime, endTime)
}

// FindFilesByHash finds files in a snapshot with a specific hash
func (s *Store) FindFilesByHash(snapshotID, hash string) (map[string]metadata.FileMetadata, error) {
	return s.metadataStore.FindFilesByHash(snapshotID, hash)
}

// calculateTotalSize calculates the total size of all entries
func calculateTotalSize(entries []walker.SnapshotEntry) int64 {
	var totalSize int64
	for _, entry := range entries {
		if !entry.IsDir {
			totalSize += entry.Size
		}
	}
	return totalSize
}
