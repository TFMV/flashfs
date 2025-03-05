package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/TFMV/flashfs/schema/flashfs"
	"github.com/cespare/xxhash/v2"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/klauspost/compress/zstd"
	"github.com/spaolacci/murmur3"
)

const (
	// DefaultCompression is the default compression level
	DefaultCompression = 3
	// DefaultCacheSize is the default number of snapshots to cache
	DefaultCacheSize = 10
	// SnapshotFileExt is the file extension for snapshots
	SnapshotFileExt = ".snap"
	// DiffFileExt is the file extension for diff files
	DiffFileExt = ".diff"
)

var (
	// ErrSnapshotNotFound is returned when a snapshot is not found
	ErrSnapshotNotFound = errors.New("snapshot not found")
	// ErrInvalidSnapshot is returned when a snapshot is invalid
	ErrInvalidSnapshot = errors.New("invalid snapshot")
)

// SnapshotStore manages snapshot storage operations
type SnapshotStore struct {
	baseDir       string
	encoder       *zstd.Encoder
	decoder       *zstd.Decoder
	cacheMutex    sync.RWMutex
	snapshotCache map[string][]byte
	cacheSize     int
	cacheKeys     []string
	expiryPolicy  ExpiryPolicy
}

// NewSnapshotStore creates a new snapshot store
func NewSnapshotStore(baseDir string) (*SnapshotStore, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	// Create encoder and decoder
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevel(DefaultCompression)))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &SnapshotStore{
		baseDir:       baseDir,
		encoder:       encoder,
		decoder:       decoder,
		snapshotCache: make(map[string][]byte),
		cacheSize:     DefaultCacheSize,
		cacheKeys:     make([]string, 0, DefaultCacheSize),
		expiryPolicy:  DefaultExpiryPolicy(),
	}, nil
}

// Close closes the snapshot store
func (s *SnapshotStore) Close() error {
	s.encoder.Close()
	s.decoder.Close()
	return nil
}

// WriteSnapshot writes a snapshot to disk with compression
func (s *SnapshotStore) WriteSnapshot(name string, data []byte) error {
	filename := filepath.Join(s.baseDir, name+SnapshotFileExt)

	// Compress the data
	compressed := s.encoder.EncodeAll(data, nil)

	// Use O_SYNC for durability
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(compressed)
	if err != nil {
		return err
	}

	// Update cache
	s.cacheSnapshot(name, data)

	return nil
}

// ReadSnapshot reads a snapshot from disk or cache
func (s *SnapshotStore) ReadSnapshot(name string) ([]byte, error) {
	// Check cache first
	s.cacheMutex.RLock()
	if data, ok := s.snapshotCache[name]; ok {
		s.cacheMutex.RUnlock()
		return data, nil
	}
	s.cacheMutex.RUnlock()

	// Read from disk
	filename := filepath.Join(s.baseDir, name+SnapshotFileExt)
	compressed, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	// Decompress
	data, err := s.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}

	// Update cache
	s.cacheSnapshot(name, data)

	return data, nil
}

// ListSnapshots returns a list of available snapshots
func (s *SnapshotStore) ListSnapshots() ([]string, error) {
	files, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}

	snapshots := make([]string, 0, len(files))
	for _, file := range files {
		if filepath.Ext(file.Name()) == SnapshotFileExt {
			snapshots = append(snapshots, file.Name()[:len(file.Name())-len(SnapshotFileExt)])
		}
	}

	return snapshots, nil
}

// DeleteSnapshot deletes a snapshot
func (s *SnapshotStore) DeleteSnapshot(name string) error {
	filename := filepath.Join(s.baseDir, name+SnapshotFileExt)

	// Remove from cache
	s.cacheMutex.Lock()
	delete(s.snapshotCache, name)
	for i, key := range s.cacheKeys {
		if key == name {
			s.cacheKeys = append(s.cacheKeys[:i], s.cacheKeys[i+1:]...)
			break
		}
	}
	s.cacheMutex.Unlock()

	// Delete file
	return os.Remove(filename)
}

// ComputeDiff computes the difference between two snapshots
func (s *SnapshotStore) ComputeDiff(oldName, newName string) ([]byte, error) {
	oldData, err := s.ReadSnapshot(oldName)
	if err != nil {
		return nil, err
	}

	newData, err := s.ReadSnapshot(newName)
	if err != nil {
		return nil, err
	}

	oldSnapshot := flashfs.GetRootAsSnapshot(oldData, 0)
	newSnapshot := flashfs.GetRootAsSnapshot(newData, 0)

	// Create a map of old entries for quick lookup
	oldEntries := make(map[string]*flashfs.FileEntry)
	for i := 0; i < oldSnapshot.EntriesLength(); i++ {
		var entry flashfs.FileEntry
		if oldSnapshot.Entries(&entry, i) {
			oldEntries[string(entry.Path())] = &entry
		}
	}

	// Build diff
	builder := flatbuffers.NewBuilder(1024)
	diffEntryOffsets := make([]flatbuffers.UOffsetT, 0)

	// Find added or modified entries
	for i := 0; i < newSnapshot.EntriesLength(); i++ {
		var newEntry flashfs.FileEntry
		if !newSnapshot.Entries(&newEntry, i) {
			continue
		}

		path := string(newEntry.Path())
		oldEntry, exists := oldEntries[path]

		// If entry doesn't exist or has changed, add to diff
		if !exists || entriesDiffer(&newEntry, oldEntry) {
			pathOffset := builder.CreateString(path)

			var oldHashOffset, newHashOffset flatbuffers.UOffsetT

			// Create hash vectors
			newHashBytes := newEntry.HashBytes()
			if len(newHashBytes) > 0 {
				newHashOffset = builder.CreateByteVector(newHashBytes)
			}

			if exists {
				// Modified file
				oldHashBytes := oldEntry.HashBytes()
				if len(oldHashBytes) > 0 {
					oldHashOffset = builder.CreateByteVector(oldHashBytes)
				}

				// Start building a DiffEntry for modified file
				flashfs.DiffEntryStart(builder)
				flashfs.DiffEntryAddPath(builder, pathOffset)
				flashfs.DiffEntryAddType(builder, 1) // 1 = modified
				flashfs.DiffEntryAddOldSize(builder, oldEntry.Size())
				flashfs.DiffEntryAddNewSize(builder, newEntry.Size())
				flashfs.DiffEntryAddOldMtime(builder, oldEntry.Mtime())
				flashfs.DiffEntryAddNewMtime(builder, newEntry.Mtime())
				flashfs.DiffEntryAddOldPermissions(builder, oldEntry.Permissions())
				flashfs.DiffEntryAddNewPermissions(builder, newEntry.Permissions())
				if len(oldHashBytes) > 0 {
					flashfs.DiffEntryAddOldHash(builder, oldHashOffset)
				}
				if len(newHashBytes) > 0 {
					flashfs.DiffEntryAddNewHash(builder, newHashOffset)
				}
			} else {
				// Added file
				// Start building a DiffEntry for added file
				flashfs.DiffEntryStart(builder)
				flashfs.DiffEntryAddPath(builder, pathOffset)
				flashfs.DiffEntryAddType(builder, 0) // 0 = added
				flashfs.DiffEntryAddNewSize(builder, newEntry.Size())
				flashfs.DiffEntryAddNewMtime(builder, newEntry.Mtime())
				flashfs.DiffEntryAddNewPermissions(builder, newEntry.Permissions())
				if len(newHashBytes) > 0 {
					flashfs.DiffEntryAddNewHash(builder, newHashOffset)
				}
			}

			diffEntryOffsets = append(diffEntryOffsets, flashfs.DiffEntryEnd(builder))
		}

		// Remove from old entries map to track deletions
		delete(oldEntries, path)
	}

	// Add deleted entries
	for path, oldEntry := range oldEntries {
		pathOffset := builder.CreateString(path)

		var oldHashOffset flatbuffers.UOffsetT
		oldHashBytes := oldEntry.HashBytes()
		if len(oldHashBytes) > 0 {
			oldHashOffset = builder.CreateByteVector(oldHashBytes)
		}

		// Start building a DiffEntry for deleted file
		flashfs.DiffEntryStart(builder)
		flashfs.DiffEntryAddPath(builder, pathOffset)
		flashfs.DiffEntryAddType(builder, 2) // 2 = deleted
		flashfs.DiffEntryAddOldSize(builder, oldEntry.Size())
		flashfs.DiffEntryAddOldMtime(builder, oldEntry.Mtime())
		flashfs.DiffEntryAddOldPermissions(builder, oldEntry.Permissions())
		if len(oldHashBytes) > 0 {
			flashfs.DiffEntryAddOldHash(builder, oldHashOffset)
		}

		diffEntryOffsets = append(diffEntryOffsets, flashfs.DiffEntryEnd(builder))
	}

	// Create vector of diff entries
	flashfs.DiffStartEntriesVector(builder, len(diffEntryOffsets))
	for i := len(diffEntryOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(diffEntryOffsets[i])
	}
	entriesVector := builder.EndVector(len(diffEntryOffsets))

	flashfs.DiffStart(builder)
	flashfs.DiffAddEntries(builder, entriesVector)
	diff := flashfs.DiffEnd(builder)
	builder.Finish(diff)

	return builder.FinishedBytes(), nil
}

// StoreDiff stores a diff between two snapshots
func (s *SnapshotStore) StoreDiff(oldName, newName string) error {
	diff, err := s.ComputeDiff(oldName, newName)
	if err != nil {
		return err
	}

	diffName := oldName + "_" + newName
	filename := filepath.Join(s.baseDir, diffName+DiffFileExt)

	// Compress the diff
	compressed := s.encoder.EncodeAll(diff, nil)

	return os.WriteFile(filename, compressed, 0644)
}

// ApplyDiff applies a diff to a snapshot
func (s *SnapshotStore) ApplyDiff(baseName, diffName string) ([]byte, error) {
	baseData, err := s.ReadSnapshot(baseName)
	if err != nil {
		return nil, err
	}

	filename := filepath.Join(s.baseDir, diffName+DiffFileExt)
	compressed, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	// Decompress
	diffData, err := s.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}

	baseSnapshot := flashfs.GetRootAsSnapshot(baseData, 0)
	diff := flashfs.GetRootAsDiff(diffData, 0)

	// Create a map of base entries
	baseEntries := make(map[string]*flashfs.FileEntry)
	for i := 0; i < baseSnapshot.EntriesLength(); i++ {
		var entry flashfs.FileEntry
		if baseSnapshot.Entries(&entry, i) {
			entryCopy := new(flashfs.FileEntry)
			*entryCopy = entry
			baseEntries[string(entry.Path())] = entryCopy
		}
	}

	// Apply diff entries
	for i := 0; i < diff.EntriesLength(); i++ {
		var diffEntry flashfs.DiffEntry
		if !diff.Entries(&diffEntry, i) {
			continue
		}

		path := string(diffEntry.Path())
		entryType := diffEntry.Type()

		switch entryType {
		case 0, 1: // Added or Modified
			// For both added and modified, we create a new entry with updated data
			// The difference is that for added entries, we won't have an existing entry
			isDir := false
			if entryType == 1 { // Modified - try to get isDir from existing entry
				if existingEntry, ok := baseEntries[path]; ok {
					isDir = existingEntry.IsDir()
				}
			}

			// We need to build a complete new snapshot entry from scratch
			// to avoid any FlatBuffers memory issues
			builder := flatbuffers.NewBuilder(0)

			// Create string and vector offsets first
			pathOffset := builder.CreateString(path)

			// Hash bytes
			var hashOffset flatbuffers.UOffsetT
			newHashBytes := diffEntry.NewHashBytes()
			if len(newHashBytes) > 0 {
				hashOffset = builder.CreateByteVector(newHashBytes)
			}

			// Create the FileEntry
			flashfs.FileEntryStart(builder)
			flashfs.FileEntryAddPath(builder, pathOffset)
			flashfs.FileEntryAddSize(builder, diffEntry.NewSize())
			flashfs.FileEntryAddMtime(builder, diffEntry.NewMtime())
			flashfs.FileEntryAddIsDir(builder, isDir)
			flashfs.FileEntryAddPermissions(builder, diffEntry.NewPermissions())
			if len(newHashBytes) > 0 {
				flashfs.FileEntryAddHash(builder, hashOffset)
			}

			// Get the offset of the FileEntry we just created
			fileEntry := flashfs.FileEntryEnd(builder)

			// Finish the buffer with the FileEntry
			builder.Finish(fileEntry)

			// Get the finished bytes
			bytes := builder.FinishedBytes()

			// Create a fresh FileEntry object
			entry := new(flashfs.FileEntry)
			entry.Init(bytes, flatbuffers.GetUOffsetT(bytes))

			// Store the entry in our map
			baseEntries[path] = entry

		case 2: // Deleted
			// Remove the entry from the map
			delete(baseEntries, path)
		}
	}

	// Build new snapshot
	builder := flatbuffers.NewBuilder(1024)
	fileEntryOffsets := make([]flatbuffers.UOffsetT, 0, len(baseEntries))

	for _, entry := range baseEntries {
		pathOffset := builder.CreateString(string(entry.Path()))
		var hashOffset flatbuffers.UOffsetT
		hashBytes := entry.HashBytes()
		if len(hashBytes) > 0 {
			hashOffset = builder.CreateByteVector(hashBytes)
		}

		flashfs.FileEntryStart(builder)
		flashfs.FileEntryAddPath(builder, pathOffset)
		flashfs.FileEntryAddSize(builder, entry.Size())
		flashfs.FileEntryAddMtime(builder, entry.Mtime())
		flashfs.FileEntryAddIsDir(builder, entry.IsDir())
		flashfs.FileEntryAddPermissions(builder, entry.Permissions())
		if len(hashBytes) > 0 {
			flashfs.FileEntryAddHash(builder, hashOffset)
		}
		fileEntryOffsets = append(fileEntryOffsets, flashfs.FileEntryEnd(builder))
	}

	// Create vector of file entries
	flashfs.SnapshotStartEntriesVector(builder, len(fileEntryOffsets))
	for i := len(fileEntryOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(fileEntryOffsets[i])
	}
	entriesVector := builder.EndVector(len(fileEntryOffsets))

	flashfs.SnapshotStart(builder)
	flashfs.SnapshotAddEntries(builder, entriesVector)
	snapshot := flashfs.SnapshotEnd(builder)
	builder.Finish(snapshot)

	return builder.FinishedBytes(), nil
}

// QuerySnapshot queries a snapshot for entries matching criteria
func (s *SnapshotStore) QuerySnapshot(name string, filter func(*flashfs.FileEntry) bool) ([]*flashfs.FileEntry, error) {
	data, err := s.ReadSnapshot(name)
	if err != nil {
		return nil, err
	}

	snapshot := flashfs.GetRootAsSnapshot(data, 0)
	results := make([]*flashfs.FileEntry, 0)

	for i := 0; i < snapshot.EntriesLength(); i++ {
		var entry flashfs.FileEntry
		if snapshot.Entries(&entry, i) {
			if filter == nil || filter(&entry) {
				// Create a copy of the entry to return
				entryCopy := new(flashfs.FileEntry)
				*entryCopy = entry
				results = append(results, entryCopy)
			}
		}
	}

	return results, nil
}

// SetCacheSize sets the maximum number of snapshots to cache
func (s *SnapshotStore) SetCacheSize(size int) {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	s.cacheSize = size

	// Trim cache if needed
	for len(s.cacheKeys) > s.cacheSize {
		oldestKey := s.cacheKeys[0]
		s.cacheKeys = s.cacheKeys[1:]
		delete(s.snapshotCache, oldestKey)
	}
}

// cacheSnapshot adds a snapshot to the cache
func (s *SnapshotStore) cacheSnapshot(name string, data []byte) {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()

	// Check if already in cache
	for _, key := range s.cacheKeys {
		if key == name {
			// Move to end (most recently used)
			for i := range s.cacheKeys {
				if s.cacheKeys[i] == name {
					s.cacheKeys = append(s.cacheKeys[:i], s.cacheKeys[i+1:]...)
					break
				}
			}
			s.cacheKeys = append(s.cacheKeys, name)
			s.snapshotCache[name] = data
			return
		}
	}

	// Add to cache
	if len(s.cacheKeys) >= s.cacheSize {
		// Remove oldest
		oldestKey := s.cacheKeys[0]
		s.cacheKeys = s.cacheKeys[1:]
		delete(s.snapshotCache, oldestKey)
	}

	s.cacheKeys = append(s.cacheKeys, name)
	s.snapshotCache[name] = data
}

// entriesDiffer checks if two file entries are different
func entriesDiffer(a, b *flashfs.FileEntry) bool {
	if a.Size() != b.Size() ||
		a.Mtime() != b.Mtime() ||
		a.IsDir() != b.IsDir() ||
		a.Permissions() != b.Permissions() {
		return true
	}

	// Compare hashes if available
	aHash := a.HashBytes()
	bHash := b.HashBytes()
	if len(aHash) > 0 && len(bHash) > 0 {
		return !bytes.Equal(aHash, bHash)
	}

	// If we can't compare hashes, assume they're different
	return true
}

// BloomFilter implements a simple bloom filter for quick file existence checks
type BloomFilter struct {
	bits    []byte
	numHash uint
}

// NewBloomFilter creates a new bloom filter
func NewBloomFilter(size uint, numHash uint) *BloomFilter {
	return &BloomFilter{
		bits:    make([]byte, (size+7)/8),
		numHash: numHash,
	}
}

// Add adds an item to the bloom filter
func (b *BloomFilter) Add(data []byte) {
	h1 := xxhash.Sum64(data)
	h2 := murmur3.Sum64(data)

	for i := uint(0); i < b.numHash; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(len(b.bits)*8)
		b.bits[idx/8] |= 1 << (idx % 8)
	}
}

// Contains checks if an item might be in the bloom filter
func (b *BloomFilter) Contains(data []byte) bool {
	h1 := xxhash.Sum64(data)
	h2 := murmur3.Sum64(data)

	for i := uint(0); i < b.numHash; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(len(b.bits)*8)
		if b.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}

	return true
}

// CreateBloomFilterFromSnapshot creates a bloom filter from a snapshot
func (s *SnapshotStore) CreateBloomFilterFromSnapshot(name string) (*BloomFilter, error) {
	data, err := s.ReadSnapshot(name)
	if err != nil {
		return nil, err
	}

	snapshot := flashfs.GetRootAsSnapshot(data, 0)
	filter := NewBloomFilter(uint(snapshot.EntriesLength()*10), 4)

	for i := 0; i < snapshot.EntriesLength(); i++ {
		var entry flashfs.FileEntry
		if snapshot.Entries(&entry, i) {
			filter.Add(entry.Path())
		}
	}

	return filter, nil
}

// SetExpiryPolicy sets the expiry policy for the snapshot store
func (s *SnapshotStore) SetExpiryPolicy(policy ExpiryPolicy) {
	s.cacheMutex.Lock()
	defer s.cacheMutex.Unlock()
	s.expiryPolicy = policy
}

// GetExpiryPolicy returns the current expiry policy
func (s *SnapshotStore) GetExpiryPolicy() ExpiryPolicy {
	s.cacheMutex.RLock()
	defer s.cacheMutex.RUnlock()
	return s.expiryPolicy
}

// ApplyExpiryPolicy applies the current expiry policy to all snapshots
func (s *SnapshotStore) ApplyExpiryPolicy() (int, error) {
	// List all snapshots
	snapshots, err := s.ListSnapshots()
	if err != nil {
		return 0, fmt.Errorf("failed to list snapshots: %w", err)
	}

	// Get snapshot info
	snapshotInfos := make([]SnapshotInfo, 0, len(snapshots))
	for _, name := range snapshots {
		// Get snapshot timestamp
		timestamp, err := ParseSnapshotName(name)
		if err != nil {
			// Skip snapshots with invalid names
			continue
		}

		// Get snapshot size
		path := filepath.Join(s.baseDir, name)
		info, err := os.Stat(path)
		if err != nil {
			// Skip snapshots that can't be accessed
			continue
		}

		snapshotInfos = append(snapshotInfos, SnapshotInfo{
			Name:      name,
			Timestamp: timestamp,
			Size:      info.Size(),
		})
	}

	// Apply expiry policy
	s.cacheMutex.RLock()
	policy := s.expiryPolicy
	s.cacheMutex.RUnlock()

	toDelete := ApplyExpiryPolicy(snapshotInfos, policy)

	// Delete expired snapshots
	deleted := 0
	for _, name := range toDelete {
		if err := s.DeleteSnapshot(name); err != nil {
			// Continue even if some snapshots can't be deleted
			continue
		}
		deleted++
	}

	return deleted, nil
}

// CreateSnapshot creates a new snapshot with the given data and applies the expiry policy
func (s *SnapshotStore) CreateSnapshot(name string, data []byte) error {
	// Generate timestamp-based name if not provided
	if name == "" {
		now := time.Now()
		name = fmt.Sprintf("snapshot-%s-%s.snap",
			now.Format("20060102"),
			now.Format("150405"))
	}

	// Write the snapshot
	if err := s.WriteSnapshot(name, data); err != nil {
		return err
	}

	// Apply expiry policy
	_, err := s.ApplyExpiryPolicy()
	return err
}
