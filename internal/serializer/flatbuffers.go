package serializer

import (
	"encoding/hex"
	"fmt"
	"io/fs"
	"sync"
	"time"

	"github.com/TFMV/flashfs/internal/walker"
	"github.com/TFMV/flashfs/schema/flashfs"
	flatbuffers "github.com/google/flatbuffers/go"
)

// DiffType represents the type of change in a diff entry
type DiffType int8

const (
	// DiffAdded represents a file that was added
	DiffAdded DiffType = iota
	// DiffModified represents a file that was modified
	DiffModified
	// DiffDeleted represents a file that was deleted
	DiffDeleted
)

// DiffEntry represents a difference between two snapshots
type DiffEntry struct {
	Path           string
	Type           DiffType
	OldSize        int64
	NewSize        int64
	OldModTime     time.Time
	NewModTime     time.Time
	OldPermissions uint32
	NewPermissions uint32
	OldHash        string
	NewHash        string
	IsDir          bool // Default to false since we can't access it from the generated code
}

// builderPool is a pool of FlatBuffers builders to reduce allocations
var builderPool = sync.Pool{
	New: func() interface{} {
		return flatbuffers.NewBuilder(0)
	},
}

// getBuilder gets a builder from the pool or creates a new one
func getBuilder() *flatbuffers.Builder {
	return builderPool.Get().(*flatbuffers.Builder)
}

// putBuilder returns a builder to the pool
func putBuilder(builder *flatbuffers.Builder) {
	builder.Reset()
	builderPool.Put(builder)
}

// hexToBytes converts a hex string to a byte slice
func hexToBytes(hexStr string) ([]byte, error) {
	return hex.DecodeString(hexStr)
}

// bytesToHex converts a byte slice to a hex string
func bytesToHex(bytes []byte) string {
	return hex.EncodeToString(bytes)
}

// SerializeSnapshotFB serializes a snapshot using FlatBuffers
// If a builder is provided, it will be reused; otherwise, a new one will be created
func SerializeSnapshotFB(entries []walker.SnapshotEntry, builder *flatbuffers.Builder) ([]byte, error) {
	// Get or create a builder
	builderOwned := builder == nil
	if builderOwned {
		builder = getBuilder()
		defer putBuilder(builder)
	} else {
		builder.Reset()
	}

	// Create file entries
	fileEntryOffsets := make([]flatbuffers.UOffsetT, 0, len(entries))
	for _, entry := range entries {
		// Skip entries with errors
		if entry.Error != nil {
			continue
		}

		// Create path string
		pathOffset := builder.CreateString(entry.Path)

		// Create hash byte array if present
		var hashOffset flatbuffers.UOffsetT
		if entry.Hash != "" {
			// Convert hex string to byte array
			hashBytes, err := hexToBytes(entry.Hash)
			if err != nil {
				return nil, fmt.Errorf("failed to convert hash to bytes: %w", err)
			}
			hashOffset = builder.CreateByteVector(hashBytes)
		}

		// Start building the file entry
		flashfs.FileEntryStart(builder)
		flashfs.FileEntryAddPath(builder, pathOffset)
		flashfs.FileEntryAddSize(builder, entry.Size)
		flashfs.FileEntryAddMtime(builder, entry.ModTime.Unix())
		flashfs.FileEntryAddIsDir(builder, entry.IsDir)
		flashfs.FileEntryAddPermissions(builder, uint32(entry.Permissions))
		if entry.Hash != "" {
			flashfs.FileEntryAddHash(builder, hashOffset)
		}
		fileEntryOffset := flashfs.FileEntryEnd(builder)
		fileEntryOffsets = append(fileEntryOffsets, fileEntryOffset)
	}

	// Create entries vector
	flashfs.SnapshotStartEntriesVector(builder, len(fileEntryOffsets))
	for i := len(fileEntryOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(fileEntryOffsets[i])
	}
	entriesOffset := builder.EndVector(len(fileEntryOffsets))

	// Create snapshot
	flashfs.SnapshotStart(builder)
	flashfs.SnapshotAddEntries(builder, entriesOffset)
	snapshotOffset := flashfs.SnapshotEnd(builder)

	builder.Finish(snapshotOffset)
	return builder.FinishedBytes(), nil
}

// DeserializeSnapshotFB deserializes a snapshot using FlatBuffers
func DeserializeSnapshotFB(data []byte) ([]walker.SnapshotEntry, error) {
	snapshot := flashfs.GetRootAsSnapshot(data, 0)

	// Get the number of entries
	entriesLength := snapshot.EntriesLength()
	entries := make([]walker.SnapshotEntry, entriesLength)

	// Deserialize each entry
	for i := 0; i < entriesLength; i++ {
		var entry flashfs.FileEntry
		if !snapshot.Entries(&entry, i) {
			return nil, fmt.Errorf("failed to get file entry at index %d", i)
		}

		// Get the path
		path := string(entry.Path())

		// Get the hash if present
		var hash string
		if entry.HashLength() > 0 {
			hashBytes := make([]byte, entry.HashLength())
			for j := 0; j < entry.HashLength(); j++ {
				hashBytes[j] = entry.Hash(j)
			}
			hash = bytesToHex(hashBytes)
		}

		// Create the snapshot entry
		entries[i] = walker.SnapshotEntry{
			Path:        path,
			Size:        entry.Size(),
			ModTime:     time.Unix(entry.Mtime(), 0),
			IsDir:       entry.IsDir(),
			Permissions: fs.FileMode(entry.Permissions()),
			Hash:        hash,
		}
	}

	return entries, nil
}

// SerializeDiffFB serializes a diff using FlatBuffers
// If a builder is provided, it will be reused; otherwise, a new one will be created
func SerializeDiffFB(entries []DiffEntry, builder *flatbuffers.Builder) ([]byte, error) {
	// Get or create a builder
	builderOwned := builder == nil
	if builderOwned {
		builder = getBuilder()
		defer putBuilder(builder)
	} else {
		builder.Reset()
	}

	// Create diff entries
	diffEntryOffsets := make([]flatbuffers.UOffsetT, 0, len(entries))
	for _, entry := range entries {
		// Create path string
		pathOffset := builder.CreateString(entry.Path)

		// Create old hash byte array if present
		var oldHashOffset flatbuffers.UOffsetT
		if entry.OldHash != "" {
			// Convert hex string to byte array
			oldHashBytes, err := hexToBytes(entry.OldHash)
			if err != nil {
				return nil, fmt.Errorf("failed to convert old hash to bytes: %w", err)
			}
			oldHashOffset = builder.CreateByteVector(oldHashBytes)
		}

		// Create new hash byte array if present
		var newHashOffset flatbuffers.UOffsetT
		if entry.NewHash != "" {
			// Convert hex string to byte array
			newHashBytes, err := hexToBytes(entry.NewHash)
			if err != nil {
				return nil, fmt.Errorf("failed to convert new hash to bytes: %w", err)
			}
			newHashOffset = builder.CreateByteVector(newHashBytes)
		}

		// Start building the diff entry
		flashfs.DiffEntryStart(builder)
		flashfs.DiffEntryAddPath(builder, pathOffset)
		flashfs.DiffEntryAddType(builder, int8(entry.Type))
		flashfs.DiffEntryAddOldSize(builder, entry.OldSize)
		flashfs.DiffEntryAddNewSize(builder, entry.NewSize)
		flashfs.DiffEntryAddOldMtime(builder, entry.OldModTime.Unix())
		flashfs.DiffEntryAddNewMtime(builder, entry.NewModTime.Unix())
		flashfs.DiffEntryAddOldPermissions(builder, entry.OldPermissions)
		flashfs.DiffEntryAddNewPermissions(builder, entry.NewPermissions)
		if entry.OldHash != "" {
			flashfs.DiffEntryAddOldHash(builder, oldHashOffset)
		}
		if entry.NewHash != "" {
			flashfs.DiffEntryAddNewHash(builder, newHashOffset)
		}
		diffEntryOffset := flashfs.DiffEntryEnd(builder)
		diffEntryOffsets = append(diffEntryOffsets, diffEntryOffset)
	}

	// Create entries vector
	flashfs.DiffStartEntriesVector(builder, len(diffEntryOffsets))
	for i := len(diffEntryOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(diffEntryOffsets[i])
	}
	entriesOffset := builder.EndVector(len(diffEntryOffsets))

	// Create diff
	flashfs.DiffStart(builder)
	flashfs.DiffAddEntries(builder, entriesOffset)
	diffOffset := flashfs.DiffEnd(builder)

	builder.Finish(diffOffset)
	return builder.FinishedBytes(), nil
}

// DeserializeDiffFB deserializes a diff using FlatBuffers
func DeserializeDiffFB(data []byte) ([]DiffEntry, error) {
	diff := flashfs.GetRootAsDiff(data, 0)

	// Get the number of entries
	entriesLength := diff.EntriesLength()
	entries := make([]DiffEntry, entriesLength)

	// Deserialize each entry
	for i := 0; i < entriesLength; i++ {
		var entry flashfs.DiffEntry
		if !diff.Entries(&entry, i) {
			return nil, fmt.Errorf("failed to get diff entry at index %d", i)
		}

		// Get the path
		path := string(entry.Path())

		// Get the old hash if present
		var oldHash string
		if entry.OldHashLength() > 0 {
			oldHashBytes := make([]byte, entry.OldHashLength())
			for j := 0; j < entry.OldHashLength(); j++ {
				oldHashBytes[j] = entry.OldHash(j)
			}
			oldHash = bytesToHex(oldHashBytes)
		}

		// Get the new hash if present
		var newHash string
		if entry.NewHashLength() > 0 {
			newHashBytes := make([]byte, entry.NewHashLength())
			for j := 0; j < entry.NewHashLength(); j++ {
				newHashBytes[j] = entry.NewHash(j)
			}
			newHash = bytesToHex(newHashBytes)
		}

		// Create the diff entry
		entries[i] = DiffEntry{
			Path:           path,
			Type:           DiffType(entry.Type()),
			OldSize:        entry.OldSize(),
			NewSize:        entry.NewSize(),
			OldModTime:     time.Unix(entry.OldMtime(), 0),
			NewModTime:     time.Unix(entry.NewMtime(), 0),
			OldPermissions: entry.OldPermissions(),
			NewPermissions: entry.NewPermissions(),
			OldHash:        oldHash,
			NewHash:        newHash,
			IsDir:          entry.IsDir(),
		}
	}

	return entries, nil
}
