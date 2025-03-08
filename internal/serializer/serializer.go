package serializer

import (
	"encoding/hex"

	"github.com/TFMV/flashfs/internal/walker"
	schema "github.com/TFMV/flashfs/schema/flashfs"

	flatbuffers "github.com/google/flatbuffers/go"
)

// SerializeSnapshot serializes snapshot entries to FlatBuffers.
func SerializeSnapshot(entries []walker.SnapshotEntry) ([]byte, error) {
	builder := flatbuffers.NewBuilder(1024)
	fileEntryOffsets := make([]flatbuffers.UOffsetT, len(entries))
	for i, entry := range entries {
		pathOffset := builder.CreateString(entry.Path)

		// Convert string hash to byte slice if present
		var hashOffset flatbuffers.UOffsetT
		if entry.Hash != "" {
			// Convert hex string to byte slice
			hashBytes, err := hex.DecodeString(entry.Hash)
			if err == nil && len(hashBytes) > 0 {
				hashOffset = builder.CreateByteVector(hashBytes)
			}
		}

		schema.FileEntryStart(builder)
		schema.FileEntryAddPath(builder, pathOffset)
		schema.FileEntryAddSize(builder, entry.Size)
		// Convert time.Time to Unix timestamp (int64)
		schema.FileEntryAddMtime(builder, entry.ModTime.Unix())
		schema.FileEntryAddIsDir(builder, entry.IsDir)
		// Convert fs.FileMode to uint32
		schema.FileEntryAddPermissions(builder, uint32(entry.Permissions))
		if entry.Hash != "" {
			schema.FileEntryAddHash(builder, hashOffset)
		}
		fileEntryOffsets[i] = schema.FileEntryEnd(builder)
	}

	// Create vector of file entries.
	schema.SnapshotStartEntriesVector(builder, len(fileEntryOffsets))
	for i := len(fileEntryOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(fileEntryOffsets[i])
	}
	entriesVector := builder.EndVector(len(fileEntryOffsets))

	schema.SnapshotStart(builder)
	schema.SnapshotAddEntries(builder, entriesVector)
	snapshot := schema.SnapshotEnd(builder)
	builder.Finish(snapshot)
	return builder.FinishedBytes(), nil
}
