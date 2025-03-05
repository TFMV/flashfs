package serializer

import (
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
		var hashOffset flatbuffers.UOffsetT
		if len(entry.Hash) > 0 {
			hashOffset = builder.CreateByteVector(entry.Hash)
		}

		schema.FileEntryStart(builder)
		schema.FileEntryAddPath(builder, pathOffset)
		schema.FileEntryAddSize(builder, entry.Size)
		schema.FileEntryAddMtime(builder, entry.ModTime)
		schema.FileEntryAddIsDir(builder, entry.IsDir)
		schema.FileEntryAddPermissions(builder, entry.Permissions)
		if len(entry.Hash) > 0 {
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
