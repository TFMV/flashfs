# Schema

The Schema component in FlashFS defines the data structures used for serialization and deserialization of snapshots and diffs. It uses FlatBuffers to create an efficient, cross-platform binary representation of file system metadata.

## Overview

The Schema component:

- Defines the structure of snapshots and diffs
- Enables efficient binary serialization
- Provides zero-copy access to serialized data
- Ensures cross-platform compatibility
- Allows for schema evolution over time

## FlatBuffers Schema Definition

FlashFS uses FlatBuffers as its serialization framework. The schema is defined in the FlatBuffers Interface Definition Language (IDL):

```flatbuffers
namespace flashfs;

table FileEntry {
  path: string;
  size: long;
  mtime: long;
  isDir: bool;
  permissions: uint;
  hash: [ubyte];
}

table Snapshot {
  entries: [FileEntry];
}

table DiffEntry {
  path: string;
  type: byte;  // 0 = added, 1 = modified, 2 = deleted
  oldSize: long;
  newSize: long;
  oldMtime: long;
  newMtime: long;
  oldPermissions: uint;
  newPermissions: uint;
  oldHash: [ubyte];
  newHash: [ubyte];
}

table Diff {
  entries: [DiffEntry];
}

root_type Snapshot;
```

## Schema Components

### FileEntry

The `FileEntry` table represents a single file or directory in a snapshot:

- **path**: The relative path within the snapshot
- **size**: File size in bytes (0 for directories)
- **mtime**: Modification time as Unix timestamp
- **isDir**: Whether the entry is a directory
- **permissions**: File permissions as a uint32
- **hash**: BLAKE3 hash of the file contents (empty for directories)

### Snapshot

The `Snapshot` table contains a list of file entries that represent the state of a file system at a specific point in time.

### DiffEntry

The `DiffEntry` table represents a change between two snapshots:

- **path**: The path of the changed file or directory
- **type**: The type of change (added, modified, deleted)
- **oldSize/newSize**: File size before and after the change
- **oldMtime/newMtime**: Modification time before and after the change
- **oldPermissions/newPermissions**: Permissions before and after the change
- **oldHash/newHash**: Content hash before and after the change

### Diff

The `Diff` table contains a list of diff entries that represent the changes between two snapshots.

## Generated Code

The FlatBuffers compiler generates Go code from the schema definition:

```go
// Generated code provides type-safe accessors for all fields
func (e *FileEntry) Path() string
func (e *FileEntry) Size() int64
func (e *FileEntry) Mtime() int64
func (e *FileEntry) IsDir() bool
func (e *FileEntry) Permissions() uint32
func (e *FileEntry) Hash(j int) byte
func (e *FileEntry) HashLength() int
```

## Usage Examples

### Accessing Snapshot Data

```go
// Deserialize a snapshot
snapshot := flashfs.GetRootAsSnapshot(serializedData, 0)

// Get the number of entries
entriesLength := snapshot.EntriesLength()

// Access individual entries
for i := 0; i < entriesLength; i++ {
    var entry flashfs.FileEntry
    if snapshot.Entries(&entry, i) {
        fmt.Printf("Path: %s, Size: %d bytes\n", entry.Path(), entry.Size())
        
        // Access hash bytes if available
        if entry.HashLength() > 0 {
            hash := make([]byte, entry.HashLength())
            for j := 0; j < entry.HashLength(); j++ {
                hash[j] = entry.Hash(j)
            }
            fmt.Printf("Hash: %x\n", hash)
        }
    }
}
```

### Creating Serialized Data

```go
// Create a FlatBuffers builder
builder := flatbuffers.NewBuilder(0)

// Create file entries
fileEntryOffsets := make([]flatbuffers.UOffsetT, len(entries))
for i, entry := range entries {
    // Create string and byte vector offsets
    pathOffset := builder.CreateString(entry.Path)
    hashOffset := builder.CreateByteVector(entry.Hash)
    
    // Start building a FileEntry
    flashfs.FileEntryStart(builder)
    flashfs.FileEntryAddPath(builder, pathOffset)
    flashfs.FileEntryAddSize(builder, entry.Size)
    flashfs.FileEntryAddMtime(builder, entry.ModTime)
    flashfs.FileEntryAddIsDir(builder, entry.IsDir)
    flashfs.FileEntryAddPermissions(builder, entry.Permissions)
    flashfs.FileEntryAddHash(builder, hashOffset)
    
    // Finish the FileEntry
    fileEntryOffsets[i] = flashfs.FileEntryEnd(builder)
}

// Create a vector of file entries
flashfs.SnapshotStartEntriesVector(builder, len(fileEntryOffsets))
for i := len(fileEntryOffsets) - 1; i >= 0; i-- {
    builder.PrependUOffsetT(fileEntryOffsets[i])
}
entriesVector := builder.EndVector(len(fileEntryOffsets))

// Create the snapshot
flashfs.SnapshotStart(builder)
flashfs.SnapshotAddEntries(builder, entriesVector)
snapshot := flashfs.SnapshotEnd(builder)

// Finish the builder
builder.Finish(snapshot)

// Get the serialized data
serializedData := builder.FinishedBytes()
```

## Schema Evolution

The FlatBuffers schema allows for backward-compatible evolution:

- **Adding Fields**: New fields can be added without breaking compatibility with older data
- **Deprecating Fields**: Fields can be deprecated without breaking compatibility
- **Versioning**: Schema versioning can be implemented through optional fields

## Performance Considerations

The Schema component is designed for high performance:

- **Zero-Copy Deserialization**: Accessing data doesn't require unpacking or parsing
- **Memory Efficiency**: Binary representation is compact and memory-efficient
- **Cross-Platform**: Same binary format works across different platforms
- **Fast Access**: Direct access to fields without traversing the entire structure

## Integration with Other Components

The Schema component integrates with:

- **Serializer**: Provides the structure for serialization
- **Storage**: Defines the format for stored snapshots and diffs
- **Diff**: Enables efficient representation of changes

## Advanced Topics

### Custom Schemas

For specialized use cases, the schema can be extended:

```flatbuffers
// Extended schema with additional metadata
table SnapshotWithMetadata {
  entries: [FileEntry];
  creationTime: long;
  description: string;
  tags: [string];
}
```

### Schema Versioning

To maintain compatibility across versions:

```flatbuffers
// Versioned schema
table SnapshotV2 {
  entries: [FileEntry];
  version: uint = 2;  // Default value for backward compatibility
  compressionType: byte = 0;  // New field in version 2
}
```

### Nested Structures

For more complex data representation:

```flatbuffers
// Nested structures
table Directory {
  path: string;
  files: [FileEntry];
  subdirectories: [Directory];
}

table HierarchicalSnapshot {
  rootDirectory: Directory;
}
```
