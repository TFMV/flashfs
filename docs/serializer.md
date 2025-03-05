# Serializer

The Serializer component in FlashFS is responsible for efficiently converting file system metadata into a compact binary format for storage and transmission. It plays a crucial role in ensuring that snapshots are both space-efficient and quick to process.

## Overview

The Serializer uses [FlatBuffers](https://google.github.io/flatbuffers/) as its serialization framework, which provides several advantages:

- **Zero-copy deserialization**: Data can be accessed without unpacking/parsing
- **Cross-platform compatibility**: Consistent representation across different systems
- **Compact binary format**: Minimizes storage requirements
- **Schema evolution**: Allows for backward compatibility as the schema evolves

## Schema Definition

The serialization schema is defined in FlatBuffers IDL (Interface Definition Language) and includes the following main components:

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

root_type Snapshot;
```

This schema defines:

- A `FileEntry` table representing metadata for a single file or directory
- A `Snapshot` table containing a list of file entries
- The root type as `Snapshot`

## Core Functions

### SerializeSnapshot

The `SerializeSnapshot` function converts a list of file system entries into a binary representation:

```go
func SerializeSnapshot(entries []walker.SnapshotEntry) ([]byte, error)
```

This function:

1. Creates a new FlatBuffers builder
2. Converts each entry into a FlatBuffers `FileEntry`
3. Builds a vector of all file entries
4. Creates a `Snapshot` containing the entries vector
5. Returns the serialized binary data

### DeserializeSnapshot

The `DeserializeSnapshot` function converts binary data back into a usable format:

```go
func DeserializeSnapshot(data []byte) (*flashfs.Snapshot, error)
```

This function:

1. Verifies the binary data
2. Creates a FlatBuffers accessor for the data
3. Returns a reference to the snapshot without copying the data

## Performance Considerations

The Serializer is designed for high performance:

- **Memory Efficiency**: Minimizes memory allocations during serialization
- **Processing Speed**: Optimized for fast serialization and deserialization
- **Size Optimization**: Creates compact representations of file metadata

## Integration with Other Components

The Serializer integrates with:

- **Walker**: Receives file metadata from the Walker component
- **Storage**: Provides serialized data to the Storage component for compression and persistence
- **Diff**: Enables efficient comparison by providing a consistent representation format

## Example Usage

```go
// Get file metadata from the Walker
entries, err := walker.Walk("/path/to/directory")
if err != nil {
    return err
}

// Serialize the entries
serializedData, err := serializer.SerializeSnapshot(entries)
if err != nil {
    return err
}

// The serialized data can now be compressed and stored
```

## Implementation Details

The Serializer implementation handles several edge cases:

- **Large Files**: Properly represents files of any size using 64-bit integers
- **Unicode Paths**: Correctly handles international characters in file paths
- **Empty Directories**: Preserves empty directories in the snapshot
- **Missing Hashes**: Gracefully handles entries without content hashes
- **Schema Versioning**: Maintains compatibility across different versions

## Advanced Usage

### Custom Serialization

For specialized use cases, the Serializer can be extended:

```go
// Create a custom serializer with specific options
options := serializer.Options{
    IncludeHashes: true,
    CompactPaths: false,
}
serializedData, err := serializer.SerializeSnapshotWithOptions(entries, options)
```

### Partial Deserialization

For efficiency when only specific data is needed:

```go
// Access only the paths without deserializing everything
paths, err := serializer.ExtractPathsFromSnapshot(serializedData)
```
