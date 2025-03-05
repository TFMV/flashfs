# Storage

The Storage component is the core of FlashFS, responsible for managing snapshots, diffs, and providing efficient access to stored data. It handles compression, caching, and lifecycle management of snapshots.

## Overview

The Storage component provides a robust API for:

- Writing and reading snapshots
- Computing and applying diffs between snapshots
- Querying snapshot contents
- Managing snapshot lifecycle through expiry policies
- Optimizing performance through caching and Bloom filters

## SnapshotStore

The primary interface to the Storage component is the `SnapshotStore` struct:

```go
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
```

### Initialization

Create a new `SnapshotStore` with:

```go
store, err := storage.NewSnapshotStore("/path/to/snapshots")
if err != nil {
    return err
}
defer store.Close()
```

This initializes a store with:

- Default compression level (Zstd level 3)
- Default cache size (10 snapshots)
- Default expiry policy

## Core Functions

### Snapshot Management

```go
// Create a new snapshot
err := store.CreateSnapshot("backup-20230101.snap", snapshotData)

// Read a snapshot
data, err := store.ReadSnapshot("backup-20230101.snap")

// List all snapshots
snapshots, err := store.ListSnapshots()

// Delete a snapshot
err := store.DeleteSnapshot("backup-20230101.snap")
```

### Diff Operations

```go
// Compute a diff between two snapshots
// Returns a serialized Diff object containing DiffEntry items
diffData, err := store.ComputeDiff("backup-20230101.snap", "backup-20230102.snap")

// Store a diff to a file
err := store.StoreDiff("backup-20230101.snap", "backup-20230102.snap")

// Apply a diff to generate a new snapshot
// Processes each DiffEntry based on its type (added, modified, deleted)
newSnapshotData, err := store.ApplyDiff("backup-20230101.snap", "diff-20230101-20230102.diff")
```

The diff operations work with structured Diff and DiffEntry types:

```go
// DiffEntry represents a single changed file
// type field indicates: 0 = added, 1 = modified, 2 = deleted
type DiffEntry struct {
    path           string
    type           byte
    oldSize        int64
    newSize        int64
    oldMtime       int64
    newMtime       int64
    oldPermissions uint32
    newPermissions uint32
    oldHash        []byte
    newHash        []byte
}

// Diff contains all changes between two snapshots
type Diff struct {
    entries []DiffEntry
}
```

The `ComputeDiff` function compares two snapshots and creates a structured diff with:

- Added files (type = 0)
- Modified files (type = 1)
- Deleted files (type = 2)

The `ApplyDiff` function processes each DiffEntry based on its type to:

- Add new files to the base snapshot
- Update modified files in the base snapshot
- Remove deleted files from the base snapshot

### Querying

```go
// Query snapshot contents with a filter function
entries, err := store.QuerySnapshot("backup-20230101.snap", func(entry *flashfs.FileEntry) bool {
    // Return true for entries to include
    return strings.HasPrefix(entry.Path(), "/home/user/documents/")
})
```

### Expiry Policy Management

```go
// Set an expiry policy
store.SetExpiryPolicy(storage.ExpiryPolicy{
    MaxSnapshots: 50,
    MaxAge:       30 * 24 * time.Hour,
    KeepHourly:   24,
    KeepDaily:    7,
})

// Get the current expiry policy
policy := store.GetExpiryPolicy()

// Apply the expiry policy to clean up old snapshots
deleted, err := store.ApplyExpiryPolicy()
```

## Caching

The Storage component implements an LRU (Least Recently Used) cache for snapshots:

```go
// Set the cache size
store.SetCacheSize(20)

// The cache is automatically managed when reading snapshots
data, err := store.ReadSnapshot("backup-20230101.snap") // Automatically cached

// Subsequent reads of the same snapshot will use the cached version
data, err = store.ReadSnapshot("backup-20230101.snap") // Retrieved from cache
```

## Bloom Filters

Bloom filters are used to quickly identify changed files without full snapshot comparison:

```go
// Create a Bloom filter from a snapshot
filter, err := store.CreateBloomFilterFromSnapshot("backup-20230101.snap")

// Use the filter to check if a file might have changed
mightContain := filter.Contains([]byte("/path/to/file"))
```

## File Format

### Snapshots

Snapshots are stored with the `.snap` extension and contain:

1. Serialized file metadata in FlatBuffers format
2. Compressed using Zstd

### Diffs

Diffs are stored with the `.diff` extension and contain:

1. Serialized Diff object with DiffEntry items for added, modified, and deleted files
2. Each DiffEntry contains a type field and relevant metadata for the change
3. Compressed using Zstd

## Performance Considerations

The Storage component is optimized for:

- **Read Performance**: Uses caching to minimize disk I/O for frequently accessed snapshots
- **Write Performance**: Uses efficient compression to balance speed and size
- **Memory Usage**: Controls memory consumption through configurable cache size
- **Comparison Speed**: Uses Bloom filters to accelerate diff operations
- **Diff Processing**: Structured diff format enables efficient application of changes

## Implementation Details

### Compression

FlashFS uses Zstd compression with configurable levels:

- Default level: 3 (good balance of speed and compression ratio)
- Faster compression: Use lower levels (1-2)
- Better compression ratio: Use higher levels (4-9)

### Thread Safety

The Storage component is thread-safe:

- Uses read/write mutexes for cache access
- Safe for concurrent reads
- Serializes writes to prevent corruption

### Error Handling

The Storage component provides detailed error information:

- File not found errors
- Permission errors
- Corruption detection
- Version incompatibility

## Advanced Usage

### Custom Compression

```go
// Create a store with custom compression level
options := storage.Options{
    CompressionLevel: 9, // Maximum compression
}
store, err := storage.NewSnapshotStoreWithOptions("/path/to/snapshots", options)
```

### Snapshot Metadata

```go
// Get metadata about snapshots without loading full contents
metadata, err := store.GetSnapshotMetadata("backup-20230101.snap")
fmt.Printf("Size: %d bytes, Created: %s\n", metadata.Size, metadata.Timestamp)
```
