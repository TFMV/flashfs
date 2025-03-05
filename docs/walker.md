# Walker

The Walker component in FlashFS is responsible for traversing file systems and collecting metadata about files and directories. It provides the foundation for creating snapshots by efficiently gathering information about the file system structure.

## Overview

The Walker component:

- Traverses directories recursively
- Collects metadata about files and directories
- Computes content hashes for files
- Handles errors and edge cases during traversal
- Supports context-based cancellation

## Core Data Structure

The primary data structure used by the Walker is the `SnapshotEntry`:

```go
type SnapshotEntry struct {
    Path        string
    Size        int64
    ModTime     int64
    IsDir       bool
    Permissions uint32
    Hash        []byte
}
```

This structure captures essential information about each file or directory:

- **Path**: The relative path within the snapshot
- **Size**: File size in bytes (0 for directories)
- **ModTime**: Modification time as Unix timestamp
- **IsDir**: Whether the entry is a directory
- **Permissions**: File permissions as a uint32
- **Hash**: BLAKE3 hash of the file contents (nil for directories)

## Core Functions

### Walk

The primary function for traversing a file system:

```go
func Walk(root string) ([]SnapshotEntry, error)
```

This function:

1. Starts at the specified root directory
2. Recursively traverses all subdirectories
3. Collects metadata for each file and directory
4. Returns a slice of `SnapshotEntry` structures

### WalkWithContext

A context-aware version of the Walk function:

```go
func WalkWithContext(ctx context.Context, root string) ([]SnapshotEntry, error)
```

This function behaves like `Walk` but accepts a context that can be used to cancel the operation.

### ComputeHash

Calculates the BLAKE3 hash of a file:

```go
func computeHash(path string) []byte
```

This function:

1. Opens the specified file
2. Streams its contents through the BLAKE3 hash function
3. Returns the resulting hash as a byte slice

## Performance Optimizations

The Walker component includes several optimizations:

- **Efficient Directory Traversal**: Uses the high-performance `godirwalk` library instead of the standard library's `filepath.Walk`
- **Fast Hashing**: Employs BLAKE3, a cryptographic hash function optimized for speed
- **Streaming Processing**: Computes hashes by streaming file contents rather than loading entire files into memory
- **Error Handling**: Continues traversal even when individual files have errors

## Usage Examples

### Basic Usage

```go
// Walk a directory and collect file metadata
entries, err := walker.Walk("/path/to/directory")
if err != nil {
    log.Fatalf("Failed to walk directory: %v", err)
}

// Process the entries
for _, entry := range entries {
    fmt.Printf("Path: %s, Size: %d bytes, IsDir: %v\n", 
               entry.Path, entry.Size, entry.IsDir)
}
```

### With Context for Cancellation

```go
// Create a context with cancellation
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Walk with context
entries, err := walker.WalkWithContext(ctx, "/path/to/directory")
if err != nil {
    if errors.Is(err, context.DeadlineExceeded) {
        log.Fatal("Walk operation timed out")
    }
    log.Fatalf("Failed to walk directory: %v", err)
}
```

## Error Handling

The Walker component handles various error conditions:

- **Permission Denied**: Skips files/directories that cannot be accessed
- **Non-existent Files**: Returns appropriate errors for invalid paths
- **I/O Errors**: Continues traversal even when individual files have errors
- **Context Cancellation**: Stops traversal when the context is cancelled

## Implementation Details

### Directory Traversal

The Walker uses `godirwalk` for efficient directory traversal:

```go
err := godirwalk.Walk(root, &godirwalk.Options{
    Callback: func(path string, de *godirwalk.Dirent) error {
        // Process each file/directory
        // ...
        return nil
    },
    Unsorted: true, // For better performance
})
```

### Hash Calculation

File content hashes are calculated using BLAKE3:

```go
func computeHash(path string) []byte {
    f, err := os.Open(path)
    if err != nil {
        return nil
    }
    defer f.Close()
    
    hasher := blake3.New()
    if _, err := io.Copy(hasher, f); err != nil {
        return nil
    }
    
    return hasher.Sum(nil)
}
```

## Advanced Usage

### Custom Traversal Options

For specialized use cases, the Walker can be extended with custom options:

```go
options := walker.Options{
    SkipHidden: true,
    MaxDepth: 10,
    FollowSymlinks: false,
    IncludePattern: "*.txt",
    ExcludePattern: ".git/*",
}
entries, err := walker.WalkWithOptions("/path/to/directory", options)
```

### Parallel Hashing

For improved performance on multi-core systems:

```go
options := walker.Options{
    ParallelHashing: true,
    HashWorkers: 4,
}
entries, err := walker.WalkWithOptions("/path/to/directory", options)
```

## Integration with Other Components

The Walker integrates with:

- **Serializer**: Provides file metadata to be serialized
- **Storage**: Indirectly supplies the data for snapshots
- **Diff**: Enables comparison by providing consistent metadata
