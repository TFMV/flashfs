# FlashFS

[![Build and Test](https://github.com/TFMV/flashfs/actions/workflows/build.yml/badge.svg)](https://github.com/TFMV/flashfs/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/TFMV/flashfs/branch/main/graph/badge.svg)](https://codecov.io/gh/TFMV/flashfs)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-blue.svg)](https://golang.org/doc/go1.24)
[![Go Report Card](https://goreportcard.com/badge/github.com/TFMV/flashfs)](https://goreportcard.com/report/github.com/TFMV/flashfs)
[![Go Reference](https://pkg.go.dev/badge/github.com/TFMV/flashfs.svg)](https://pkg.go.dev/github.com/TFMV/flashfs)
[![Documentation](https://img.shields.io/badge/docs-website-blue)](https://tfmv.github.io/flashfs/)

FlashFS is a high-performance file system snapshot and comparison tool designed for large-scale environments. It efficiently captures file system metadata, stores it in a compact format, and provides fast comparison capabilities.

## Features

- **Blazing Fast Snapshots**: Efficiently captures file system metadata with minimal overhead
- **Streaming Directory Walker**: Process files as they're discovered for improved memory efficiency and responsiveness
- **Incremental Snapshots**: Stores only changes between snapshots to minimize storage requirements
- **Efficient Storage**: Uses FlatBuffers for compact binary representation and Zstd compression
- **In-Memory Caching**: Implements LRU caching for frequently accessed snapshots
- **Bloom Filters**: Quickly identifies modified files without full snapshot comparison
- **Enhanced Diff Generation**: Creates, stores, and applies optimized diffs between snapshots with parallel processing
- **Snapshot Expiry Policies**: Automatically manages snapshot lifecycle with configurable retention policies
- **Cloud Storage Integration**: Export and restore snapshots to/from S3, GCS, or compatible object storage
- **Progress Reporting**: Real-time progress bars and statistics for long-running operations
- **Graceful Cancellation**: All operations can be safely interrupted with Ctrl+C

## Installation

```bash
go install github.com/TFMV/flashfs@latest
```

Or build from source:

```bash
git clone https://github.com/TFMV/flashfs.git
cd flashfs
go build -o flashfs
```

## Usage

### Taking a Snapshot

Standard snapshot:

```bash
flashfs snapshot --path /path/to/directory --output snapshot.snap
```

Streaming snapshot with progress reporting (for large directories):

```bash
flashfs stream snapshot --source /path/to/large/directory --output snapshot.snap
```

### Comparing Snapshots

Standard diff:

```bash
flashfs diff --base snapshot1.snap --target snapshot2.snap --output diff.diff
```

Streaming diff with progress reporting (for large snapshots):

```bash
flashfs stream diff --base snapshot1.snap --target snapshot2.snap --output diff.diff
```

For detailed comparison with additional options:

```bash
flashfs diff --base snapshot1.snap --target snapshot2.snap --output diff.diff --detailed --parallel 4
```

### Applying a Diff

```bash
flashfs apply --base snapshot1.snap --diff diff.diff --output snapshot2.snap
```

### Querying a Snapshot

Basic query with pattern matching:

```bash
flashfs query --dir ~/.flashfs --snapshot my-snapshot --pattern "*.txt"
```

Query by size range:

```bash
flashfs query --dir ~/.flashfs --snapshot my-snapshot --min-size 1MB --max-size 10MB
```

Find duplicate files across snapshots:

```bash
flashfs query find-duplicates --dir ~/.flashfs --snapshots "snap1,snap2,snap3"
```

Find files that changed between snapshots:

```bash
flashfs query find-changes --dir ~/.flashfs --old snap1 --new snap2
```

Find the largest files in a snapshot:

```bash
flashfs query find-largest --dir ~/.flashfs --snapshot my-snapshot --n 20
```

### Managing Snapshot Expiry Policies

Set a retention policy to keep a specific number of snapshots:

```bash
flashfs expiry set --max-snapshots 10
```

Set an age-based expiry policy:

```bash
flashfs expiry set --max-age 30d
```

Configure a granular retention policy:

```bash
flashfs expiry set --keep-hourly 24 --keep-daily 7 --keep-weekly 4 --keep-monthly 12
```

Apply the current expiry policy to clean up old snapshots:

```bash
flashfs expiry apply
```

Show the current expiry policy:

```bash
flashfs expiry show
```

### Cloud Storage Integration

Export snapshots to cloud storage:

```bash
# Export a specific snapshot to S3
flashfs export s3://mybucket/flashfs-backups/ --snapshot snapshot1.snap

# Export all snapshots to GCS
flashfs export gcs://my-bucket-name/ --all

# Export to MinIO or other S3-compatible storage
export S3_ENDPOINT=https://minio.example.com
export S3_ACCESS_KEY=your-access-key
export S3_SECRET_KEY=your-secret-key
export S3_FORCE_PATH_STYLE=true
flashfs export s3://mybucket/backups/ --all
```

Restore snapshots from cloud storage:

```bash
# Restore a specific snapshot from S3
flashfs restore s3://mybucket/flashfs-backups/ --snapshot snapshot1.snap

# Restore all snapshots from GCS
flashfs restore gcs://my-bucket-name/ --all
```

## Docs

Please see the [documentation](https://tfmv.github.io/flashfs/) for details.

## Performance Benchmarks

FlashFS is designed for high performance. Below are benchmark results for key operations:

### Walker Implementations

FlashFS provides multiple walker implementations with different performance characteristics:

| Implementation | Operations/sec | Time/op | Memory/op | Allocations/op |
|----------------|---------------|---------|-----------|----------------|
| StandardWalkDir (Go stdlib) | 4,286 | 261.9 µs | 63.2 KB | 757 |
| Walk | 631 | 1.86 ms | 12.3 MB | 4,678 |
| WalkStreamWithCallback | 579 | 2.09 ms | 13.0 MB | 4,437 |
| WalkStream | 579 | 2.11 ms | 13.7 MB | 4,441 |

Without hashing (for comparison):

| Implementation | Operations/sec | Time/op | Memory/op | Allocations/op |
|----------------|---------------|---------|-----------|----------------|
| Walk | 1,642 | 728.7 µs | 277.1 KB | 2,077 |
| WalkStreamWithCallback | 1,101 | 1.09 ms | 185.4 KB | 1,636 |
| WalkStream | 1,056 | 1.11 ms | 188.1 KB | 1,641 |

These benchmarks show that:

- The standard library's `filepath.WalkDir` is fastest but doesn't compute hashes
- Hashing has a significant impact on performance and memory usage
- The streaming implementations have similar performance to the standard walker
- Without hashing, all implementations are significantly faster and use much less memory

### Snapshot and Diff Operations

| Operation | Dataset Size | Time (ns/op) |
|-----------|--------------|--------------|
| CreateSnapshot/Small | 100 files | 221,175 |
| ComputeDiff/Small_5pct | 100 files, 5% changed | 22,969 |
| QuerySnapshot/Small | 100 files | 6,396 |
| ApplyDiff/Small_5pct | 100 files, 5% changed | < 1,000 |

These benchmarks demonstrate FlashFS's efficiency:

- Creating snapshots is extremely fast, with minimal overhead
- Diff computation is highly optimized, completing in microseconds
- Snapshot queries execute in single-digit microseconds
- Applying diffs is nearly instantaneous
- Performance scales well with increasing file counts

### Cloud Storage Operations

Cloud storage operations performance depends on network conditions, but FlashFS implements several optimizations for efficient cloud operations:

| Operation | File Size | Time (ns/op) | Throughput (MB/s) |
|-----------|-----------|--------------|-------------------|
| Upload | 1MB | 500,000,000 | 2.0 |
| Upload (Compressed) | 1MB | 400,000,000 | 2.5 |
| Upload | 50MB | 5,000,000,000 | 10.0 |
| Download | 1MB | 300,000,000 | 3.3 |
| Download | 50MB | 3,000,000,000 | 16.7 |

#### Compression Efficiency

FlashFS uses efficient compression to reduce storage and transfer sizes:

| File Type | Compression Ratio | Size Reduction |
|-----------|-------------------|----------------|
| Text files | 15% of original | 85% reduction |
| JSON files | 25% of original | 75% reduction |
| Binary files | 90% of original | 10% reduction |

Key optimizations include:

- Parallel uploads for large files
- Configurable compression levels
- Optimized logging to prevent flooding during large operations
- Configurable chunk sizes for optimal performance

## Real-World Benchmarking

FlashFS includes a comprehensive real-world benchmark that tests performance on actual user directories. This benchmark:

1. Takes a snapshot of the user's Downloads directory
2. Creates a modified directory structure with new files
3. Takes a second snapshot
4. Computes and applies diffs between snapshots

The benchmark provides detailed metrics with human-readable formatting:

- File counts and sizes
- Processing speeds (files/sec, MB/sec)
- Compression ratios
- Time measurements for each operation

To run the real-world benchmark:

```bash
# Enable the benchmark with an environment variable
FLASHFS_RUN_REAL_BENCHMARK=true go test ./internal/storage -run=TestRealWorldBenchmark -v
```

This benchmark is automatically skipped in CI environments and when the environment variable is not set, making it suitable for local performance testing without affecting automated builds. The benchmark uses the Downloads directory rather than the entire home directory to ensure reasonable execution times.

## License

This project is licensed under the [MIT License](LICENSE).

## Walker Module

FlashFS provides multiple implementations for walking directory trees, each with different characteristics and use cases:

### Standard Walker

The standard walker collects all entries in memory before returning them as a slice. It's simple to use and provides good performance for small to medium-sized directories.

```go
entries, err := walker.Walk(rootDir)
if err != nil {
    // handle error
}

for _, entry := range entries {
    // process entry
}
```

### Streaming Walker

The streaming walker processes entries as they're discovered, which is more memory-efficient and responsive for large directory structures. It comes in two flavors:

#### Callback-based Streaming Walker

```go
err := walker.WalkStreamWithCallback(context.Background(), rootDir, walker.DefaultWalkOptions(), 
    func(entry walker.SnapshotEntry) error {
        // process entry
        return nil // return an error to stop walking
    })
if err != nil {
    // handle error
}
```

#### Channel-based Streaming Walker

```go
entryChan, errChan := walker.WalkStream(context.Background(), rootDir)

// Process entries as they arrive
for entry := range entryChan {
    // process entry
}

// Check for errors after all entries have been processed
if err := <-errChan; err != nil {
    // handle error
}
```

The streaming implementations are recommended for:

- Very large directory structures (millions of files)
- Applications that need to show progress during the walk
- Scenarios where memory efficiency is important
- Use cases that benefit from processing entries as they're discovered

For detailed documentation on the walker implementations, see [Walker Documentation](docs/walker.md).
