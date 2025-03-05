# Enhanced Diff Computation

FlashFS provides powerful diff computation capabilities that enable efficient comparison, storage, and application of changes between snapshots. This feature is essential for incremental backups, file synchronization, and monitoring changes over time.

## Benefits

- **Storage Efficiency**: Store only the changes between snapshots instead of full copies
- **Transfer Optimization**: Transmit only the differences when synchronizing snapshots
- **Change Tracking**: Easily identify what files have been added, modified, or deleted
- **Performance**: Utilize parallel processing and pre-filtering for fast comparisons
- **Flexibility**: Configure comparison options based on specific needs

## How Diff Computation Works

FlashFS implements a multi-stage approach to efficiently compute differences between snapshots:

### 1. Bloom Filter Pre-check

Before performing detailed comparisons, FlashFS uses Bloom filters to quickly identify files that may have changed:

1. Creates a Bloom filter from the base snapshot
2. Tests each file in the target snapshot against the filter
3. Files that fail the Bloom filter test are candidates for detailed comparison

This pre-filtering step significantly reduces the number of files that need detailed comparison, especially in large snapshots where most files remain unchanged.

### 2. Detailed Comparison

For files identified by the Bloom filter, FlashFS performs a detailed comparison:

1. Compares file metadata (path, size, modification time, permissions)
2. Optionally compares file content hashes for detecting changes even when metadata is unchanged
3. Identifies files that have been added, modified, or deleted

### 3. Parallel Processing

To accelerate diff computation for large snapshots, FlashFS can distribute the comparison work across multiple CPU cores:

1. Divides the file list into chunks
2. Processes each chunk in parallel
3. Combines the results into a unified diff

### 4. Diff Storage

The computed diff is stored in a compact format:

1. Records only the changes (added, modified, deleted files)
2. Uses the same efficient FlatBuffers serialization as snapshots
3. Applies Zstd compression to minimize storage requirements

## Command Reference

### Computing a Diff

```bash
flashfs diff [options]
```

Options:

- `--base <file>`: Base snapshot file (required)
- `--target <file>`: Target snapshot file (required)
- `--output <file>`: Output diff file (required)
- `--detailed`: Perform detailed comparison including file content hashes
- `--parallel <n>`: Number of parallel workers for comparison (default: number of CPU cores)
- `--no-hash`: Skip hash comparison (faster but less accurate)
- `--path-filter <pattern>`: Only compare files matching the specified path pattern

### Applying a Diff

```bash
flashfs apply [options]
```

Options:

- `--base <file>`: Base snapshot file (required)
- `--diff <file>`: Diff file to apply (required)
- `--output <file>`: Output snapshot file (required)

### Viewing Diff Information

```bash
flashfs diff-info [options]
```

Options:

- `--diff <file>`: Diff file to analyze (required)
- `--verbose`: Show detailed information about each changed file

## Examples

### Basic Diff Computation

Compute the differences between two snapshots:

```bash
flashfs diff --base snapshot1.snap --target snapshot2.snap --output changes.diff
```

### Detailed Comparison with Parallel Processing

Perform a detailed comparison using 8 parallel workers:

```bash
flashfs diff --base snapshot1.snap --target snapshot2.snap --output changes.diff --detailed --parallel 8
```

### Path-Filtered Comparison

Compare only files in a specific directory:

```bash
flashfs diff --base snapshot1.snap --target snapshot2.snap --output changes.diff --path-filter "/home/user/documents/*"
```

### Applying a Diff to Generate a New Snapshot

Apply a diff to a base snapshot to generate a new snapshot:

```bash
flashfs apply --base snapshot1.snap --diff changes.diff --output snapshot2.snap
```

### Viewing Diff Information

Analyze the contents of a diff file:

```bash
flashfs diff-info --diff changes.diff --verbose
```

## Implementation Details

The diff computation is implemented in the `SnapshotStore` struct with the following key methods:

- `ComputeDiff`: Computes the differences between two snapshots
- `StoreDiff`: Stores the computed diff to a file
- `ApplyDiff`: Applies a diff to a base snapshot to generate a new snapshot

The diff system uses the same efficient serialization and compression techniques as the snapshot system, ensuring consistent performance and storage efficiency.

## Performance Considerations

- **Bloom Filters**: The Bloom filter pre-check significantly reduces comparison time for large snapshots
- **Parallel Processing**: Utilizing multiple CPU cores can dramatically speed up diff computation
- **Hash Comparison**: Enabling hash comparison provides more accurate results but increases computation time
- **Path Filtering**: Using path filters can focus the comparison on relevant files, reducing processing time

For optimal performance, adjust the parallelism level based on your system's capabilities and use path filters when only specific directories are of interest.
