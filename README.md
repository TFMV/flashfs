# FlashFS: High-Performance File System Snapshot Tool

[![Build and Test](https://github.com/TFMV/flashfs/actions/workflows/build.yml/badge.svg)](https://github.com/TFMV/flashfs/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/TFMV/flashfs/branch/main/graph/badge.svg)](https://codecov.io/gh/TFMV/flashfs)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-blue.svg)](https://golang.org/doc/go1.24)
[![Go Report Card](https://goreportcard.com/badge/github.com/TFMV/flashfs)](https://goreportcard.com/report/github.com/TFMV/flashfs)
[![Go Reference](https://pkg.go.dev/badge/github.com/TFMV/flashfs.svg)](https://pkg.go.dev/github.com/TFMV/flashfs)
[![Documentation](https://img.shields.io/badge/docs-website-blue)](https://tfmv.github.io/flashfs/)

FlashFS is a high-performance file system snapshot and comparison tool designed for large-scale environments. It efficiently captures file system metadata, stores it in a compact format, and provides fast comparison capabilities.

## Features

- **Blazing Fast Snapshots**: Efficiently captures file system metadata with minimal overhead
- **Incremental Snapshots**: Stores only changes between snapshots to minimize storage requirements
- **Efficient Storage**: Uses FlatBuffers for compact binary representation and Zstd compression
- **In-Memory Caching**: Implements LRU caching for frequently accessed snapshots
- **Bloom Filters**: Quickly identifies modified files without full snapshot comparison
- **Enhanced Diff Generation**: Creates, stores, and applies optimized diffs between snapshots with parallel processing
- **Snapshot Expiry Policies**: Automatically manages snapshot lifecycle with configurable retention policies
- **Cloud Storage Integration**: Export and restore snapshots to/from S3, GCS, or compatible object storage

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

```bash
flashfs snapshot --path /path/to/directory --output snapshot.snap
```

### Comparing Snapshots

```bash
flashfs diff --base snapshot1.snap --target snapshot2.snap --output diff.diff
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

```bash
flashfs query --snapshot snapshot.snap --path "/some/path/*" --modified-after "2023-01-01"
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

Please see the [documentation](TFMV.github.io/flashfs/) for details.

## Architecture

```mermaid
graph TD
    subgraph CLI
        A[Root Command] --> B[Snapshot Command]
        A --> C[Diff Command]
        A --> D[Query Command]
        A --> E1[Expiry Command]
        A --> CS1[Export Command]
        A --> CS2[Restore Command]
    end

    subgraph Core Components
        E[Walker] --> |File Metadata| F[Serializer]
        F --> |FlatBuffers| G[Storage]
        G --> |Compressed Data| H[(Snapshot Files)]
    end

    subgraph Storage Module
        G --> G1[SnapshotStore]
        G1 --> G2[Write Snapshot]
        G1 --> G3[Read Snapshot]
        G1 --> G4[Compute Diff]
        G1 --> G5[Apply Diff]
        G1 --> G6[Query Snapshot]
        G1 --> G7[Bloom Filter]
        G1 --> G9[Expiry Policy]
        G2 --> |Zstd Compression| H
        G3 --> |Cache| G8[Memory Cache]
        G3 --> |Decompress| H
        G9 --> |Manage Lifecycle| H
    end

    subgraph Cloud Storage Module
        CS3[Cloud Storage] --> CS4[S3 Provider]
        CS3 --> CS5[GCS Provider]
        CS3 --> CS6[Upload/Download]
        CS3 --> CS7[Compression]
        CS6 --> |Export| H
        H --> |Restore| CS6
    end

    subgraph Walker Module
        E --> E2[File System Traversal]
        E2 --> E3[Metadata Collection]
        E3 --> E4[Hash Calculation]
    end

    subgraph Serializer Module
        F --> F1[FlatBuffers Schema]
        F1 --> F2[Binary Serialization]
    end

    subgraph Expiry Module
        G9 --> EP1[Retention Policies]
        EP1 --> EP2[Hourly/Daily/Weekly]
        EP1 --> EP3[Monthly/Yearly]
        G9 --> EP4[Max Age Limit]
        G9 --> EP5[Max Snapshots Limit]
    end

    subgraph Diff Module
        G4 --> D1[Bloom Filter Check]
        G4 --> D2[Parallel Processing]
        G4 --> D3[Optimized Comparison]
        G5 --> D4[Incremental Updates]
    end

    B --> E
    C --> G4
    D --> G6
    E1 --> G9
    CS1 --> CS3
    CS2 --> CS3
```

## License

This project is licensed under the [MIT License](LICENSE).
