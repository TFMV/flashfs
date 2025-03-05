# Command Line Interface

FlashFS provides a comprehensive command-line interface (CLI) for interacting with snapshots, diffs, and expiry policies. The CLI is built using [Cobra](https://github.com/spf13/cobra), a powerful library for creating modern CLI applications.

## Overview

The FlashFS CLI includes commands for:

- Creating and managing snapshots
- Computing and applying diffs between snapshots
- Querying snapshot contents
- Managing snapshot expiry policies

## Command Structure

```bash
flashfs
├── snapshot - Create a snapshot of a directory
├── diff - Compute differences between snapshots
├── apply - Apply a diff to a snapshot
├── query - Query snapshot contents
└── expiry - Manage snapshot expiry policies
    ├── set - Set expiry policy
    ├── apply - Apply expiry policy
    └── show - Show current expiry policy
```

## Global Flags

These flags apply to all commands:

```
--help, -h     Show help for a command
--verbose, -v  Enable verbose output
```

## Snapshot Command

Create a snapshot of a directory.

```bash
flashfs snapshot [flags]
```

### Flags

```bash
--path, -p string     Directory to snapshot (required)
--output, -o string   Output snapshot file (required)
--exclude string      Exclude pattern (e.g., "*.tmp,*.log")
--no-hash             Skip file content hashing
```

### Examples

```bash
# Create a snapshot of the current directory
flashfs snapshot --path . --output backup.snap

# Create a snapshot excluding temporary files
flashfs snapshot --path /home/user/documents --output docs.snap --exclude "*.tmp,*.bak"

# Create a snapshot without computing file hashes (faster but less accurate)
flashfs snapshot --path /var/www --output web.snap --no-hash
```

## Diff Command

Compute differences between snapshots and store them in a structured format.

```bash
flashfs diff [flags]
```

### Flags

```
--base, -b string     Base snapshot file (required)
--target, -t string   Target snapshot file (required)
--output, -o string   Output diff file (required)
--detailed            Perform detailed comparison including file content hashes
--parallel int        Number of parallel workers for comparison (default: number of CPU cores)
--no-hash             Skip hash comparison (faster but less accurate)
--path-filter string  Only compare files matching the specified path pattern
```

### Examples

```bash
# Compute differences between two snapshots
flashfs diff --base snapshot1.snap --target snapshot2.snap --output changes.diff

# Detailed comparison with 8 parallel workers
flashfs diff --base snapshot1.snap --target snapshot2.snap --output changes.diff --detailed --parallel 8

# Compare only files in a specific directory
flashfs diff --base snapshot1.snap --target snapshot2.snap --output changes.diff --path-filter "/home/user/documents/*"
```

### Diff Format

The generated diff file contains a structured representation of changes:

- **Added files**: Files that exist in the target snapshot but not in the base snapshot
- **Modified files**: Files that exist in both snapshots but have different metadata or content
- **Deleted files**: Files that exist in the base snapshot but not in the target snapshot

Each change is stored as a `DiffEntry` with:

- Path of the changed file
- Type of change (added, modified, deleted)
- Before and after values for size, modification time, permissions, and content hash (as applicable)

This structured format enables efficient application of changes and provides detailed information about what has changed between snapshots.

## Apply Command

Apply a diff to a snapshot to generate a new snapshot.

```bash
flashfs apply [flags]
```

### Flags

```
--base, -b string    Base snapshot file (required)
--diff, -d string    Diff file to apply (required)
--output, -o string  Output snapshot file (required)
```

### Examples

```bash
# Apply a diff to generate a new snapshot
flashfs apply --base snapshot1.snap --diff changes.diff --output snapshot2.snap
```

### How Apply Works

The apply command:

1. Loads the base snapshot into memory
2. Deserializes the diff file into a structured Diff object
3. Processes each DiffEntry based on its type:
   - For added files (type 0): Creates a new entry in the snapshot
   - For modified files (type 1): Updates the existing entry with new metadata
   - For deleted files (type 2): Removes the entry from the snapshot
4. Serializes the modified snapshot and writes it to the output file

This structured approach ensures that changes are applied correctly and efficiently, maintaining the integrity of your file system representation.

## Query Command

Query snapshot contents.

```bash
flashfs query [flags]
```

### Flags

```
--snapshot, -s string       Snapshot file to query (required)
--path string               Path pattern to filter by (e.g., "/home/user/*")
--modified-after string     Show files modified after this date (format: YYYY-MM-DD)
--modified-before string    Show files modified before this date (format: YYYY-MM-DD)
--size-gt int               Show files larger than this size (in bytes)
--size-lt int               Show files smaller than this size (in bytes)
--is-dir                    Show only directories
--is-file                   Show only files
--format string             Output format: text, json, csv (default "text")
```

### Examples

```bash
# List all files in a snapshot
flashfs query --snapshot backup.snap

# Find large files modified recently
flashfs query --snapshot backup.snap --size-gt 10485760 --modified-after "2023-01-01"

# Find all .log files in a specific directory
flashfs query --snapshot backup.snap --path "/var/log/*.log"

# Export results as JSON
flashfs query --snapshot backup.snap --path "*.mp4" --format json > videos.json
```

## Expiry Command

Manage snapshot expiry policies.

```bash
flashfs expiry [command]
```

### Subcommands

#### Set

Set the expiry policy for snapshots.

```bash
flashfs expiry set [flags]
```

##### Flags

```
--max-snapshots int    Maximum number of snapshots to keep (0 = unlimited)
--max-age string       Maximum age of snapshots to keep (e.g., 30d, 2w, 6m, 1y)
--keep-hourly int      Number of hourly snapshots to keep
--keep-daily int       Number of daily snapshots to keep
--keep-weekly int      Number of weekly snapshots to keep
--keep-monthly int     Number of monthly snapshots to keep
--keep-yearly int      Number of yearly snapshots to keep
--apply                Apply the policy immediately after setting it
--dir string           Base directory for snapshots (default: current directory)
```

##### Examples

```bash
# Keep only the 10 most recent snapshots
flashfs expiry set --max-snapshots 10

# Remove snapshots older than 30 days
flashfs expiry set --max-age 30d

# Set a comprehensive retention policy
flashfs expiry set --keep-hourly 24 --keep-daily 7 --keep-weekly 4 --keep-monthly 12 --keep-yearly 5

# Set a policy and apply it immediately
flashfs expiry set --max-age 30d --apply
```

#### Apply

Apply the current expiry policy to snapshots.

```bash
flashfs expiry apply [flags]
```

##### Flags

```
--dir string  Base directory for snapshots (default: current directory)
```

##### Examples

```bash
# Apply the current expiry policy
flashfs expiry apply

# Apply the policy to snapshots in a specific directory
flashfs expiry apply --dir /path/to/snapshots
```

#### Show

Show the current expiry policy.

```bash
flashfs expiry show [flags]
```

##### Flags

```
--dir string  Base directory for snapshots (default: current directory)
```

##### Examples

```bash
# Show the current expiry policy
flashfs expiry show

# Show the policy for snapshots in a specific directory
flashfs expiry show --dir /path/to/snapshots
```

## Implementation Details

### Command Registration

Commands are registered in the `cmd` package:

```go
func init() {
    RootCmd.AddCommand(snapshotCmd)
    RootCmd.AddCommand(diffCmd)
    RootCmd.AddCommand(applyCmd)
    RootCmd.AddCommand(queryCmd)
    RootCmd.AddCommand(expiryCmd)
    
    // Register expiry subcommands
    expiryCmd.AddCommand(setExpiryCmd)
    expiryCmd.AddCommand(applyExpiryCmd)
    expiryCmd.AddCommand(showExpiryCmd)
}
```

### Command Execution

Each command is implemented as a Cobra command with a `RunE` function:

```go
var snapshotCmd = &cobra.Command{
    Use:   "snapshot",
    Short: "Create a snapshot of a directory",
    Long:  `Create a snapshot of a directory, capturing file metadata and optionally content hashes.`,
    RunE: func(cmd *cobra.Command, args []string) error {
        // Command implementation
        // ...
        return nil
    },
}
```

### Error Handling

The CLI provides detailed error messages and appropriate exit codes:

```go
if err != nil {
    fmt.Fprintf(os.Stderr, "Error: %v\n", err)
    os.Exit(1)
}
```

## Advanced Usage

### Scripting

FlashFS commands can be used in scripts for automation:

```bash
#!/bin/bash
# Create daily snapshots and apply expiry policy

# Create a snapshot with the current date
DATE=$(date +%Y%m%d)
flashfs snapshot --path /home/user/documents --output backup-$DATE.snap

# Apply expiry policy to clean up old snapshots
flashfs expiry apply
```

### Piping Output

Query results can be piped to other commands:

```bash
# Find large files and sort by size
flashfs query --snapshot backup.snap --size-gt 10485760 --format csv | sort -t, -k2 -n

# Find recent changes and send a report by email
flashfs query --snapshot backup.snap --modified-after "2023-01-01" | mail -s "Recent Changes" user@example.com
```

### Integration with Other Tools

FlashFS can be integrated with other tools:

```bash
# Use with find to process multiple directories
find /home -type d -name "projects" | xargs -I{} flashfs snapshot --path {} --output {}.snap

# Use with cron for scheduled snapshots
# Add to crontab: 0 0 * * * /path/to/snapshot_script.sh
```
