# Snapshot Expiry Policy

FlashFS provides a robust snapshot lifecycle management system through configurable expiry policies. This feature allows you to automatically manage the retention and deletion of snapshots based on various criteria.

## Benefits

- **Automated Cleanup**: Automatically remove old or unnecessary snapshots
- **Storage Optimization**: Prevent storage space from being consumed by outdated snapshots
- **Flexible Retention**: Configure granular retention policies based on time periods
- **Policy Combinations**: Combine different policy types for comprehensive lifecycle management

## Expiry Policy Types

FlashFS supports several types of expiry policies that can be used individually or in combination:

### 1. Maximum Snapshots Limit

Limit the total number of snapshots to keep, removing the oldest ones when the limit is exceeded.

```bash
flashfs expiry set --max-snapshots 50
```

### 2. Maximum Age Limit

Automatically remove snapshots older than a specified duration.

```bash
flashfs expiry set --max-age 90d  # Keep snapshots for 90 days
```

Supported duration units:

- `h` or `hour(s)` - Hours
- `d` or `day(s)` - Days
- `w` or `week(s)` - Weeks
- `m` or `month(s)` - Months (30 days)
- `y` or `year(s)` - Years (365 days)

### 3. Time-Based Retention Policies

Keep a specific number of snapshots at different time intervals:

```bash
# Keep 24 hourly, 7 daily, 4 weekly, 12 monthly, and 5 yearly snapshots
flashfs expiry set --keep-hourly 24 --keep-daily 7 --keep-weekly 4 --keep-monthly 12 --keep-yearly 5
```

- **Hourly**: Keep the most recent snapshot from each hour
- **Daily**: Keep the most recent snapshot from each day
- **Weekly**: Keep the most recent snapshot from each week
- **Monthly**: Keep the most recent snapshot from each month
- **Yearly**: Keep the most recent snapshot from each year

## How It Works

When applying an expiry policy, FlashFS:

1. Sorts all snapshots by timestamp (newest first)
2. Identifies snapshots to keep based on retention policies
3. Applies maximum snapshots limit (if configured)
4. Applies maximum age limit (if configured)
5. Deletes snapshots that don't meet the retention criteria

The policy is automatically applied when creating new snapshots, or can be manually applied using the `expiry apply` command.

## Command Reference

### Setting an Expiry Policy

```bash
flashfs expiry set [options]
```

Options:

- `--max-snapshots <number>`: Maximum number of snapshots to keep (0 = unlimited)
- `--max-age <duration>`: Maximum age of snapshots to keep (e.g., 30d, 2w, 6m, 1y)
- `--keep-hourly <number>`: Number of hourly snapshots to keep
- `--keep-daily <number>`: Number of daily snapshots to keep
- `--keep-weekly <number>`: Number of weekly snapshots to keep
- `--keep-monthly <number>`: Number of monthly snapshots to keep
- `--keep-yearly <number>`: Number of yearly snapshots to keep
- `--apply`: Apply the policy immediately after setting it
- `--dir <path>`: Base directory for snapshots (defaults to current directory)

### Applying an Expiry Policy

```bash
flashfs expiry apply [options]
```

Options:

- `--dir <path>`: Base directory for snapshots (defaults to current directory)

### Showing the Current Expiry Policy

```bash
flashfs expiry show [options]
```

Options:

- `--dir <path>`: Base directory for snapshots (defaults to current directory)

## Examples

### Basic Retention Policy

Keep the 10 most recent snapshots:

```bash
flashfs expiry set --max-snapshots 10
```

### Age-Based Cleanup

Remove snapshots older than 30 days:

```bash
flashfs expiry set --max-age 30d
```

### Comprehensive Backup Strategy

Implement a comprehensive backup strategy with different retention periods:

```bash
flashfs expiry set --keep-hourly 24 --keep-daily 7 --keep-weekly 4 --keep-monthly 12 --keep-yearly 5
```

This will keep:

- 24 hourly snapshots (one per hour for the last day)
- 7 daily snapshots (one per day for the last week)
- 4 weekly snapshots (one per week for the last month)
- 12 monthly snapshots (one per month for the last year)
- 5 yearly snapshots (one per year for the last five years)

### Combined Policy

Combine different policy types for comprehensive management:

```bash
flashfs expiry set --max-snapshots 100 --max-age 365d --keep-hourly 24 --keep-daily 7
```

This will:

1. Apply the hourly and daily retention policies
2. Ensure no more than 100 snapshots are kept in total
3. Remove any snapshots older than 365 days

### Immediate Application

Set a policy and apply it immediately:

```bash
flashfs expiry set --max-age 30d --apply
```

## Implementation Details

The expiry policy is implemented in the `SnapshotStore` struct and is persisted between sessions. When a new snapshot is created, the policy is automatically applied to clean up old snapshots according to the configured rules.

The policy implementation prioritizes retention policies (hourly, daily, etc.) before applying maximum snapshots and maximum age limits. This ensures that important historical snapshots are preserved even when limits are applied.
