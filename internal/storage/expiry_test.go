package storage

import (
	"testing"
	"time"
)

func TestApplyExpiryPolicy(t *testing.T) {
	// Create test snapshots
	now := time.Now()
	snapshots := []SnapshotInfo{
		// Recent snapshots (last 24 hours)
		{Name: "backup-20230101-120000.snap", Timestamp: now.Add(-1 * time.Hour), Size: 1000},
		{Name: "backup-20230101-110000.snap", Timestamp: now.Add(-2 * time.Hour), Size: 1000},
		{Name: "backup-20230101-100000.snap", Timestamp: now.Add(-3 * time.Hour), Size: 1000},
		{Name: "backup-20230101-090000.snap", Timestamp: now.Add(-4 * time.Hour), Size: 1000},
		{Name: "backup-20230101-080000.snap", Timestamp: now.Add(-5 * time.Hour), Size: 1000},

		// Older snapshots (different days)
		{Name: "backup-20230101-070000.snap", Timestamp: now.Add(-24 * time.Hour), Size: 1000},
		{Name: "backup-20230101-060000.snap", Timestamp: now.Add(-48 * time.Hour), Size: 1000},
		{Name: "backup-20230101-050000.snap", Timestamp: now.Add(-72 * time.Hour), Size: 1000},
		{Name: "backup-20230101-040000.snap", Timestamp: now.Add(-96 * time.Hour), Size: 1000},

		// Even older snapshots (different weeks)
		{Name: "backup-20230101-030000.snap", Timestamp: now.Add(-7 * 24 * time.Hour), Size: 1000},
		{Name: "backup-20230101-020000.snap", Timestamp: now.Add(-14 * 24 * time.Hour), Size: 1000},

		// Very old snapshots (different months)
		{Name: "backup-20230101-010000.snap", Timestamp: now.Add(-30 * 24 * time.Hour), Size: 1000},
		{Name: "backup-20230101-000000.snap", Timestamp: now.Add(-60 * 24 * time.Hour), Size: 1000},
	}

	tests := []struct {
		name            string
		policy          ExpiryPolicy
		wantDeleteCount int
	}{
		{
			name: "Keep all snapshots",
			policy: ExpiryPolicy{
				MaxSnapshots: 0,
				MaxAge:       0,
				KeepHourly:   0,
				KeepDaily:    0,
				KeepWeekly:   0,
				KeepMonthly:  0,
				KeepYearly:   0,
			},
			wantDeleteCount: 0,
		},
		{
			name: "Max snapshots limit",
			policy: ExpiryPolicy{
				MaxSnapshots: 5,
				MaxAge:       0,
				KeepHourly:   0,
				KeepDaily:    0,
				KeepWeekly:   0,
				KeepMonthly:  0,
				KeepYearly:   0,
			},
			wantDeleteCount: len(snapshots) - 5,
		},
		{
			name: "Max age limit (3 days)",
			policy: ExpiryPolicy{
				MaxSnapshots: 0,
				MaxAge:       3 * 24 * time.Hour,
				KeepHourly:   0,
				KeepDaily:    0,
				KeepWeekly:   0,
				KeepMonthly:  0,
				KeepYearly:   0,
			},
			wantDeleteCount: 5, // The 5 oldest snapshots are more than 3 days old
		},
		{
			name: "Retention policy - hourly only",
			policy: ExpiryPolicy{
				MaxSnapshots: 0,
				MaxAge:       0,
				KeepHourly:   3,
				KeepDaily:    0,
				KeepWeekly:   0,
				KeepMonthly:  0,
				KeepYearly:   0,
			},
			wantDeleteCount: len(snapshots) - 3, // Keep 3 hourly snapshots
		},
		{
			name: "Retention policy - daily only",
			policy: ExpiryPolicy{
				MaxSnapshots: 0,
				MaxAge:       0,
				KeepHourly:   0,
				KeepDaily:    2,
				KeepWeekly:   0,
				KeepMonthly:  0,
				KeepYearly:   0,
			},
			wantDeleteCount: len(snapshots) - 2, // Keep 2 daily snapshots
		},
		{
			name: "Retention policy - combined",
			policy: ExpiryPolicy{
				MaxSnapshots: 0,
				MaxAge:       0,
				KeepHourly:   2,
				KeepDaily:    2,
				KeepWeekly:   1,
				KeepMonthly:  1,
				KeepYearly:   0,
			},
			wantDeleteCount: len(snapshots) - 6, // Keep 2 hourly + 2 daily + 1 weekly + 1 monthly
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Print snapshot information for debugging
			t.Logf("Test case: %s", tt.name)
			t.Logf("Policy: MaxSnapshots=%d, MaxAge=%v, KeepHourly=%d, KeepDaily=%d, KeepWeekly=%d, KeepMonthly=%d, KeepYearly=%d",
				tt.policy.MaxSnapshots, tt.policy.MaxAge, tt.policy.KeepHourly, tt.policy.KeepDaily,
				tt.policy.KeepWeekly, tt.policy.KeepMonthly, tt.policy.KeepYearly)

			t.Logf("Snapshots (%d total):", len(snapshots))
			for i, s := range snapshots {
				t.Logf("  %d: %s (age: %v)", i, s.Name, now.Sub(s.Timestamp))
			}

			toDelete := ApplyExpiryPolicy(snapshots, tt.policy)

			t.Logf("Snapshots to delete (%d):", len(toDelete))
			for i, name := range toDelete {
				t.Logf("  %d: %s", i, name)
			}

			if len(toDelete) != tt.wantDeleteCount {
				t.Errorf("ApplyExpiryPolicy() returned %d snapshots to delete, want %d",
					len(toDelete), tt.wantDeleteCount)
			}
		})
	}
}
