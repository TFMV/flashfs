package cmd

import (
	"fmt"
	"strconv"
	"time"

	"github.com/TFMV/flashfs/internal/storage"
	"github.com/spf13/cobra"
)

var expiryCmd = &cobra.Command{
	Use:   "expiry",
	Short: "Manage snapshot expiry policies",
	Long: `Manage snapshot expiry and rotation policies.
This command allows you to configure how many snapshots to keep and for how long.`,
}

var setExpiryCmd = &cobra.Command{
	Use:   "set",
	Short: "Set expiry policy",
	Long: `Set the expiry policy for snapshots.
You can configure the maximum number of snapshots to keep, the maximum age,
and how many snapshots to keep at different time intervals (hourly, daily, etc.).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flags
		baseDir, _ := cmd.Flags().GetString("dir")
		maxSnapshots, _ := cmd.Flags().GetInt("max-snapshots")
		maxAge, _ := cmd.Flags().GetString("max-age")
		keepHourly, _ := cmd.Flags().GetInt("keep-hourly")
		keepDaily, _ := cmd.Flags().GetInt("keep-daily")
		keepWeekly, _ := cmd.Flags().GetInt("keep-weekly")
		keepMonthly, _ := cmd.Flags().GetInt("keep-monthly")
		keepYearly, _ := cmd.Flags().GetInt("keep-yearly")

		// Parse max age
		var maxAgeDuration time.Duration
		if maxAge != "" {
			var err error
			maxAgeDuration, err = parseDuration(maxAge)
			if err != nil {
				return fmt.Errorf("invalid max-age value: %w", err)
			}
		}

		// Create snapshot store
		store, err := storage.NewSnapshotStore(baseDir)
		if err != nil {
			return fmt.Errorf("failed to create snapshot store: %w", err)
		}
		defer store.Close()

		// Create expiry policy
		policy := storage.ExpiryPolicy{
			MaxSnapshots: maxSnapshots,
			MaxAge:       maxAgeDuration,
			KeepHourly:   keepHourly,
			KeepDaily:    keepDaily,
			KeepWeekly:   keepWeekly,
			KeepMonthly:  keepMonthly,
			KeepYearly:   keepYearly,
		}

		// Set expiry policy
		store.SetExpiryPolicy(policy)

		// Apply expiry policy if requested
		apply, _ := cmd.Flags().GetBool("apply")
		if apply {
			deleted, err := store.ApplyExpiryPolicy()
			if err != nil {
				return fmt.Errorf("failed to apply expiry policy: %w", err)
			}
			fmt.Printf("Applied expiry policy: %d snapshots deleted\n", deleted)
		} else {
			fmt.Println("Expiry policy set successfully")
		}

		return nil
	},
}

var applyExpiryCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply expiry policy",
	Long: `Apply the current expiry policy to snapshots.
This will delete snapshots according to the configured policy.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flags
		baseDir, _ := cmd.Flags().GetString("dir")

		// Create snapshot store
		store, err := storage.NewSnapshotStore(baseDir)
		if err != nil {
			return fmt.Errorf("failed to create snapshot store: %w", err)
		}
		defer store.Close()

		// Apply expiry policy
		deleted, err := store.ApplyExpiryPolicy()
		if err != nil {
			return fmt.Errorf("failed to apply expiry policy: %w", err)
		}

		fmt.Printf("Applied expiry policy: %d snapshots deleted\n", deleted)
		return nil
	},
}

var showExpiryCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current expiry policy",
	Long:  `Show the current expiry policy configuration.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flags
		baseDir, _ := cmd.Flags().GetString("dir")

		// Create snapshot store
		store, err := storage.NewSnapshotStore(baseDir)
		if err != nil {
			return fmt.Errorf("failed to create snapshot store: %w", err)
		}
		defer store.Close()

		// Get expiry policy
		policy := store.GetExpiryPolicy()

		// Display policy
		fmt.Println("Current expiry policy:")
		fmt.Printf("  Max snapshots: %d\n", policy.MaxSnapshots)
		if policy.MaxAge > 0 {
			fmt.Printf("  Max age: %s\n", formatDuration(policy.MaxAge))
		} else {
			fmt.Println("  Max age: unlimited")
		}
		fmt.Printf("  Keep hourly: %d\n", policy.KeepHourly)
		fmt.Printf("  Keep daily: %d\n", policy.KeepDaily)
		fmt.Printf("  Keep weekly: %d\n", policy.KeepWeekly)
		fmt.Printf("  Keep monthly: %d\n", policy.KeepMonthly)
		fmt.Printf("  Keep yearly: %d\n", policy.KeepYearly)

		return nil
	},
}

// parseDuration parses a duration string with support for days, weeks, months, and years
func parseDuration(s string) (time.Duration, error) {
	// Try standard duration parsing first
	d, err := time.ParseDuration(s)
	if err == nil {
		return d, nil
	}

	// Custom parsing for days, weeks, months, years
	var value int
	var unit string

	// Extract numeric value and unit
	for i, c := range s {
		if c < '0' || c > '9' {
			value, err = strconv.Atoi(s[:i])
			if err != nil {
				return 0, fmt.Errorf("invalid duration format: %s", s)
			}
			unit = s[i:]
			break
		}
	}

	// Convert to duration
	switch unit {
	case "d", "day", "days":
		return time.Duration(value) * 24 * time.Hour, nil
	case "w", "week", "weeks":
		return time.Duration(value) * 7 * 24 * time.Hour, nil
	case "m", "month", "months":
		return time.Duration(value) * 30 * 24 * time.Hour, nil
	case "y", "year", "years":
		return time.Duration(value) * 365 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unknown duration unit: %s", unit)
	}
}

// formatDuration formats a duration in a human-readable format
func formatDuration(d time.Duration) string {
	days := int(d.Hours() / 24)
	if days >= 365 {
		years := days / 365
		return fmt.Sprintf("%d years", years)
	} else if days >= 30 {
		months := days / 30
		return fmt.Sprintf("%d months", months)
	} else if days >= 7 {
		weeks := days / 7
		return fmt.Sprintf("%d weeks", weeks)
	} else if days > 0 {
		return fmt.Sprintf("%d days", days)
	} else {
		return d.String()
	}
}

func init() {
	RootCmd.AddCommand(expiryCmd)
	expiryCmd.AddCommand(setExpiryCmd)
	expiryCmd.AddCommand(applyExpiryCmd)
	expiryCmd.AddCommand(showExpiryCmd)

	// Add flags for set command
	setExpiryCmd.Flags().Int("max-snapshots", 0, "Maximum number of snapshots to keep (0 = unlimited)")
	setExpiryCmd.Flags().String("max-age", "", "Maximum age of snapshots to keep (e.g., 30d, 2w, 6m, 1y)")
	setExpiryCmd.Flags().Int("keep-hourly", 24, "Number of hourly snapshots to keep")
	setExpiryCmd.Flags().Int("keep-daily", 7, "Number of daily snapshots to keep")
	setExpiryCmd.Flags().Int("keep-weekly", 4, "Number of weekly snapshots to keep")
	setExpiryCmd.Flags().Int("keep-monthly", 12, "Number of monthly snapshots to keep")
	setExpiryCmd.Flags().Int("keep-yearly", 5, "Number of yearly snapshots to keep")
	setExpiryCmd.Flags().Bool("apply", false, "Apply the policy immediately after setting it")
}
