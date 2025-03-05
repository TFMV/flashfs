package cmd

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/TFMV/flashfs/internal/diff"
	"github.com/spf13/cobra"
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Compare two snapshots",
	Long: `Compare two snapshots and show the differences between them.
The command identifies new, modified, and deleted files between snapshots.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get context from command
		ctx := cmd.Context()

		// Get flags
		oldFile, err := cmd.Flags().GetString("old")
		if err != nil {
			return fmt.Errorf("failed to get old file: %w", err)
		}

		newFile, err := cmd.Flags().GetString("new")
		if err != nil {
			return fmt.Errorf("failed to get new file: %w", err)
		}

		format, err := cmd.Flags().GetString("format")
		if err != nil {
			return fmt.Errorf("failed to get format: %w", err)
		}

		pathPrefix, err := cmd.Flags().GetString("path")
		if err != nil {
			return fmt.Errorf("failed to get path prefix: %w", err)
		}

		useHashDiff, err := cmd.Flags().GetBool("hash")
		if err != nil {
			return fmt.Errorf("failed to get hash flag: %w", err)
		}

		workers, err := cmd.Flags().GetInt("workers")
		if err != nil {
			return fmt.Errorf("failed to get workers: %w", err)
		}

		useBloomFilter, err := cmd.Flags().GetBool("bloom")
		if err != nil {
			return fmt.Errorf("failed to get bloom filter flag: %w", err)
		}

		// Validate files exist
		if _, err := os.Stat(oldFile); os.IsNotExist(err) {
			return fmt.Errorf("old snapshot file not found: %s", oldFile)
		}

		if _, err := os.Stat(newFile); os.IsNotExist(err) {
			return fmt.Errorf("new snapshot file not found: %s", newFile)
		}

		// Create diff options
		options := diff.DiffOptions{
			PathPrefix:     pathPrefix,
			UseHashDiff:    useHashDiff,
			Workers:        workers,
			UseBloomFilter: useBloomFilter,
		}

		// Start timing
		startTime := time.Now()
		log.Printf("Comparing snapshots: %s and %s", oldFile, newFile)
		if pathPrefix != "" {
			log.Printf("Filtering by path prefix: %s", pathPrefix)
		}

		// Get detailed diffs
		diffs, err := diff.CompareSnapshotsDetailedWithOptions(ctx, oldFile, newFile, options)
		if err != nil {
			return fmt.Errorf("failed to compare snapshots: %w", err)
		}

		// Print results based on format
		switch strings.ToLower(format) {
		case "table":
			printDiffTable(diffs)
		case "json":
			printDiffJSON(diffs)
		default:
			printDiffSimple(diffs)
		}

		// Log summary
		duration := time.Since(startTime)
		log.Printf("Found %d differences in %v", len(diffs), duration)

		return nil
	},
}

func printDiffTable(entries []diff.DiffEntry) {
	// Create a new tabwriter
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	// Print header
	fmt.Fprintln(w, "TYPE\tPATH\tDETAILS")
	fmt.Fprintln(w, "----\t----\t-------")

	// Print each entry
	for _, entry := range entries {
		var details string
		switch entry.Type {
		case "Modified":
			details = fmt.Sprintf("size: %d → %d, mtime: %d → %d",
				entry.OldSize, entry.NewSize,
				entry.OldModTime, entry.NewModTime)
			if entry.HashDiff {
				details += ", hash changed"
			}
		case "New":
			details = fmt.Sprintf("size: %d, mtime: %d", entry.NewSize, entry.NewModTime)
		case "Deleted":
			details = fmt.Sprintf("size: %d, mtime: %d", entry.OldSize, entry.OldModTime)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", entry.Type, entry.Path, details)
	}
}

func printDiffJSON(entries []diff.DiffEntry) {
	// Simple JSON output (could use encoding/json for more complex output)
	fmt.Println("[")
	for i, entry := range entries {
		comma := ","
		if i == len(entries)-1 {
			comma = ""
		}

		var details string
		switch entry.Type {
		case "Modified":
			details = fmt.Sprintf(`"old_size": %d, "new_size": %d, "old_mtime": %d, "new_mtime": %d, "hash_diff": %t`,
				entry.OldSize, entry.NewSize, entry.OldModTime, entry.NewModTime, entry.HashDiff)
		case "New":
			details = fmt.Sprintf(`"size": %d, "mtime": %d`, entry.NewSize, entry.NewModTime)
		case "Deleted":
			details = fmt.Sprintf(`"size": %d, "mtime": %d`, entry.OldSize, entry.OldModTime)
		}

		fmt.Printf(`  {"type": "%s", "path": "%s", %s}%s`, entry.Type, entry.Path, details, comma)
		fmt.Println()
	}
	fmt.Println("]")
}

func printDiffSimple(entries []diff.DiffEntry) {
	for _, entry := range entries {
		fmt.Println(entry.DetailedString())
	}
}

func init() {
	RootCmd.AddCommand(diffCmd)

	// Required flags
	diffCmd.Flags().String("old", "", "Path to the old snapshot file")
	diffCmd.Flags().String("new", "", "Path to the new snapshot file")
	diffCmd.MarkFlagRequired("old")
	diffCmd.MarkFlagRequired("new")

	// Optional flags
	diffCmd.Flags().String("format", "simple", "Output format (simple, table, json)")
	diffCmd.Flags().String("path", "", "Filter by path prefix (for partial diffs)")
	diffCmd.Flags().Bool("hash", true, "Use hash-based diffing for unchanged size/mtime")
	diffCmd.Flags().Int("workers", runtime.NumCPU(), "Number of worker goroutines (0 = auto)")
	diffCmd.Flags().Bool("bloom", true, "Use bloom filters for quick detection of changes")
}
