package cmd

import (
	"fmt"
	"log"
	"os"
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
			return fmt.Errorf("error getting old file flag: %w", err)
		}

		newFile, err := cmd.Flags().GetString("new")
		if err != nil {
			return fmt.Errorf("error getting new file flag: %w", err)
		}

		detailed, err := cmd.Flags().GetBool("detailed")
		if err != nil {
			return fmt.Errorf("error getting detailed flag: %w", err)
		}

		format, err := cmd.Flags().GetString("format")
		if err != nil {
			return fmt.Errorf("error getting format flag: %w", err)
		}

		// Validate flags
		if oldFile == "" || newFile == "" {
			return fmt.Errorf("both --old and --new snapshot files must be specified")
		}

		// Validate that files exist
		if _, err := os.Stat(oldFile); os.IsNotExist(err) {
			return fmt.Errorf("old snapshot file not found: %s", oldFile)
		}
		if _, err := os.Stat(newFile); os.IsNotExist(err) {
			return fmt.Errorf("new snapshot file not found: %s", newFile)
		}

		// Start timing
		startTime := time.Now()

		// Log start
		log.Printf("Starting diff between %s and %s", oldFile, newFile)

		// Use detailed comparison if requested
		if detailed {
			entries, err := diff.CompareSnapshotsDetailed(ctx, oldFile, newFile)
			if err != nil {
				return fmt.Errorf("error comparing snapshots: %w", err)
			}

			// Print results based on format
			switch strings.ToLower(format) {
			case "table":
				printDiffTable(entries)
			case "json":
				printDiffJSON(entries)
			default:
				printDiffSimple(entries)
			}

			// Log completion
			log.Printf("Diff completed in %v, found %d differences", time.Since(startTime), len(entries))

			return nil
		}

		// Use simple comparison
		diffs, err := diff.CompareSnapshotsWithContext(ctx, oldFile, newFile)
		if err != nil {
			return fmt.Errorf("error comparing snapshots: %w", err)
		}

		// Print results
		for _, d := range diffs {
			fmt.Println(d)
		}

		// Log completion
		log.Printf("Diff completed in %v, found %d differences", time.Since(startTime), len(diffs))

		return nil
	},
}

// printDiffTable prints diff entries in a tabular format
func printDiffTable(entries []diff.DiffEntry) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TYPE\tPATH")
	fmt.Fprintln(w, "----\t----")

	for _, entry := range entries {
		fmt.Fprintf(w, "%s\t%s\n", entry.Type, entry.Path)
	}

	w.Flush()
}

// printDiffJSON prints diff entries in JSON format
func printDiffJSON(entries []diff.DiffEntry) {
	fmt.Println("[")
	for i, entry := range entries {
		comma := ","
		if i == len(entries)-1 {
			comma = ""
		}
		fmt.Printf("  {\"type\": \"%s\", \"path\": \"%s\"}%s\n", entry.Type, entry.Path, comma)
	}
	fmt.Println("]")
}

// printDiffSimple prints diff entries in a simple format
func printDiffSimple(entries []diff.DiffEntry) {
	for _, entry := range entries {
		fmt.Printf("%s: %s\n", entry.Type, entry.Path)
	}
}

func init() {
	diffCmd.Flags().String("old", "", "Old snapshot file")
	diffCmd.Flags().String("new", "", "New snapshot file")
	diffCmd.Flags().Bool("detailed", false, "Show detailed diff information")
	diffCmd.Flags().String("format", "simple", "Output format (simple, table, json)")
	RootCmd.AddCommand(diffCmd)
}
