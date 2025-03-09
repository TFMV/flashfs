package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/TFMV/flashfs/internal/query"
	"github.com/TFMV/flashfs/internal/storage"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query snapshots for files matching criteria",
	Long: `Query snapshots for files matching various criteria.

This command allows you to search for files across snapshots using different
criteria such as path patterns, size ranges, modification times, and content hashes.
It can search in both the hot layer (BuntDB) and the cold layer (FlatBuffers snapshots).

Examples:
  flashfs query --snapshot my-snapshot --pattern "*.txt"
  flashfs query --snapshot my-snapshot --min-size 1MB --max-size 10MB
  flashfs query --snapshot my-snapshot --modified-after "2023-01-01" --modified-before "2023-02-01"
  flashfs query --snapshot my-snapshot --hash "abcdef1234567890"
  flashfs query --snapshot my-snapshot --is-dir
  flashfs query --all-snapshots --pattern "*.log" --hot-layer-only`,
	RunE: func(cmd *cobra.Command, args []string) error {
		baseDir, _ := cmd.Flags().GetString("dir")
		if baseDir == "" {
			return fmt.Errorf("--dir must be specified")
		}

		// Create a store
		store, err := storage.NewStore(baseDir, storage.DefaultStoreOptions())
		if err != nil {
			return err
		}
		defer store.Close()

		// Create a query engine
		queryEngine := query.NewQueryEngine(store)

		// Parse options
		pattern, _ := cmd.Flags().GetString("pattern")
		minSize, _ := cmd.Flags().GetString("min-size")
		maxSize, _ := cmd.Flags().GetString("max-size")
		startTime, _ := cmd.Flags().GetString("start-time")
		endTime, _ := cmd.Flags().GetString("end-time")
		hash, _ := cmd.Flags().GetString("hash")
		isDir, _ := cmd.Flags().GetBool("is-dir")
		allSnapshots, _ := cmd.Flags().GetBool("all-snapshots")
		snapshotID, _ := cmd.Flags().GetString("snapshot")

		// Build query options
		options := query.DefaultQueryOptions()
		if pattern != "" {
			options.Pattern = pattern
		}
		if minSize != "" {
			size, err := parseSize(minSize)
			if err != nil {
				return fmt.Errorf("invalid min-size: %w", err)
			}
			options.MinSize = size
		}
		if maxSize != "" {
			size, err := parseSize(maxSize)
			if err != nil {
				return fmt.Errorf("invalid max-size: %w", err)
			}
			options.MaxSize = size
		}
		if startTime != "" {
			t, err := time.Parse(time.RFC3339, startTime)
			if err != nil {
				return fmt.Errorf("invalid start-time: %w", err)
			}
			options.StartTime = t
		}
		if endTime != "" {
			t, err := time.Parse(time.RFC3339, endTime)
			if err != nil {
				return fmt.Errorf("invalid end-time: %w", err)
			}
			options.EndTime = t
		}
		if hash != "" {
			options.Hash = hash
		}
		if cmd.Flags().Changed("is-dir") {
			options.IsDir = &isDir
		}

		// Execute the query
		var results []query.FileResult
		if allSnapshots {
			results, err = queryEngine.QueryAllSnapshots(options)
		} else {
			results, err = queryEngine.QuerySnapshot(snapshotID, options)
		}

		if err != nil {
			return err
		}

		// Print results
		for _, result := range results {
			fmt.Printf("%s\t%d\t%s\t%v\t%s\n",
				result.Path,
				result.Size,
				result.ModTime.Format(time.RFC3339),
				result.IsDir,
				result.Hash)
		}

		return nil
	},
}

var findDuplicatesCmd = &cobra.Command{
	Use:   "find-duplicates",
	Short: "Find duplicate files across snapshots",
	Long: `Find duplicate files across snapshots based on content hash.

This command identifies files with the same content hash across multiple snapshots,
which indicates that they contain the same data.

Examples:
  flashfs query find-duplicates --snapshots "snap1,snap2,snap3"
  flashfs query find-duplicates --all-snapshots --min-size 1MB`,
	RunE: func(cmd *cobra.Command, args []string) error {
		baseDir, _ := cmd.Flags().GetString("dir")
		if baseDir == "" {
			return fmt.Errorf("--dir must be specified")
		}

		// Create a store
		store, err := storage.NewStore(baseDir, storage.DefaultStoreOptions())
		if err != nil {
			return err
		}
		defer store.Close()

		// Create a query engine
		queryEngine := query.NewQueryEngine(store)

		// Get snapshot IDs
		snapshotIDs, _ := cmd.Flags().GetStringSlice("snapshots")
		if len(snapshotIDs) == 0 {
			return fmt.Errorf("at least one snapshot must be specified")
		}

		// Build query options
		options := query.DefaultQueryOptions()
		pattern, _ := cmd.Flags().GetString("pattern")
		if pattern != "" {
			options.Pattern = pattern
		}

		// Find duplicates
		duplicates, err := queryEngine.FindDuplicateFiles(snapshotIDs, options)
		if err != nil {
			return err
		}

		// Print results
		for hash, files := range duplicates {
			fmt.Printf("Hash: %s\n", hash)
			for _, file := range files {
				fmt.Printf("  %s (snapshot: %s)\n", file.Path, file.SnapshotID)
			}
		}

		return nil
	},
}

var findChangesCmd = &cobra.Command{
	Use:   "find-changes",
	Short: "Find files that changed between snapshots",
	RunE: func(cmd *cobra.Command, args []string) error {
		baseDir, _ := cmd.Flags().GetString("dir")
		if baseDir == "" {
			return fmt.Errorf("--dir must be specified")
		}

		oldID, _ := cmd.Flags().GetString("old")
		newID, _ := cmd.Flags().GetString("new")
		if oldID == "" || newID == "" {
			return fmt.Errorf("both --old and --new must be specified")
		}

		// Create a store
		store, err := storage.NewStore(baseDir, storage.DefaultStoreOptions())
		if err != nil {
			return err
		}
		defer store.Close()

		// Create a query engine
		queryEngine := query.NewQueryEngine(store)

		// Build query options
		options := query.DefaultQueryOptions()
		pattern, _ := cmd.Flags().GetString("pattern")
		if pattern != "" {
			options.Pattern = pattern
		}

		// Find changes
		changes, err := queryEngine.FindFilesChangedBetweenSnapshots(oldID, newID, options)
		if err != nil {
			return err
		}

		// Print results
		for _, change := range changes {
			fmt.Printf("%s\n", change.Path)
		}

		return nil
	},
}

var findLargestCmd = &cobra.Command{
	Use:   "find-largest",
	Short: "Find the N largest files in a snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		baseDir, _ := cmd.Flags().GetString("dir")
		if baseDir == "" {
			return fmt.Errorf("--dir must be specified")
		}

		snapshotID, _ := cmd.Flags().GetString("snapshot")
		if snapshotID == "" {
			return fmt.Errorf("--snapshot must be specified")
		}

		n, _ := cmd.Flags().GetInt("n")
		if n <= 0 {
			return fmt.Errorf("--n must be greater than 0")
		}

		// Create a store
		store, err := storage.NewStore(baseDir, storage.DefaultStoreOptions())
		if err != nil {
			return err
		}
		defer store.Close()

		// Create a query engine
		queryEngine := query.NewQueryEngine(store)

		// Build query options
		options := query.DefaultQueryOptions()
		pattern, _ := cmd.Flags().GetString("pattern")
		if pattern != "" {
			options.Pattern = pattern
		}

		// Find largest files
		results, err := queryEngine.FindLargestFiles(snapshotID, n, options)
		if err != nil {
			return err
		}

		// Print results
		for _, result := range results {
			fmt.Printf("%s\t%d\n", result.Path, result.Size)
		}

		return nil
	},
}

func init() {
	// Set the base directory
	baseDir := filepath.Join(os.Getenv("HOME"), ".flashfs")

	// Add the query command to the root command
	RootCmd.AddCommand(queryCmd)

	// Add flags to the query command
	queryCmd.Flags().String("dir", baseDir, "Base directory containing snapshots")
	queryCmd.Flags().String("pattern", "", "File pattern to match")
	queryCmd.Flags().String("min-size", "", "Minimum file size (e.g. 1MB)")
	queryCmd.Flags().String("max-size", "", "Maximum file size (e.g. 1GB)")
	queryCmd.Flags().String("start-time", "", "Start time (RFC3339)")
	queryCmd.Flags().String("end-time", "", "End time (RFC3339)")
	queryCmd.Flags().String("hash", "", "File hash to match")
	queryCmd.Flags().Bool("is-dir", false, "Match directories only")
	queryCmd.Flags().Bool("all-snapshots", false, "Query all snapshots")
	queryCmd.Flags().String("snapshot", "", "Snapshot ID to query")

	// Add subcommands
	queryCmd.AddCommand(findDuplicatesCmd)
	queryCmd.AddCommand(findChangesCmd)
	queryCmd.AddCommand(findLargestCmd)

	// Add flags to the find-duplicates command
	findDuplicatesCmd.Flags().String("dir", "", "Base directory containing snapshots")
	findDuplicatesCmd.Flags().StringSlice("snapshots", nil, "Snapshot IDs to search")
	findDuplicatesCmd.Flags().String("pattern", "", "File pattern to match")

	// Add flags to the find-changes command
	findChangesCmd.Flags().String("dir", "", "Base directory containing snapshots")
	findChangesCmd.Flags().String("old", "", "Old snapshot ID")
	findChangesCmd.Flags().String("new", "", "New snapshot ID")
	findChangesCmd.Flags().String("pattern", "", "File pattern to match")

	// Add flags to the find-largest command
	findLargestCmd.Flags().String("dir", "", "Base directory containing snapshots")
	findLargestCmd.Flags().String("snapshot", "", "Snapshot ID to query")
	findLargestCmd.Flags().Int("n", 10, "Number of files to return")
	findLargestCmd.Flags().String("pattern", "", "File pattern to match")
}

func parseSize(s string) (int64, error) {
	s = strings.ToUpper(s)
	multiplier := int64(1)

	if strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = s[:len(s)-2]
	} else if strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = s[:len(s)-2]
	} else if strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-2]
	} else if strings.HasSuffix(s, "TB") {
		multiplier = 1024 * 1024 * 1024 * 1024
		s = s[:len(s)-2]
	}

	size, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}

	return size * multiplier, nil
}
