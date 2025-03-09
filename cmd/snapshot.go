package cmd

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/TFMV/flashfs/internal/serializer"
	"github.com/TFMV/flashfs/internal/storage"
	"github.com/TFMV/flashfs/internal/walker"
	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Take a full snapshot of the filesystem",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		path, _ := cmd.Flags().GetString("path")
		output, _ := cmd.Flags().GetString("output")
		if path == "" || output == "" {
			return fmt.Errorf("both --path and --output must be specified")
		}

		fmt.Printf("Starting snapshot of %s\n", path)

		// Check if context is canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Use streaming walker with callback for better memory efficiency and progress reporting
		var entries []walker.SnapshotEntry
		var processedCount int64
		var lastReportTime = time.Now()
		var reportInterval = 1 * time.Second

		// Set up walk options
		options := walker.DefaultWalkOptions()

		err := walker.WalkStreamWithCallback(ctx, path, options, func(entry walker.SnapshotEntry) error {
			entries = append(entries, entry)

			// Update processed count and report progress periodically
			newCount := atomic.AddInt64(&processedCount, 1)

			// Report progress every second
			if time.Since(lastReportTime) > reportInterval {
				fmt.Printf("\rProcessed %d files/directories...", newCount)
				lastReportTime = time.Now()
			}

			return nil
		})

		// Final progress update
		fmt.Printf("\rProcessed %d files/directories\n", atomic.LoadInt64(&processedCount))

		if err != nil {
			return err
		}

		// Check if context is canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Println("Serializing snapshot data...")
		fbData, err := serializer.SerializeSnapshot(entries)
		if err != nil {
			return err
		}

		// Check if context is canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Create a snapshot store
		store, err := storage.NewSnapshotStore(filepath.Dir(output))
		if err != nil {
			return err
		}
		defer store.Close()

		// Use the snapshot name as the basename without extension
		snapshotName := filepath.Base(output)
		if ext := filepath.Ext(snapshotName); ext != "" {
			snapshotName = snapshotName[:len(snapshotName)-len(ext)]
		}

		// Check if context is canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Println("Writing snapshot to disk...")
		if err := store.WriteSnapshot(snapshotName, fbData); err != nil {
			return err
		}
		fmt.Printf("Snapshot saved to %s\n", output)
		return nil
	},
}

func init() {
	snapshotCmd.Flags().String("path", "", "Path to snapshot")
	snapshotCmd.Flags().String("output", "", "Output file for snapshot")
	RootCmd.AddCommand(snapshotCmd)
}
