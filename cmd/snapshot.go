package cmd

import (
	"fmt"
	"path/filepath"

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

		entries, err := walker.WalkWithContext(ctx, path)
		if err != nil {
			return err
		}

		// Check if context is canceled
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

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
