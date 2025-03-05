package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/TFMV/flashfs/internal/cloudstorage"
	"github.com/TFMV/flashfs/internal/storage"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/spf13/cobra"
)

var exportCmd = &cobra.Command{
	Use:   "export [destination]",
	Short: "Export snapshots to cloud storage",
	Long: `Export snapshots to cloud storage (S3, GCS, or compatible).

Examples:
  flashfs export s3://mybucket/flashfs-backups/
  flashfs export gcs://my-bucket-name/

Environment variables for S3:
  S3_ENDPOINT        - Custom endpoint for S3-compatible storage (e.g., MinIO)
  S3_ACCESS_KEY      - Access key for S3
  S3_SECRET_KEY      - Secret key for S3
  S3_REGION          - Region for S3 (default: us-east-1)
  S3_INSECURE        - Use insecure connection (default: false)
  S3_FORCE_PATH_STYLE - Use path-style addressing (default: false)

Environment variables for GCS:
  GOOGLE_APPLICATION_CREDENTIALS - Path to service account JSON file
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get context from command
		ctx := cmd.Context()

		// Get destination
		destination := args[0]

		// Validate destination
		if err := cloudstorage.ValidateDestination(destination); err != nil {
			return fmt.Errorf("invalid destination: %w", err)
		}

		// Parse destination
		storageType, bucket, prefix, err := cloudstorage.ParseDestination(destination)
		if err != nil {
			return fmt.Errorf("failed to parse destination: %w", err)
		}

		// Get flags
		baseDir, _ := cmd.Flags().GetString("dir")
		snapshotName, _ := cmd.Flags().GetString("snapshot")
		compress, _ := cmd.Flags().GetBool("compress")
		all, _ := cmd.Flags().GetBool("all")

		// Create logger
		logger := kitlog.NewLogfmtLogger(os.Stderr)
		logger = level.NewFilter(logger, level.AllowInfo())

		// Create cloud storage client
		cloudStorage, err := cloudstorage.NewFromEnv(ctx, storageType, bucket, logger)
		if err != nil {
			return fmt.Errorf("failed to create cloud storage client: %w", err)
		}
		defer cloudStorage.Close()

		// Create snapshot store
		snapshotStore, err := storage.NewSnapshotStore(baseDir)
		if err != nil {
			return fmt.Errorf("failed to create snapshot store: %w", err)
		}
		defer snapshotStore.Close()

		// Export snapshots
		if all {
			// Export all snapshots
			snapshots, err := snapshotStore.ListSnapshots()
			if err != nil {
				return fmt.Errorf("failed to list snapshots: %w", err)
			}

			if len(snapshots) == 0 {
				fmt.Println("No snapshots found")
				return nil
			}

			fmt.Printf("Exporting %d snapshots to %s\n", len(snapshots), destination)

			for _, snapshot := range snapshots {
				if err := exportSnapshot(ctx, baseDir, cloudStorage, snapshot, prefix, compress); err != nil {
					return fmt.Errorf("failed to export snapshot %s: %w", snapshot, err)
				}
			}

			fmt.Printf("Successfully exported %d snapshots to %s\n", len(snapshots), destination)
		} else if snapshotName != "" {
			// Export a specific snapshot
			fmt.Printf("Exporting snapshot %s to %s\n", snapshotName, destination)

			if err := exportSnapshot(ctx, baseDir, cloudStorage, snapshotName, prefix, compress); err != nil {
				return fmt.Errorf("failed to export snapshot %s: %w", snapshotName, err)
			}

			fmt.Printf("Successfully exported snapshot %s to %s\n", snapshotName, destination)
		} else {
			return fmt.Errorf("either --snapshot or --all flag must be specified")
		}

		return nil
	},
}

func exportSnapshot(ctx context.Context, baseDir string, cloudStorage *cloudstorage.CloudStorage, snapshotName, prefix string, compress bool) error {
	// Get the snapshot file path
	snapshotPath := filepath.Join(baseDir, snapshotName)

	// Check if the snapshot exists
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		return fmt.Errorf("snapshot %s does not exist", snapshotName)
	}

	// Generate object name
	objectName := cloudstorage.GetObjectName(prefix, snapshotName)

	// Upload the snapshot
	if err := cloudStorage.Upload(ctx, snapshotPath, objectName, compress); err != nil {
		return fmt.Errorf("failed to upload snapshot: %w", err)
	}

	return nil
}

func init() {
	RootCmd.AddCommand(exportCmd)

	// Required flags
	exportCmd.Flags().String("dir", "snapshots", "Directory containing snapshots")
	exportCmd.Flags().String("snapshot", "", "Name of the snapshot to export")
	exportCmd.Flags().Bool("all", false, "Export all snapshots")
	exportCmd.Flags().Bool("compress", true, "Compress snapshots before uploading")

	// Mark flags as required
	if err := exportCmd.MarkFlagRequired("dir"); err != nil {
		panic(err)
	}
}
