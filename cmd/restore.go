package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/TFMV/flashfs/internal/cloudstorage"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/spf13/cobra"
)

var restoreCmd = &cobra.Command{
	Use:   "restore [source]",
	Short: "Restore snapshots from cloud storage",
	Long: `Restore snapshots from cloud storage (S3, GCS, or compatible).

Examples:
  flashfs restore s3://mybucket/flashfs-backups/
  flashfs restore gcs://my-bucket-name/

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

		// Get source
		source := args[0]

		// Validate source
		if err := cloudstorage.ValidateDestination(source); err != nil {
			return fmt.Errorf("invalid source: %w", err)
		}

		// Parse source
		storageType, bucket, prefix, err := cloudstorage.ParseDestination(source)
		if err != nil {
			return fmt.Errorf("failed to parse source: %w", err)
		}

		// Get flags
		baseDir, _ := cmd.Flags().GetString("dir")
		snapshotName, _ := cmd.Flags().GetString("snapshot")
		decompress, _ := cmd.Flags().GetBool("decompress")
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

		// Create base directory if it doesn't exist
		if err := os.MkdirAll(baseDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Restore snapshots
		if all {
			// List objects with the given prefix
			objects, err := cloudStorage.List(ctx, prefix)
			if err != nil {
				return fmt.Errorf("failed to list objects: %w", err)
			}

			if len(objects) == 0 {
				fmt.Println("No snapshots found")
				return nil
			}

			fmt.Printf("Restoring %d snapshots from %s\n", len(objects), source)

			for _, object := range objects {
				// Skip directories
				if strings.HasSuffix(object, "/") {
					continue
				}

				// Extract snapshot name from object name
				snapshotName := filepath.Base(object)
				if decompress && strings.HasSuffix(snapshotName, ".zst") {
					snapshotName = strings.TrimSuffix(snapshotName, ".zst")
				}

				// Restore snapshot
				if err := restoreSnapshot(ctx, baseDir, cloudStorage, object, snapshotName, decompress); err != nil {
					return fmt.Errorf("failed to restore snapshot %s: %w", snapshotName, err)
				}
			}

			fmt.Printf("Successfully restored %d snapshots from %s\n", len(objects), source)
		} else if snapshotName != "" {
			// Restore a specific snapshot
			objectName := cloudstorage.GetObjectName(prefix, snapshotName)
			if decompress {
				objectName += ".zst"
			}

			// Check if the object exists
			exists, err := cloudStorage.Exists(ctx, objectName)
			if err != nil {
				return fmt.Errorf("failed to check if object exists: %w", err)
			}

			if !exists {
				return fmt.Errorf("snapshot %s does not exist", snapshotName)
			}

			fmt.Printf("Restoring snapshot %s from %s\n", snapshotName, source)

			// Restore snapshot
			if err := restoreSnapshot(ctx, baseDir, cloudStorage, objectName, snapshotName, decompress); err != nil {
				return fmt.Errorf("failed to restore snapshot %s: %w", snapshotName, err)
			}

			fmt.Printf("Successfully restored snapshot %s from %s\n", snapshotName, source)
		} else {
			return fmt.Errorf("either --snapshot or --all flag must be specified")
		}

		return nil
	},
}

func restoreSnapshot(ctx context.Context, baseDir string, cloudStorage *cloudstorage.CloudStorage, objectName, snapshotName string, decompress bool) error {
	// Get the snapshot file path
	snapshotPath := filepath.Join(baseDir, snapshotName)

	// Download the snapshot
	if err := cloudStorage.Download(ctx, objectName, snapshotPath, decompress); err != nil {
		return fmt.Errorf("failed to download snapshot: %w", err)
	}

	return nil
}

func init() {
	RootCmd.AddCommand(restoreCmd)

	// Required flags
	restoreCmd.Flags().String("dir", "snapshots", "Directory to restore snapshots to")
	restoreCmd.Flags().String("snapshot", "", "Name of the snapshot to restore")
	restoreCmd.Flags().Bool("all", false, "Restore all snapshots")
	restoreCmd.Flags().Bool("decompress", true, "Decompress snapshots after downloading")

	// Mark flags as required
	if err := restoreCmd.MarkFlagRequired("dir"); err != nil {
		panic(err)
	}
}
