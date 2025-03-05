package cloudstorage

import (
	"context"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDestination(t *testing.T) {
	tests := []struct {
		name            string
		destination     string
		wantStorageType StorageType
		wantBucket      string
		wantPrefix      string
		wantErr         bool
	}{
		{
			name:            "valid S3 URL",
			destination:     "s3://mybucket/path/to/snapshots",
			wantStorageType: S3Storage,
			wantBucket:      "mybucket",
			wantPrefix:      "path/to/snapshots",
			wantErr:         false,
		},
		{
			name:            "valid GCS URL",
			destination:     "gcs://mybucket/path/to/snapshots",
			wantStorageType: GCSStorage,
			wantBucket:      "mybucket",
			wantPrefix:      "path/to/snapshots",
			wantErr:         false,
		},
		{
			name:            "invalid URL scheme",
			destination:     "ftp://mybucket/path/to/snapshots",
			wantStorageType: "",
			wantBucket:      "",
			wantPrefix:      "",
			wantErr:         true,
		},
		{
			name:            "invalid URL format",
			destination:     "s3:mybucket/path/to/snapshots",
			wantStorageType: "",
			wantBucket:      "",
			wantPrefix:      "",
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStorageType, gotBucket, gotPrefix, err := ParseDestination(tt.destination)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantStorageType, gotStorageType)
			assert.Equal(t, tt.wantBucket, gotBucket)
			assert.Equal(t, tt.wantPrefix, gotPrefix)
		})
	}
}

func TestGetObjectName(t *testing.T) {
	tests := []struct {
		name         string
		prefix       string
		snapshotName string
		want         string
	}{
		{
			name:         "empty prefix",
			prefix:       "",
			snapshotName: "snapshot1",
			want:         "snapshot1",
		},
		{
			name:         "prefix without trailing slash",
			prefix:       "path/to/snapshots",
			snapshotName: "snapshot1",
			want:         "path/to/snapshots/snapshot1",
		},
		{
			name:         "prefix with trailing slash",
			prefix:       "path/to/snapshots/",
			snapshotName: "snapshot1",
			want:         "path/to/snapshots/snapshot1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetObjectName(tt.prefix, tt.snapshotName)
			assert.Equal(t, tt.want, got)
		})
	}
}

// This test requires actual credentials and is skipped by default
func TestCloudStorageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Skip if no credentials are available
	if os.Getenv("S3_ACCESS_KEY") == "" || os.Getenv("S3_SECRET_KEY") == "" {
		t.Skip("skipping integration test: S3 credentials not available")
	}

	ctx := context.Background()
	logger := log.NewLogfmtLogger(os.Stdout)

	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "flashfs-cloudstorage-test-*.txt")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Write some data to the file
	testData := []byte("test data for cloud storage")
	_, err = tempFile.Write(testData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	// Create a cloud storage client
	storageType := S3Storage
	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("skipping integration test: S3_TEST_BUCKET not set")
	}

	storage, err := NewFromEnv(ctx, storageType, bucket, logger)
	require.NoError(t, err)
	defer storage.Close()

	// Test object name
	objectName := "test/flashfs-cloudstorage-test.txt"

	// Upload the file
	err = storage.Upload(ctx, tempFile.Name(), objectName, true)
	require.NoError(t, err)

	// Check if the object exists
	exists, err := storage.Exists(ctx, objectName+".zst")
	require.NoError(t, err)
	assert.True(t, exists)

	// Download the file
	downloadPath := tempFile.Name() + ".downloaded"
	defer os.Remove(downloadPath)
	err = storage.Download(ctx, objectName+".zst", downloadPath, true)
	require.NoError(t, err)

	// Verify the downloaded content
	downloadedData, err := os.ReadFile(downloadPath)
	require.NoError(t, err)
	assert.Equal(t, testData, downloadedData)

	// List objects
	objects, err := storage.List(ctx, "test/")
	require.NoError(t, err)
	assert.Contains(t, objects, objectName+".zst")

	// Delete the object
	err = storage.Delete(ctx, objectName+".zst")
	require.NoError(t, err)

	// Verify the object is deleted
	exists, err = storage.Exists(ctx, objectName+".zst")
	require.NoError(t, err)
	assert.False(t, exists)
}
