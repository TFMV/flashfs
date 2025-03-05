package cloudstorage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

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

// TestBenchmarkStorage is a test that benchmarks storage operations
func TestBenchmarkStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	// Skip if no credentials are available
	if os.Getenv("S3_ACCESS_KEY") == "" || os.Getenv("S3_SECRET_KEY") == "" {
		t.Skip("skipping benchmark test: S3 credentials not available")
	}

	ctx := context.Background()
	logger := log.NewLogfmtLogger(os.Stdout)

	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "flashfs-benchmark-test-*.dat")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Write 1MB of data to the file
	testData := make([]byte, 1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	_, err = tempFile.Write(testData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	// Create a cloud storage client
	storageType := S3Storage
	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("skipping benchmark test: S3_TEST_BUCKET not set")
	}

	storage, err := NewFromEnv(ctx, storageType, bucket, logger)
	require.NoError(t, err)
	defer storage.Close()

	// Test object name
	objectName := "test/flashfs-benchmark-test.dat"

	// Run the benchmark
	iterations := 3
	if testing.Short() {
		iterations = 1
	}

	err = runStorageBenchmark(ctx, t, storage, tempFile.Name(), objectName, false, iterations)
	require.NoError(t, err)

	// Run with compression
	err = runStorageBenchmark(ctx, t, storage, tempFile.Name(), objectName+"-compressed", true, iterations)
	require.NoError(t, err)
}

// runStorageBenchmark runs a benchmark of upload and download performance
func runStorageBenchmark(ctx context.Context, t *testing.T, cs *CloudStorage, localFile, objectName string, compress bool, iterations int) error {
	var totalUploadTime, totalDownloadTime time.Duration
	var totalUploadBytes, totalDownloadBytes int64

	for i := 0; i < iterations; i++ {
		// Upload benchmark
		start := time.Now()
		if err := cs.Upload(ctx, localFile, objectName, compress); err != nil {
			return err
		}
		uploadTime := time.Since(start)
		totalUploadTime += uploadTime

		// Get file size for bandwidth calculation
		fileInfo, err := os.Stat(localFile)
		if err != nil {
			return err
		}
		totalUploadBytes += fileInfo.Size()

		// Download benchmark (to a temporary file)
		tmpFile := localFile + ".download"
		start = time.Now()
		if err := cs.Download(ctx, objectName, tmpFile, compress); err != nil {
			return err
		}
		downloadTime := time.Since(start)
		totalDownloadTime += downloadTime

		// Get downloaded file size
		downloadInfo, err := os.Stat(tmpFile)
		if err != nil {
			return err
		}
		totalDownloadBytes += downloadInfo.Size()

		// Clean up
		_ = os.Remove(tmpFile)
		_ = cs.Delete(ctx, objectName)
	}

	avgUpload := totalUploadTime / time.Duration(iterations)
	avgDownload := totalDownloadTime / time.Duration(iterations)
	avgUploadMBps := float64(totalUploadBytes) / totalUploadTime.Seconds() / 1024 / 1024
	avgDownloadMBps := float64(totalDownloadBytes) / totalDownloadTime.Seconds() / 1024 / 1024

	t.Logf("Benchmark results for bucket %s (compress=%v):", cs.bucket.Name(), compress)
	t.Logf("Average upload time: %v (%.2f MB/s)", avgUpload, avgUploadMBps)
	t.Logf("Average download time: %v (%.2f MB/s)", avgDownload, avgDownloadMBps)

	return nil
}

// BenchmarkUploadDownload is a Go benchmark function that measures upload and download performance
func BenchmarkUploadDownload(b *testing.B) {
	// Skip if no credentials are available
	if os.Getenv("S3_ACCESS_KEY") == "" || os.Getenv("S3_SECRET_KEY") == "" {
		b.Skip("skipping benchmark: S3 credentials not available")
	}

	ctx := context.Background()
	logger := log.NewNopLogger() // Use NopLogger to avoid logging during benchmarks

	// Create a temporary file for testing with 1MB of data
	tempFile, err := os.CreateTemp("", "flashfs-benchmark-*.dat")
	require.NoError(b, err)
	defer os.Remove(tempFile.Name())

	// Write 1MB of data to the file
	testData := make([]byte, 1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	_, err = tempFile.Write(testData)
	require.NoError(b, err)
	require.NoError(b, tempFile.Close())

	// Create a cloud storage client
	storageType := S3Storage
	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		b.Skip("skipping benchmark: S3_TEST_BUCKET not set")
	}

	storage, err := NewFromEnv(ctx, storageType, bucket, logger)
	require.NoError(b, err)
	defer storage.Close()

	// Test object name
	objectName := fmt.Sprintf("test/flashfs-benchmark-%d.dat", time.Now().UnixNano())
	tmpFile := tempFile.Name() + ".download"
	defer os.Remove(tmpFile)

	// Reset the timer before the benchmark loop
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Upload
		err = storage.Upload(ctx, tempFile.Name(), objectName, false)
		require.NoError(b, err)

		// Download
		err = storage.Download(ctx, objectName, tmpFile, false)
		require.NoError(b, err)

		// Clean up
		_ = os.Remove(tmpFile)
		_ = storage.Delete(ctx, objectName)
	}
}

// BenchmarkUploadDownloadCompressed is a Go benchmark function that measures compressed upload and download performance
func BenchmarkUploadDownloadCompressed(b *testing.B) {
	// Skip if no credentials are available
	if os.Getenv("S3_ACCESS_KEY") == "" || os.Getenv("S3_SECRET_KEY") == "" {
		b.Skip("skipping benchmark: S3 credentials not available")
	}

	ctx := context.Background()
	logger := log.NewNopLogger() // Use NopLogger to avoid logging during benchmarks

	// Create a temporary file for testing with 1MB of data
	tempFile, err := os.CreateTemp("", "flashfs-benchmark-*.dat")
	require.NoError(b, err)
	defer os.Remove(tempFile.Name())

	// Write 1MB of data to the file
	testData := make([]byte, 1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	_, err = tempFile.Write(testData)
	require.NoError(b, err)
	require.NoError(b, tempFile.Close())

	// Create a cloud storage client
	storageType := S3Storage
	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		b.Skip("skipping benchmark: S3_TEST_BUCKET not set")
	}

	storage, err := NewFromEnv(ctx, storageType, bucket, logger)
	require.NoError(b, err)
	defer storage.Close()

	// Test object name
	objectName := fmt.Sprintf("test/flashfs-benchmark-compressed-%d.dat", time.Now().UnixNano())
	tmpFile := tempFile.Name() + ".download"
	defer os.Remove(tmpFile)

	// Reset the timer before the benchmark loop
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Upload with compression
		err = storage.Upload(ctx, tempFile.Name(), objectName, true)
		require.NoError(b, err)

		// Download with decompression
		err = storage.Download(ctx, objectName+".zst", tmpFile, true)
		require.NoError(b, err)

		// Clean up
		_ = os.Remove(tmpFile)
		_ = storage.Delete(ctx, objectName+".zst")
	}
}
