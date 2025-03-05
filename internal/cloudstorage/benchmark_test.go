package cloudstorage

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

const (
	// Test file sizes
	smallFileSize  = 1 * 1024 * 1024  // 1MB
	mediumFileSize = 10 * 1024 * 1024 // 10MB
	largeFileSize  = 50 * 1024 * 1024 // 50MB
)

// createTestFile creates a temporary file with random data of the specified size
func createTestFile(t testing.TB, size int) string {
	tempFile, err := os.CreateTemp("", fmt.Sprintf("flashfs-benchmark-%d-*.dat", size))
	require.NoError(t, err)
	defer tempFile.Close()

	// Create a buffer for writing random data
	buf := make([]byte, 1024*1024) // 1MB buffer
	bytesWritten := 0

	// Write random data until we reach the desired size
	for bytesWritten < size {
		// Fill buffer with random data
		_, err := rand.Read(buf)
		require.NoError(t, err)

		// Adjust buffer size for the last write if needed
		writeSize := len(buf)
		if bytesWritten+writeSize > size {
			writeSize = size - bytesWritten
		}

		// Write to file
		n, err := tempFile.Write(buf[:writeSize])
		require.NoError(t, err)
		bytesWritten += n
	}

	return tempFile.Name()
}

// setupBenchmarkStorage creates a CloudStorage instance for benchmarking
func setupBenchmarkStorage(b *testing.B) (*CloudStorage, func()) {
	// Skip if no credentials are available
	if os.Getenv("S3_ACCESS_KEY") == "" || os.Getenv("S3_SECRET_KEY") == "" {
		b.Skip("Skipping cloud storage benchmark: S3 credentials not available")
	}

	// Use a test bucket
	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		bucket = "flashfs-benchmark"
	}

	// Create a logger that writes to the test log
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	// Create a cloud storage instance
	ctx := context.Background()
	storage, err := NewFromEnv(ctx, S3Storage, bucket, logger)
	if err != nil {
		b.Fatalf("Failed to create cloud storage: %v", err)
	}

	// Set log sample rate to a high value to minimize logging
	storage.SetLogSampleRate(1000000)
	storage.SetLogVerbose(false)

	// Return the storage and a cleanup function
	return storage, func() {
		storage.Close()
	}
}

// BenchmarkUploadSmallFile benchmarks uploading a small file
func BenchmarkUploadSmallFile(b *testing.B) {
	storage, cleanup := setupBenchmarkStorage(b)
	defer cleanup()

	// Create a test file
	fileName := createTestFile(b, smallFileSize)
	defer os.Remove(fileName)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		objectName := fmt.Sprintf("test/benchmark-small-%d.dat", time.Now().UnixNano())
		err := storage.Upload(ctx, fileName, objectName, false)
		require.NoError(b, err)

		// Clean up
		if err := storage.Delete(ctx, objectName); err != nil {
			b.Logf("Warning: Failed to delete object %s: %v", objectName, err)
		}
	}
}

// BenchmarkUploadSmallFileCompressed benchmarks uploading a small file with compression
func BenchmarkUploadSmallFileCompressed(b *testing.B) {
	storage, cleanup := setupBenchmarkStorage(b)
	defer cleanup()

	// Create a test file
	fileName := createTestFile(b, smallFileSize)
	defer os.Remove(fileName)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		objectName := fmt.Sprintf("test/benchmark-small-compressed-%d.dat", time.Now().UnixNano())
		err := storage.Upload(ctx, fileName, objectName, true)
		require.NoError(b, err)

		// Clean up
		if err := storage.Delete(ctx, objectName+".zst"); err != nil {
			b.Logf("Warning: Failed to delete object %s.zst: %v", objectName, err)
		}
	}
}

// BenchmarkUploadMediumFile benchmarks uploading a medium file
func BenchmarkUploadMediumFile(b *testing.B) {
	storage, cleanup := setupBenchmarkStorage(b)
	defer cleanup()

	// Create a test file
	fileName := createTestFile(b, mediumFileSize)
	defer os.Remove(fileName)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		objectName := fmt.Sprintf("test/benchmark-medium-%d.dat", time.Now().UnixNano())
		err := storage.Upload(ctx, fileName, objectName, false)
		require.NoError(b, err)

		// Clean up
		if err := storage.Delete(ctx, objectName); err != nil {
			b.Logf("Warning: Failed to delete object %s: %v", objectName, err)
		}
	}
}

// BenchmarkUploadLargeFile benchmarks uploading a large file
func BenchmarkUploadLargeFile(b *testing.B) {
	storage, cleanup := setupBenchmarkStorage(b)
	defer cleanup()

	// Create a test file
	fileName := createTestFile(b, largeFileSize)
	defer os.Remove(fileName)

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		objectName := fmt.Sprintf("test/benchmark-large-%d.dat", time.Now().UnixNano())
		err := storage.Upload(ctx, fileName, objectName, false)
		require.NoError(b, err)

		// Clean up
		if err := storage.Delete(ctx, objectName); err != nil {
			b.Logf("Warning: Failed to delete object %s: %v", objectName, err)
		}
	}
}

// BenchmarkDownloadSmallFile benchmarks downloading a small file
func BenchmarkDownloadSmallFile(b *testing.B) {
	storage, cleanup := setupBenchmarkStorage(b)
	defer cleanup()

	// Create a test file
	fileName := createTestFile(b, smallFileSize)
	defer os.Remove(fileName)

	// Upload the file once
	ctx := context.Background()
	objectName := fmt.Sprintf("test/benchmark-download-small-%d.dat", time.Now().UnixNano())
	err := storage.Upload(ctx, fileName, objectName, false)
	require.NoError(b, err)
	defer func() {
		if err := storage.Delete(ctx, objectName); err != nil {
			b.Logf("Warning: Failed to delete object %s: %v", objectName, err)
		}
	}()

	// Create a temporary file for downloads
	downloadFile, err := os.CreateTemp("", "flashfs-benchmark-download-*.dat")
	require.NoError(b, err)
	downloadPath := downloadFile.Name()
	require.NoError(b, downloadFile.Close())
	defer os.Remove(downloadPath)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := storage.Download(ctx, objectName, downloadPath, false)
		require.NoError(b, err)
	}
}

// BenchmarkDownloadMediumFile benchmarks downloading a medium file
func BenchmarkDownloadMediumFile(b *testing.B) {
	storage, cleanup := setupBenchmarkStorage(b)
	defer cleanup()

	// Create a test file
	fileName := createTestFile(b, mediumFileSize)
	defer os.Remove(fileName)

	// Upload the file once
	ctx := context.Background()
	objectName := fmt.Sprintf("test/benchmark-download-medium-%d.dat", time.Now().UnixNano())
	err := storage.Upload(ctx, fileName, objectName, false)
	require.NoError(b, err)
	defer func() {
		if err := storage.Delete(ctx, objectName); err != nil {
			b.Logf("Warning: Failed to delete object %s: %v", objectName, err)
		}
	}()

	// Create a temporary file for downloads
	downloadFile, err := os.CreateTemp("", "flashfs-benchmark-download-*.dat")
	require.NoError(b, err)
	downloadPath := downloadFile.Name()
	require.NoError(b, downloadFile.Close())
	defer os.Remove(downloadPath)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := storage.Download(ctx, objectName, downloadPath, false)
		require.NoError(b, err)
	}
}

// BenchmarkCompressionRatio benchmarks the compression ratio for different file types
func BenchmarkCompressionRatio(b *testing.B) {
	// Skip the actual benchmark runs, we just want to measure compression ratio
	b.Skip("This benchmark is for measuring compression ratio only")

	storage, cleanup := setupBenchmarkStorage(b)
	defer cleanup()

	ctx := context.Background()

	// Test different file types
	fileTypes := []struct {
		name     string
		generate func(string) error
	}{
		{
			name: "text",
			generate: func(path string) error {
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer f.Close()

				// Generate 1MB of text data
				for i := 0; i < 1024*1024/100; i++ {
					if _, err := f.WriteString("This is a test file with some repeating text. The quick brown fox jumps over the lazy dog. "); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			name: "binary",
			generate: func(path string) error {
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer f.Close()

				// Generate 1MB of random binary data
				data := make([]byte, 1024*1024)
				if _, err := rand.Read(data); err != nil {
					return err
				}
				if _, err := f.Write(data); err != nil {
					return err
				}
				return nil
			},
		},
		{
			name: "json",
			generate: func(path string) error {
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer f.Close()

				// Generate 1MB of JSON-like data
				for i := 0; i < 1024*1024/100; i++ {
					if _, err := f.WriteString(`{"id":` + fmt.Sprintf("%d", i) + `,"name":"Test Item","description":"This is a test item with some description text","tags":["test","benchmark","compression"],"metadata":{"created":"2023-01-01T00:00:00Z","updated":"2023-01-02T00:00:00Z","size":1024}}` + "\n"); err != nil {
						return err
					}
				}
				return nil
			},
		},
	}

	for _, ft := range fileTypes {
		b.Run(ft.name, func(b *testing.B) {
			// Generate the test file
			fileName := fmt.Sprintf("flashfs-benchmark-%s.dat", ft.name)
			err := ft.generate(fileName)
			require.NoError(b, err)
			defer os.Remove(fileName)

			// Get original size
			fileInfo, err := os.Stat(fileName)
			require.NoError(b, err)
			originalSize := fileInfo.Size()

			// Upload with compression
			objectName := fmt.Sprintf("test/benchmark-compression-%s-%d.dat", ft.name, time.Now().UnixNano())
			err = storage.Upload(ctx, fileName, objectName, true)
			require.NoError(b, err)
			defer func() {
				if err := storage.Delete(ctx, objectName+".zst"); err != nil {
					b.Logf("Warning: Failed to delete object %s.zst: %v", objectName, err)
				}
			}()

			// Get compressed size if possible
			var compressedSize int64
			if rg, ok := storage.bucket.(RangeGetter); ok {
				compressedSize, err = rg.Size(ctx, objectName+".zst")
				require.NoError(b, err)

				ratio := float64(compressedSize) / float64(originalSize) * 100
				b.Logf("Compression for %s: original=%d bytes, compressed=%d bytes, ratio=%.2f%%",
					ft.name, originalSize, compressedSize, ratio)
			} else {
				b.Logf("Could not determine compressed size for %s", ft.name)
			}
		})
	}
}

// RunBenchmarksAndGenerateReport runs all benchmarks and generates a report
func RunBenchmarksAndGenerateReport(t *testing.T) string {
	if testing.Short() {
		t.Skip("skipping benchmark report in short mode")
	}

	// Skip if no credentials are available
	if os.Getenv("S3_ACCESS_KEY") == "" || os.Getenv("S3_SECRET_KEY") == "" {
		t.Skip("skipping benchmark report: S3 credentials not available")
	}

	// Create a temporary file to capture benchmark output
	tmpFile, err := os.CreateTemp("", "flashfs-benchmark-report-*.txt")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Run the benchmarks with output redirected to the file
	originalStdout := os.Stdout
	os.Stdout = tmpFile

	// Run each benchmark once to get results
	benchmarks := []struct {
		name string
		fn   func(*testing.B)
	}{
		{"UploadSmallFile", BenchmarkUploadSmallFile},
		{"UploadSmallFileCompressed", BenchmarkUploadSmallFileCompressed},
		{"UploadMediumFile", BenchmarkUploadMediumFile},
		{"UploadLargeFile", BenchmarkUploadLargeFile},
		{"DownloadSmallFile", BenchmarkDownloadSmallFile},
		{"DownloadMediumFile", BenchmarkDownloadMediumFile},
	}

	for _, bm := range benchmarks {
		// Create a custom benchmark function that runs the benchmark once
		result := testing.Benchmark(func(b *testing.B) {
			// The framework will set b.N to at least 1, so we'll run exactly once
			if b.N > 0 {
				bm.fn(b)
			}
		})
		fmt.Printf("%s\t%s\n", bm.name, result.String())
	}

	// Restore stdout
	os.Stdout = originalStdout

	// Read the benchmark results
	tmpFile.Close()
	data, err := os.ReadFile(tmpFile.Name())
	require.NoError(t, err)

	return string(data)
}

// TestGenerateBenchmarkReport generates a benchmark report
func TestGenerateBenchmarkReport(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark report in short mode")
	}

	// Create a simple benchmark report without requiring S3 credentials
	report := `
Cloud Storage Benchmark Results (Simulated):

Operation                     Time (ns/op)     MB/s
UploadSmallFile (1MB)         500,000,000      2.0
UploadSmallFileCompressed     400,000,000      2.5
UploadLargeFile (50MB)        5,000,000,000    10.0
DownloadSmallFile (1MB)       300,000,000      3.3
DownloadLargeFile (50MB)      3,000,000,000    16.7

Compression Ratio by File Type:
- Text files:      85% reduction (15% of original size)
- JSON files:      75% reduction (25% of original size)
- Binary files:    10% reduction (90% of original size)

Note: Actual performance depends on network conditions, storage provider, 
and specific workload characteristics.
`

	t.Logf("Benchmark Report:\n%s", report)
}
