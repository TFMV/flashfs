package storage

import (
	cryptorand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/flashfs/internal/serializer"
	"github.com/TFMV/flashfs/internal/walker"
	"github.com/TFMV/flashfs/schema/flashfs"
	"github.com/stretchr/testify/require"
)

const (
	// Test file counts for different benchmark scenarios
	smallFileCount  = 100
	mediumFileCount = 1000
	largeFileCount  = 5000

	// File size ranges
	minFileSize = 1024       // 1KB
	maxFileSize = 1024 * 100 // 100KB
)

// setupTestDirectory creates a test directory with the specified number of files
func setupTestDirectory(t testing.TB, fileCount int) string {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "flashfs-benchmark-")
	require.NoError(t, err)

	// Create random files
	for i := 0; i < fileCount; i++ {
		// Create subdirectories for some files to simulate a realistic filesystem
		var filePath string
		if i%10 == 0 {
			// Create a subdirectory
			subdir := filepath.Join(tempDir, fmt.Sprintf("dir%d", i/10))
			err := os.MkdirAll(subdir, 0755)
			require.NoError(t, err)
			filePath = filepath.Join(subdir, fmt.Sprintf("file%d.txt", i))
		} else {
			filePath = filepath.Join(tempDir, fmt.Sprintf("file%d.txt", i))
		}

		// Determine file size (random between min and max)
		fileSize := rand.Intn(maxFileSize-minFileSize) + minFileSize

		// Create and write to the file
		err := createRandomFile(filePath, fileSize)
		require.NoError(t, err)
	}

	return tempDir
}

// createRandomFile creates a file with random content
func createRandomFile(path string, size int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	data := make([]byte, size)
	_, err = cryptorand.Read(data)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	return err
}

// modifyDirectory makes random changes to a directory for diff testing
func modifyDirectory(t testing.TB, dir string, changePercentage int) {
	// Walk the directory to get all files
	entries, err := filepath.Glob(filepath.Join(dir, "**", "*"))
	require.NoError(t, err)

	// Calculate how many files to modify
	filesToModify := len(entries) * changePercentage / 100

	// Randomly select files to modify
	for i := 0; i < filesToModify; i++ {
		// Pick a random file
		fileIndex := rand.Intn(len(entries))
		filePath := entries[fileIndex]

		// Skip directories
		fileInfo, err := os.Stat(filePath)
		require.NoError(t, err)
		if fileInfo.IsDir() {
			continue
		}

		// Determine what kind of modification to make
		modType := rand.Intn(3)
		switch modType {
		case 0:
			// Modify content
			fileSize := rand.Intn(maxFileSize-minFileSize) + minFileSize
			err := createRandomFile(filePath, fileSize)
			require.NoError(t, err)
		case 1:
			// Delete file
			err := os.Remove(filePath)
			require.NoError(t, err)
		case 2:
			// Create new file
			newFilePath := filepath.Join(filepath.Dir(filePath), fmt.Sprintf("new_file_%d.txt", rand.Intn(1000)))
			fileSize := rand.Intn(maxFileSize-minFileSize) + minFileSize
			err := createRandomFile(newFilePath, fileSize)
			require.NoError(t, err)
		}
	}
}

// BenchmarkCreateSnapshot benchmarks creating snapshots with different file counts
func BenchmarkCreateSnapshot(b *testing.B) {
	benchmarks := []struct {
		name      string
		fileCount int
	}{
		{"Small", smallFileCount},
		{"Medium", mediumFileCount},
		{"Large", largeFileCount},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Setup test directory
			testDir := setupTestDirectory(b, bm.fileCount)
			defer os.RemoveAll(testDir)

			// Setup snapshot store
			snapshotDir, err := os.MkdirTemp("", "flashfs-snapshot-")
			require.NoError(b, err)
			defer os.RemoveAll(snapshotDir)

			store, err := NewSnapshotStore(snapshotDir)
			require.NoError(b, err)
			defer store.Close()

			// Reset timer before the actual benchmark
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Walk the directory to get entries
				entries, err := walker.Walk(testDir)
				require.NoError(b, err)

				// Serialize and store the snapshot
				snapshotData, err := serializeEntries(entries)
				require.NoError(b, err)

				err = store.WriteSnapshot(fmt.Sprintf("snapshot_%d", i), snapshotData)
				require.NoError(b, err)
			}
		})
	}
}

// Helper function to serialize walker entries
func serializeEntries(entries []walker.SnapshotEntry) ([]byte, error) {
	// Use the actual serializer from the serializer package
	return serializer.SerializeSnapshot(entries)
}

// BenchmarkComputeDiff benchmarks computing diffs between snapshots
func BenchmarkComputeDiff(b *testing.B) {
	benchmarks := []struct {
		name             string
		fileCount        int
		changePercentage int
	}{
		{"Small_5pct", smallFileCount, 5},
		{"Small_20pct", smallFileCount, 20},
		{"Medium_5pct", mediumFileCount, 5},
		{"Medium_20pct", mediumFileCount, 20},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Setup test directory
			testDir := setupTestDirectory(b, bm.fileCount)
			defer os.RemoveAll(testDir)

			// Setup snapshot store
			snapshotDir, err := os.MkdirTemp("", "flashfs-snapshot-")
			require.NoError(b, err)
			defer os.RemoveAll(snapshotDir)

			store, err := NewSnapshotStore(snapshotDir)
			require.NoError(b, err)
			defer store.Close()

			// Create first snapshot
			entries1, err := walker.Walk(testDir)
			require.NoError(b, err)
			snapshotData1, err := serializeEntries(entries1)
			require.NoError(b, err)
			err = store.WriteSnapshot("snapshot_before", snapshotData1)
			require.NoError(b, err)

			// Modify directory
			modifyDirectory(b, testDir, bm.changePercentage)

			// Create second snapshot
			entries2, err := walker.Walk(testDir)
			require.NoError(b, err)
			snapshotData2, err := serializeEntries(entries2)
			require.NoError(b, err)
			err = store.WriteSnapshot("snapshot_after", snapshotData2)
			require.NoError(b, err)

			// Reset timer before the actual benchmark
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := store.ComputeDiff("snapshot_before", "snapshot_after")
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkQuerySnapshot benchmarks querying snapshots
func BenchmarkQuerySnapshot(b *testing.B) {
	benchmarks := []struct {
		name      string
		fileCount int
	}{
		{"Small", smallFileCount},
		{"Medium", mediumFileCount},
		{"Large", largeFileCount},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Setup test directory
			testDir := setupTestDirectory(b, bm.fileCount)
			defer os.RemoveAll(testDir)

			// Setup snapshot store
			snapshotDir, err := os.MkdirTemp("", "flashfs-snapshot-")
			require.NoError(b, err)
			defer os.RemoveAll(snapshotDir)

			store, err := NewSnapshotStore(snapshotDir)
			require.NoError(b, err)
			defer store.Close()

			// Create snapshot
			entries, err := walker.Walk(testDir)
			require.NoError(b, err)
			snapshotData, err := serializeEntries(entries)
			require.NoError(b, err)
			err = store.WriteSnapshot("test_snapshot", snapshotData)
			require.NoError(b, err)

			// Reset timer before the actual benchmark
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Query for files matching a pattern
				_, err := store.QuerySnapshot("test_snapshot", func(entry *flashfs.FileEntry) bool {
					// Match files with "file" in the name
					return entry != nil && len(entry.Path()) > 0 && entry.Path()[0] == 'f'
				})
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkApplyDiff benchmarks applying diffs to snapshots
func BenchmarkApplyDiff(b *testing.B) {
	benchmarks := []struct {
		name             string
		fileCount        int
		changePercentage int
	}{
		{"Small_5pct", smallFileCount, 5},
		{"Small_20pct", smallFileCount, 20},
		{"Medium_5pct", mediumFileCount, 5},
		{"Medium_20pct", mediumFileCount, 20},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Setup test directory
			testDir := setupTestDirectory(b, bm.fileCount)
			defer os.RemoveAll(testDir)

			// Setup snapshot store
			snapshotDir, err := os.MkdirTemp("", "flashfs-snapshot-")
			require.NoError(b, err)
			defer os.RemoveAll(snapshotDir)

			store, err := NewSnapshotStore(snapshotDir)
			require.NoError(b, err)
			defer store.Close()

			// Create first snapshot
			entries1, err := walker.Walk(testDir)
			require.NoError(b, err)
			snapshotData1, err := serializeEntries(entries1)
			require.NoError(b, err)
			err = store.WriteSnapshot("snapshot_before", snapshotData1)
			require.NoError(b, err)

			// Modify directory
			modifyDirectory(b, testDir, bm.changePercentage)

			// Create second snapshot
			entries2, err := walker.Walk(testDir)
			require.NoError(b, err)
			snapshotData2, err := serializeEntries(entries2)
			require.NoError(b, err)
			err = store.WriteSnapshot("snapshot_after", snapshotData2)
			require.NoError(b, err)

			// Compute and store diff
			err = store.StoreDiff("snapshot_before", "snapshot_after")
			require.NoError(b, err)

			// Reset timer before the actual benchmark
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := store.ApplyDiff("snapshot_before", "diff_snapshot_before_snapshot_after")
				require.NoError(b, err)
			}
		})
	}
}

// RunBenchmarksAndGenerateReport runs all benchmarks and generates a report
func RunBenchmarksAndGenerateReport(t *testing.T) string {
	if testing.Short() {
		t.Skip("skipping benchmark report in short mode")
	}

	// Redirect stdout to capture benchmark output
	originalStdout := os.Stdout
	tmpFile, err := os.CreateTemp("", "benchmark-*.txt")
	require.NoError(t, err)
	os.Stdout = tmpFile

	// Define benchmarks to run
	benchmarks := []struct {
		name string
		fn   func(b *testing.B)
	}{
		{"CreateSnapshot/Small", func(b *testing.B) {
			b.Run("Small", func(b *testing.B) {
				// Setup once before the benchmark
				testDir := setupTestDirectory(b, smallFileCount)
				defer os.RemoveAll(testDir)

				snapshotDir, err := os.MkdirTemp("", "flashfs-snapshot-")
				require.NoError(b, err)
				defer os.RemoveAll(snapshotDir)

				store, err := NewSnapshotStore(snapshotDir)
				require.NoError(b, err)
				defer store.Close()

				// Pre-walk the directory to avoid including walk time in the benchmark
				entries, err := walker.Walk(testDir)
				require.NoError(b, err)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					// Only benchmark the serialization and storage
					snapshotData, err := serializeEntries(entries)
					require.NoError(b, err)

					err = store.WriteSnapshot(fmt.Sprintf("snapshot_%d", i), snapshotData)
					require.NoError(b, err)
				}
			})
		}},
		{"ComputeDiff/Small_5pct", func(b *testing.B) {
			b.Run("Small_5pct", func(b *testing.B) {
				testDir := setupTestDirectory(b, smallFileCount)
				defer os.RemoveAll(testDir)

				snapshotDir, err := os.MkdirTemp("", "flashfs-snapshot-")
				require.NoError(b, err)
				defer os.RemoveAll(snapshotDir)

				store, err := NewSnapshotStore(snapshotDir)
				require.NoError(b, err)
				defer store.Close()

				entries1, err := walker.Walk(testDir)
				require.NoError(b, err)
				snapshotData1, err := serializeEntries(entries1)
				require.NoError(b, err)
				err = store.WriteSnapshot("snapshot_before", snapshotData1)
				require.NoError(b, err)

				modifyDirectory(b, testDir, 5)

				entries2, err := walker.Walk(testDir)
				require.NoError(b, err)
				snapshotData2, err := serializeEntries(entries2)
				require.NoError(b, err)
				err = store.WriteSnapshot("snapshot_after", snapshotData2)
				require.NoError(b, err)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_, err := store.ComputeDiff("snapshot_before", "snapshot_after")
					require.NoError(b, err)
				}
			})
		}},
		{"QuerySnapshot/Small", func(b *testing.B) {
			b.Run("Small", func(b *testing.B) {
				testDir := setupTestDirectory(b, smallFileCount)
				defer os.RemoveAll(testDir)

				snapshotDir, err := os.MkdirTemp("", "flashfs-snapshot-")
				require.NoError(b, err)
				defer os.RemoveAll(snapshotDir)

				store, err := NewSnapshotStore(snapshotDir)
				require.NoError(b, err)
				defer store.Close()

				entries, err := walker.Walk(testDir)
				require.NoError(b, err)
				snapshotData, err := serializeEntries(entries)
				require.NoError(b, err)
				err = store.WriteSnapshot("test_snapshot", snapshotData)
				require.NoError(b, err)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_, err := store.QuerySnapshot("test_snapshot", func(entry *flashfs.FileEntry) bool {
						return entry != nil && len(entry.Path()) > 0 && entry.Path()[0] == 'f'
					})
					require.NoError(b, err)
				}
			})
		}},
		{"ApplyDiff/Small_5pct", func(b *testing.B) {
			b.Run("Small_5pct", func(b *testing.B) {
				testDir := setupTestDirectory(b, smallFileCount)
				defer os.RemoveAll(testDir)

				snapshotDir, err := os.MkdirTemp("", "flashfs-snapshot-")
				require.NoError(b, err)
				defer os.RemoveAll(snapshotDir)

				store, err := NewSnapshotStore(snapshotDir)
				require.NoError(b, err)
				defer store.Close()

				entries1, err := walker.Walk(testDir)
				require.NoError(b, err)
				snapshotData1, err := serializeEntries(entries1)
				require.NoError(b, err)
				err = store.WriteSnapshot("snapshot_before", snapshotData1)
				require.NoError(b, err)

				modifyDirectory(b, testDir, 5)

				entries2, err := walker.Walk(testDir)
				require.NoError(b, err)
				snapshotData2, err := serializeEntries(entries2)
				require.NoError(b, err)
				err = store.WriteSnapshot("snapshot_after", snapshotData2)
				require.NoError(b, err)

				err = store.StoreDiff("snapshot_before", "snapshot_after")
				require.NoError(b, err)

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_, err := store.ApplyDiff("snapshot_before", "diff_snapshot_before_snapshot_after")
					require.NoError(b, err)
				}
			})
		}},
	}

	// Run each benchmark with multiple iterations
	for _, bm := range benchmarks {
		// Create a custom benchmark function that runs multiple times
		benchFunc := func(b *testing.B) {
			// Run the benchmark with default b.N value
			bm.fn(b)
		}

		result := testing.Benchmark(benchFunc)
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

	report := RunBenchmarksAndGenerateReport(t)
	t.Logf("Benchmark Report:\n%s", report)
}
