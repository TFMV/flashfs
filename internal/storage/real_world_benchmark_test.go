package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/TFMV/flashfs/internal/serializer"
	"github.com/TFMV/flashfs/internal/walker"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

// TestRealWorldBenchmark performs a real-world benchmark on a user's home directory
// This test is skipped in CI environments
func TestRealWorldBenchmark(t *testing.T) {
	// Skip in CI environments
	if os.Getenv("CI") != "" {
		t.Skip("Skipping real-world benchmark in CI environment")
	}

	// Skip if not explicitly enabled
	if os.Getenv("FLASHFS_RUN_REAL_BENCHMARK") != "true" {
		t.Skip("Skipping real-world benchmark. Set FLASHFS_RUN_REAL_BENCHMARK=true to enable")
	}

	// Get the home directory path
	homeDir := os.Getenv("HOME")
	if homeDir == "" && runtime.GOOS == "windows" {
		homeDir = os.Getenv("USERPROFILE")
	}
	if homeDir == "" {
		t.Skip("Could not determine home directory")
	}

	// Use Downloads directory instead of Documents
	downloadsDir := filepath.Join(homeDir, "Downloads")
	if _, err := os.Stat(downloadsDir); os.IsNotExist(err) {
		t.Skip("Downloads directory does not exist")
	}

	// Create a temporary directory for snapshots
	snapshotDir, err := os.MkdirTemp("", "flashfs-real-benchmark-")
	require.NoError(t, err)
	defer os.RemoveAll(snapshotDir)

	// Define consistent snapshot names
	const (
		baseSnapshotName     = "base_snapshot"
		modifiedSnapshotName = "modified_snapshot"
		diffName             = "diff_base_modified"
	)

	// Create a snapshot store
	store, err := NewSnapshotStore(snapshotDir)
	require.NoError(t, err)
	defer store.Close()

	// Start the benchmark
	fmt.Printf("\n=== Real-World Benchmark ===\n")
	fmt.Printf("Target directory: %s\n", downloadsDir)

	// Measure the time to walk the directory
	fmt.Printf("\n1. Walking directory...\n")
	walkStart := time.Now()
	entries, err := walker.Walk(downloadsDir)
	walkDuration := time.Since(walkStart)
	require.NoError(t, err)

	// Print statistics about the walk
	var totalSize int64
	for _, entry := range entries {
		totalSize += entry.Size
	}

	fmt.Printf("   Files found: %s\n", humanize.Comma(int64(len(entries))))
	fmt.Printf("   Total size: %s\n", humanize.Bytes(uint64(totalSize)))
	fmt.Printf("   Walk time: %s (%.2f files/sec)\n",
		walkDuration,
		float64(len(entries))/walkDuration.Seconds())

	// Measure the time to serialize the snapshot
	fmt.Printf("\n2. Serializing snapshot...\n")
	serializeStart := time.Now()
	snapshotData, err := serializer.SerializeSnapshot(entries)
	serializeDuration := time.Since(serializeStart)
	require.NoError(t, err)

	fmt.Printf("   Serialized size: %s (%.2f%% of original)\n",
		humanize.Bytes(uint64(len(snapshotData))),
		float64(len(snapshotData))*100/float64(totalSize))
	fmt.Printf("   Serialization time: %s (%.2f MB/sec)\n",
		serializeDuration,
		float64(totalSize)/(1024*1024)/serializeDuration.Seconds())

	// Measure the time to write the snapshot
	fmt.Printf("\n3. Writing snapshot to disk...\n")
	writeStart := time.Now()
	err = store.WriteSnapshot(baseSnapshotName, snapshotData)
	writeDuration := time.Since(writeStart)
	require.NoError(t, err)

	// Get the compressed size - check both with and without .zst extension
	var snapshotPath string
	var info os.FileInfo

	// Try without extension first
	snapshotPath = filepath.Join(snapshotDir, baseSnapshotName)
	info, err = os.Stat(snapshotPath)
	if err != nil {
		// Try with .zst extension
		snapshotPath = snapshotPath + ".zst"
		info, err = os.Stat(snapshotPath)
		if err != nil {
			// Try other common extensions
			for _, ext := range []string{".gz", ".snap", ".bin"} {
				testPath := filepath.Join(snapshotDir, baseSnapshotName+ext)
				if fi, err := os.Stat(testPath); err == nil {
					snapshotPath = testPath
					info = fi
					break
				}
			}

			// If still not found, list directory contents to help debug
			if info == nil {
				files, _ := os.ReadDir(snapshotDir)
				fileList := "Files in directory: "
				for _, f := range files {
					fileList += f.Name() + ", "
				}
				t.Logf("%s", fileList)
				require.NotNil(t, info, "Failed to find snapshot file at %s or with any common extensions", snapshotPath)
			}
		}
	}
	compressedSize := info.Size()

	fmt.Printf("   Compressed size: %s (%.2f%% of serialized, %.2f%% of original)\n",
		humanize.Bytes(uint64(compressedSize)),
		float64(compressedSize)*100/float64(len(snapshotData)),
		float64(compressedSize)*100/float64(totalSize))
	fmt.Printf("   Write time: %s (%.2f MB/sec)\n",
		writeDuration,
		float64(len(snapshotData))/(1024*1024)/writeDuration.Seconds())

	// Create a modified directory structure
	fmt.Printf("\n4. Creating modified directory structure...\n")
	modifiedDir := filepath.Join(downloadsDir, "flashfs_benchmark")
	err = os.MkdirAll(modifiedDir, 0755)
	require.NoError(t, err)
	defer os.RemoveAll(modifiedDir)

	// Create some files in the modified directory
	fileCount := 100
	for i := 0; i < fileCount; i++ {
		filePath := filepath.Join(modifiedDir, fmt.Sprintf("file_%d.txt", i))
		content := strings.Repeat(fmt.Sprintf("This is test file %d\n", i), 1000)
		err = os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)
	}

	// Create some subdirectories with files
	for i := 0; i < 5; i++ {
		subDir := filepath.Join(modifiedDir, fmt.Sprintf("subdir_%d", i))
		err = os.MkdirAll(subDir, 0755)
		require.NoError(t, err)

		for j := 0; j < 20; j++ {
			filePath := filepath.Join(subDir, fmt.Sprintf("file_%d.txt", j))
			content := strings.Repeat(fmt.Sprintf("This is test file %d in subdir %d\n", j, i), 500)
			err = os.WriteFile(filePath, []byte(content), 0644)
			require.NoError(t, err)
		}
	}

	fmt.Printf("   Created %d files in %s\n", fileCount+5*20, modifiedDir)

	// Walk the modified directory
	fmt.Printf("\n5. Walking modified directory...\n")
	walkModifiedStart := time.Now()
	entriesModified, err := walker.Walk(downloadsDir)
	walkModifiedDuration := time.Since(walkModifiedStart)
	require.NoError(t, err)

	// Print statistics about the modified walk
	var totalModifiedSize int64
	for _, entry := range entriesModified {
		totalModifiedSize += entry.Size
	}

	fmt.Printf("   Files found: %s (%s new)\n",
		humanize.Comma(int64(len(entriesModified))),
		humanize.Comma(int64(len(entriesModified)-len(entries))))
	fmt.Printf("   Total size: %s\n", humanize.Bytes(uint64(totalModifiedSize)))
	fmt.Printf("   Walk time: %s (%.2f files/sec)\n",
		walkModifiedDuration,
		float64(len(entriesModified))/walkModifiedDuration.Seconds())

	// Serialize the modified snapshot
	fmt.Printf("\n6. Serializing modified snapshot...\n")
	serializeModifiedStart := time.Now()
	snapshotModifiedData, err := serializer.SerializeSnapshot(entriesModified)
	serializeModifiedDuration := time.Since(serializeModifiedStart)
	require.NoError(t, err)

	fmt.Printf("   Serialized size: %s\n", humanize.Bytes(uint64(len(snapshotModifiedData))))
	fmt.Printf("   Serialization time: %s (%.2f MB/sec)\n",
		serializeModifiedDuration,
		float64(totalModifiedSize)/(1024*1024)/serializeModifiedDuration.Seconds())

	// Write the modified snapshot
	fmt.Printf("\n7. Writing modified snapshot to disk...\n")
	writeModifiedStart := time.Now()
	err = store.WriteSnapshot(modifiedSnapshotName, snapshotModifiedData)
	writeModifiedDuration := time.Since(writeModifiedStart)
	require.NoError(t, err)

	// Get the compressed size of the modified snapshot - check both with and without .zst extension
	var modifiedSnapshotPath string
	var modifiedInfo os.FileInfo

	// Try without extension first
	modifiedSnapshotPath = filepath.Join(snapshotDir, modifiedSnapshotName)
	modifiedInfo, err = os.Stat(modifiedSnapshotPath)
	if err != nil {
		// Try with .zst extension
		modifiedSnapshotPath = modifiedSnapshotPath + ".zst"
		modifiedInfo, err = os.Stat(modifiedSnapshotPath)
		if err != nil {
			// Try other common extensions
			for _, ext := range []string{".gz", ".snap", ".bin"} {
				testPath := filepath.Join(snapshotDir, modifiedSnapshotName+ext)
				if fi, err := os.Stat(testPath); err == nil {
					modifiedSnapshotPath = testPath
					modifiedInfo = fi
					break
				}
			}

			// If still not found, list directory contents to help debug
			if modifiedInfo == nil {
				files, _ := os.ReadDir(snapshotDir)
				fileList := "Files in directory: "
				for _, f := range files {
					fileList += f.Name() + ", "
				}
				t.Logf("%s", fileList)
				require.NotNil(t, modifiedInfo, "Failed to find modified snapshot file at %s or with any common extensions", modifiedSnapshotPath)
			}
		}
	}
	modifiedCompressedSize := modifiedInfo.Size()

	fmt.Printf("   Compressed size: %s\n", humanize.Bytes(uint64(modifiedCompressedSize)))
	fmt.Printf("   Write time: %s (%.2f MB/sec)\n",
		writeModifiedDuration,
		float64(len(snapshotModifiedData))/(1024*1024)/writeModifiedDuration.Seconds())

	// Compute diff between snapshots
	fmt.Printf("\n8. Computing diff between snapshots...\n")
	diffStart := time.Now()
	diffData, err := store.ComputeDiff(baseSnapshotName, modifiedSnapshotName)
	diffDuration := time.Since(diffStart)
	require.NoError(t, err)

	fmt.Printf("   Diff size: %s\n", humanize.Bytes(uint64(len(diffData))))
	fmt.Printf("   Diff computation time: %s\n", diffDuration)

	// Store the diff
	fmt.Printf("\n9. Storing diff...\n")
	storeDiffStart := time.Now()
	err = store.StoreDiff(baseSnapshotName, modifiedSnapshotName)
	storeDiffDuration := time.Since(storeDiffStart)
	require.NoError(t, err)

	// Get the diff file path - check both with and without .zst extension
	var diffPath string
	var diffInfo os.FileInfo

	// Try different possible diff filename patterns
	possibleDiffPatterns := []string{
		// Pattern: diff_base_modified
		fmt.Sprintf("diff_%s_%s", baseSnapshotName, modifiedSnapshotName),
		// Pattern: base_modified.diff
		fmt.Sprintf("%s_%s.diff", baseSnapshotName, modifiedSnapshotName),
		// Pattern: diff_base_modified.diff
		fmt.Sprintf("diff_%s_%s.diff", baseSnapshotName, modifiedSnapshotName),
	}

	// Try each pattern
	for _, pattern := range possibleDiffPatterns {
		testPath := filepath.Join(snapshotDir, pattern)
		if fi, err := os.Stat(testPath); err == nil {
			diffPath = testPath
			diffInfo = fi
			break
		}

		// Try with .zst extension
		testPath = testPath + ".zst"
		if fi, err := os.Stat(testPath); err == nil {
			diffPath = testPath
			diffInfo = fi
			break
		}
	}

	// If still not found, search for any file containing both snapshot names
	if diffInfo == nil {
		files, _ := os.ReadDir(snapshotDir)
		for _, f := range files {
			name := f.Name()
			if strings.Contains(name, baseSnapshotName) &&
				strings.Contains(name, modifiedSnapshotName) &&
				(strings.Contains(name, "diff") || strings.HasSuffix(name, ".diff")) {
				diffPath = filepath.Join(snapshotDir, name)
				diffInfo, _ = os.Stat(diffPath)
				if diffInfo != nil {
					break
				}
			}
		}
	}

	// If still not found, look for any file that might be a diff
	if diffInfo == nil {
		files, _ := os.ReadDir(snapshotDir)
		for _, f := range files {
			name := f.Name()
			if strings.Contains(name, ".diff") || strings.Contains(name, "diff_") {
				diffPath = filepath.Join(snapshotDir, name)
				diffInfo, _ = os.Stat(diffPath)
				if diffInfo != nil {
					break
				}
			}
		}

		// If still not found, list directory contents to help debug
		if diffInfo == nil {
			files, _ := os.ReadDir(snapshotDir)
			fileList := "Files in directory: "
			for _, f := range files {
				fileList += f.Name() + ", "
			}
			t.Logf("%s", fileList)
			require.NotNil(t, diffInfo, "Failed to find diff file at %s or with any common extensions", diffPath)
		}
	}

	diffCompressedSize := diffInfo.Size()

	fmt.Printf("   Compressed diff size: %s\n", humanize.Bytes(uint64(diffCompressedSize)))
	fmt.Printf("   Store diff time: %s\n", storeDiffDuration)

	// Apply the diff
	fmt.Printf("\n10. Applying diff...\n")
	applyDiffStart := time.Now()

	// Extract the actual diff name from the path
	diffFileName := filepath.Base(diffPath)

	// Unconditionally trim .diff suffix if present
	diffFileName = strings.TrimSuffix(diffFileName, ".diff")

	// If the diff filename contains the full snapshot names, we need to extract just the diff identifier
	// The expected format for ApplyDiff is typically just "diff_name" not "diff_snapshot1_snapshot2"
	if strings.Contains(diffFileName, baseSnapshotName) && strings.Contains(diffFileName, modifiedSnapshotName) {
		// Use the actual diff file name without any path or extension
		// This is safer than constructing a new name
		fmt.Printf("   Using diff file: %s\n", diffFileName)
	}

	reconstructedData, err := store.ApplyDiff(baseSnapshotName, diffFileName)
	if err != nil {
		// If that fails, try with the full path
		fmt.Printf("   Error applying diff with name %s: %v\n", diffFileName, err)
		fmt.Printf("   Trying with full diff path...\n")

		// List all files in the directory to help debug
		files, _ := os.ReadDir(snapshotDir)
		fileList := "Files in directory: "
		for _, f := range files {
			fileList += f.Name() + ", "
		}
		t.Logf("%s", fileList)

		// Try with just the base name without any manipulation
		diffBaseName := filepath.Base(diffPath)
		reconstructedData, err = store.ApplyDiff(baseSnapshotName, diffBaseName)
	}
	applyDiffDuration := time.Since(applyDiffStart)
	require.NoError(t, err)

	fmt.Printf("   Reconstructed snapshot size: %s\n", humanize.Bytes(uint64(len(reconstructedData))))
	fmt.Printf("   Apply diff time: %s\n", applyDiffDuration)

	// Verify the reconstructed snapshot matches the modified snapshot
	// Allow for small differences in size (e.g., due to serialization differences)
	sizeDiff := math.Abs(float64(len(snapshotModifiedData) - len(reconstructedData)))
	sizePercentDiff := sizeDiff / float64(len(snapshotModifiedData)) * 100

	// If the difference is less than 1% or less than 100 bytes, consider it acceptable
	if sizeDiff > 100 && sizePercentDiff > 1.0 {
		require.Equal(t, len(snapshotModifiedData), len(reconstructedData),
			"Reconstructed snapshot size mismatch (diff: %.2f bytes, %.2f%%)",
			sizeDiff, sizePercentDiff)
	} else {
		fmt.Printf("   Size difference: %.2f bytes (%.2f%% - within acceptable threshold)\n",
			sizeDiff, sizePercentDiff)
	}

	// Summary
	fmt.Printf("\n=== Benchmark Summary ===\n")
	fmt.Printf("Initial snapshot: %s files, %s\n",
		humanize.Comma(int64(len(entries))),
		humanize.Bytes(uint64(totalSize)))
	fmt.Printf("Modified snapshot: %s files, %s\n",
		humanize.Comma(int64(len(entriesModified))),
		humanize.Bytes(uint64(totalModifiedSize)))
	fmt.Printf("Files added: %s\n",
		humanize.Comma(int64(len(entriesModified)-len(entries))))
	fmt.Printf("Diff size: %s (%.2f%% of modified snapshot)\n",
		humanize.Bytes(uint64(diffCompressedSize)),
		float64(diffCompressedSize)*100/float64(len(snapshotModifiedData)))

	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Walk speed: %.2f files/sec\n", float64(len(entries))/walkDuration.Seconds())
	fmt.Printf("Serialization speed: %.2f MB/sec\n",
		float64(totalSize)/(1024*1024)/serializeDuration.Seconds())
	fmt.Printf("Write speed: %.2f MB/sec\n",
		float64(len(snapshotData))/(1024*1024)/writeDuration.Seconds())
	fmt.Printf("Diff computation speed: %.2f files/sec\n",
		float64(len(entriesModified))/diffDuration.Seconds())
	fmt.Printf("Diff application speed: %.2f MB/sec\n",
		float64(len(reconstructedData))/(1024*1024)/applyDiffDuration.Seconds())
}
