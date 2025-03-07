package walker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWalk tests the basic functionality of the Walk function
func TestWalk(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "walker-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create some test files and directories
	dirs := []string{
		filepath.Join(tempDir, "dir1"),
		filepath.Join(tempDir, "dir2"),
		filepath.Join(tempDir, "dir1", "subdir1"),
	}

	files := []string{
		filepath.Join(tempDir, "file1.txt"),
		filepath.Join(tempDir, "file2.txt"),
		filepath.Join(tempDir, "dir1", "file3.txt"),
		filepath.Join(tempDir, "dir2", "file4.txt"),
		filepath.Join(tempDir, "dir1", "subdir1", "file5.txt"),
	}

	// Create directories
	for _, dir := range dirs {
		err := os.MkdirAll(dir, 0755)
		require.NoError(t, err)
	}

	// Create files with some content
	for _, file := range files {
		err := os.WriteFile(file, []byte("test content"), 0644)
		require.NoError(t, err)
	}

	// Test Walk function
	entries, err := Walk(tempDir)
	require.NoError(t, err)

	// Verify we have the expected number of entries (dirs + files + root directory)
	// The +1 is for the root directory itself, which is now included in the results
	require.Equal(t, len(dirs)+len(files)+1, len(entries))

	// Test WalkWithContext function
	ctx := context.Background()
	entriesWithCtx, err := WalkWithContext(ctx, tempDir)
	require.NoError(t, err)
	require.Equal(t, len(entries), len(entriesWithCtx))

	// Test context cancellation
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	_, err = WalkWithContext(cancelCtx, tempDir)
	require.Error(t, err)
}

// createSmallBenchmarkDirStructure creates a small directory structure for benchmarking
func createSmallBenchmarkDirStructure(b *testing.B, root string, depth, width int) {
	if depth <= 0 {
		return
	}

	// Create files at this level
	for i := 0; i < width; i++ {
		file := filepath.Join(root, fmt.Sprintf("file%d.txt", i))
		err := os.WriteFile(file, []byte("test content"), 0644)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Create subdirectories and recurse
	for i := 0; i < width; i++ {
		dir := filepath.Join(root, fmt.Sprintf("dir%d", i))
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			b.Fatal(err)
		}
		createSmallBenchmarkDirStructure(b, dir, depth-1, width)
	}
}

// BenchmarkStdlibWalkDir benchmarks the standard library's filepath.WalkDir
func BenchmarkStdlibWalkDir(b *testing.B) {
	fmt.Println("Starting BenchmarkStdlibWalkDir")

	// Create a small directory structure for benchmarking
	tempDir, err := os.MkdirTemp("", "walker-bench-stdlib-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	fmt.Printf("Created temp directory: %s\n", tempDir)

	// Create a small directory structure (depth=3, width=3)
	fmt.Println("Creating directory structure...")
	createSmallBenchmarkDirStructure(b, tempDir, 3, 3)
	fmt.Println("Directory structure created")

	// Count the number of files and directories
	var fileCount int
	err = filepath.WalkDir(tempDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fileCount++
		return nil
	})
	if err != nil {
		b.Fatalf("Error counting files: %v", err)
	}
	fmt.Printf("Total files and directories: %d\n", fileCount)

	b.ResetTimer()
	fmt.Println("Starting benchmark iterations...")

	for i := 0; i < b.N; i++ {
		fmt.Printf("Iteration %d/%d\n", i+1, b.N)
		var count int

		startTime := time.Now()
		err := filepath.WalkDir(tempDir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			count++
			return nil
		})
		duration := time.Since(startTime)

		if err != nil {
			b.Fatalf("Error in WalkDir: %v", err)
		}

		fmt.Printf("Iteration %d completed in %v, found %d entries\n", i+1, duration, count)
		b.ReportMetric(float64(count), "entries")
	}

	fmt.Println("BenchmarkStdlibWalkDir completed")
}

// BenchmarkWalkerWalk benchmarks our custom Walk implementation
func BenchmarkWalkerWalk(b *testing.B) {
	fmt.Println("Starting BenchmarkWalkerWalk")

	// Create a small directory structure for benchmarking
	tempDir, err := os.MkdirTemp("", "walker-bench-walker-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	fmt.Printf("Created temp directory: %s\n", tempDir)

	// Create a small directory structure (depth=3, width=3)
	fmt.Println("Creating directory structure...")
	createSmallBenchmarkDirStructure(b, tempDir, 3, 3)
	fmt.Println("Directory structure created")

	// Count the number of files and directories
	entries, err := Walk(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	fmt.Printf("Total files and directories: %d\n", len(entries))

	b.ResetTimer()
	fmt.Println("Starting benchmark iterations...")

	for i := 0; i < b.N; i++ {
		fmt.Printf("Iteration %d/%d\n", i+1, b.N)

		startTime := time.Now()
		entries, err := Walk(tempDir)
		duration := time.Since(startTime)

		if err != nil {
			b.Fatalf("Error in Walk: %v", err)
		}

		fmt.Printf("Iteration %d completed in %v, found %d entries\n", i+1, duration, len(entries))
		b.ReportMetric(float64(len(entries)), "entries")
	}

	fmt.Println("BenchmarkWalkerWalk completed")
}

// TestCompareWalkImplementations directly compares filepath.WalkDir with our implementation
func TestCompareWalkImplementations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "walker-compare-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a small directory structure
	fmt.Println("Creating test directory structure...")
	createTestDirStructure(t, tempDir, 3, 3)
	fmt.Println("Test directory structure created")

	// Count files with filepath.WalkDir
	fmt.Println("Testing filepath.WalkDir...")
	stdlibStart := time.Now()
	var stdlibCount int
	err = filepath.WalkDir(tempDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		stdlibCount++
		return nil
	})
	stdlibDuration := time.Since(stdlibStart)
	require.NoError(t, err)
	fmt.Printf("filepath.WalkDir found %d entries in %v\n", stdlibCount, stdlibDuration)

	// Count files with our Walk implementation (with hashing)
	fmt.Println("Testing our Walk implementation...")
	walkStart := time.Now()
	entries, err := Walk(tempDir)
	walkDuration := time.Since(walkStart)
	require.NoError(t, err)
	fmt.Printf("Our Walk implementation found %d entries in %v\n", len(entries), walkDuration)

	// Count files with our Walk implementation (without hashing)
	fmt.Println("Testing our Walk implementation without hashing...")
	options := DefaultWalkOptions()
	options.ComputeHashes = false
	walkNoHashStart := time.Now()
	entriesNoHash, err := WalkWithOptions(context.Background(), tempDir, options)
	walkNoHashDuration := time.Since(walkNoHashStart)
	require.NoError(t, err)
	fmt.Printf("Our Walk implementation without hashing found %d entries in %v\n", len(entriesNoHash), walkNoHashDuration)

	// Compare results
	if stdlibCount != len(entries) {
		t.Errorf("Count mismatch: filepath.WalkDir found %d entries, our Walk found %d entries",
			stdlibCount, len(entries))
	} else {
		fmt.Printf("Both implementations found the same number of entries: %d\n", stdlibCount)
		fmt.Printf("filepath.WalkDir took %v, our Walk took %v, our Walk without hashing took %v\n",
			stdlibDuration, walkDuration, walkNoHashDuration)

		if stdlibDuration < walkDuration {
			fmt.Println("filepath.WalkDir was faster than our Walk with hashing!")
		} else {
			fmt.Println("Our Walk with hashing was faster!")
		}

		if stdlibDuration < walkNoHashDuration {
			fmt.Println("filepath.WalkDir was faster than our Walk without hashing!")
		} else {
			fmt.Println("Our Walk without hashing was faster!")
		}
	}
}

// createTestDirStructure creates a directory structure for testing
func createTestDirStructure(t testing.TB, root string, depth, width int) {
	if depth <= 0 {
		return
	}

	// Create files at this level
	for i := 0; i < width; i++ {
		file := filepath.Join(root, fmt.Sprintf("file%d.txt", i))
		err := os.WriteFile(file, []byte("test content"), 0644)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Create subdirectories and recurse
	for i := 0; i < width; i++ {
		dir := filepath.Join(root, fmt.Sprintf("dir%d", i))
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			t.Fatal(err)
		}
		createTestDirStructure(t, dir, depth-1, width)
	}
}

func TestComputeHash(t *testing.T) {
	// Create a temporary file for testing
	tempFile, err := os.CreateTemp("", "flashfs-hash-test-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write some content to the file
	content := []byte("test content for hash computation")
	if _, err := tempFile.Write(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tempFile.Close() // Close to ensure content is flushed

	// Compute hash
	hash := computeHash(tempFile.Name())
	if len(hash) == 0 {
		t.Error("Hash computation failed")
	}

	// Compute hash again to verify consistency
	hash2 := computeHash(tempFile.Name())
	if len(hash2) == 0 {
		t.Error("Second hash computation failed")
	}

	// Hashes should be equal for the same content
	if string(hash) != string(hash2) {
		t.Error("Hash inconsistency: hashes for the same content are different")
	}

	// Modify the file and check that the hash changes
	time.Sleep(1 * time.Second) // Ensure file modification time changes
	newContent := []byte("modified content for hash computation")
	if err := os.WriteFile(tempFile.Name(), newContent, 0644); err != nil {
		t.Fatalf("Failed to modify temp file: %v", err)
	}

	hash3 := computeHash(tempFile.Name())
	if len(hash3) == 0 {
		t.Error("Third hash computation failed")
	}

	// Hashes should be different for different content
	if string(hash) == string(hash3) {
		t.Error("Hash inconsistency: hashes for different content are the same")
	}
}

func TestWalkErrorHandling(t *testing.T) {
	// Test with a non-existent directory
	nonExistentPath := filepath.Join(os.TempDir(), "non-existent-dir-"+time.Now().Format("20060102150405"))
	_, err := Walk(nonExistentPath)
	if err == nil {
		t.Error("Expected error for non-existent directory, got nil")
	}

	// Test with a file instead of a directory
	tempFile, err := os.CreateTemp("", "flashfs-walk-error-test-*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	_, err = Walk(tempFile.Name())
	if err == nil {
		t.Error("Expected error when walking a file instead of a directory, got nil")
	}
}

func TestWalkWithContext(t *testing.T) {
	// Create a temporary directory structure for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-ctx-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test directory structure with many subdirectories to ensure the walk takes some time
	for i := 0; i < 10; i++ {
		dir := filepath.Join(tempDir, "dir"+string(rune('a'+i%26)))
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create directory %s: %v", dir, err)
		}

		// Create subdirectories
		for j := 0; j < 10; j++ {
			subdir := filepath.Join(dir, "subdir"+string(rune('a'+j%26)))
			if err := os.MkdirAll(subdir, 0755); err != nil {
				t.Fatalf("Failed to create directory %s: %v", subdir, err)
			}

			// Create files in subdirectories
			for k := 0; k < 10; k++ {
				file := filepath.Join(subdir, "file"+string(rune('a'+k%26))+".txt")
				content := []byte("test content " + string(rune('a'+i%26)) + string(rune('a'+j%26)) + string(rune('a'+k%26)))
				if err := os.WriteFile(file, content, 0644); err != nil {
					t.Fatalf("Failed to create file %s: %v", file, err)
				}
			}
		}
	}

	// Test with a canceled context
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context immediately
	cancel()

	// The walk should return quickly with a context canceled error
	_, err = WalkWithContext(ctx, tempDir)
	if err == nil {
		t.Error("Expected context canceled error, got nil")
	} else if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}

	// Skip the timeout test in short mode
	if !testing.Short() {
		// Test with a valid context but create a separate goroutine that will cancel it
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		// Use a channel to signal when the walk has started
		started := make(chan struct{})

		// Start the walk in a goroutine
		resultCh := make(chan error, 1)
		go func() {
			// Signal that we're starting
			close(started)
			_, err := WalkWithContext(ctx, tempDir)
			resultCh <- err
		}()

		// Wait for the walk to start
		<-started

		// Sleep a bit to let the walk get going
		time.Sleep(10 * time.Millisecond)

		// Cancel the context
		cancel()

		// Wait for the result
		select {
		case err := <-resultCh:
			if err == nil {
				t.Error("Expected context canceled error, got nil")
			} else if err != context.Canceled {
				t.Errorf("Expected context.Canceled error, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Timed out waiting for walk to be canceled")
		}
	}

	// Test with a valid context
	ctx = context.Background()
	entries, err := WalkWithContext(ctx, tempDir)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify we got some entries
	if len(entries) == 0 {
		t.Error("Expected entries, got none")
	}
}
