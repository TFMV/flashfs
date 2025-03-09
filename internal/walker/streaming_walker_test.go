package walker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// createTestFileStructure creates a simple test directory structure
func createTestFileStructure(t testing.TB, baseDir string) {
	// Create a test directory structure
	testDirs := []string{
		"dir1",
		"dir1/subdir1",
		"dir2",
	}

	testFiles := []string{
		"file1.txt",
		"dir1/file2.txt",
		"dir1/subdir1/file3.txt",
		"dir2/file4.txt",
	}

	// Create directories
	for _, dir := range testDirs {
		dirPath := filepath.Join(baseDir, dir)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			t.Fatal(err)
		}
	}

	// Create files
	for _, file := range testFiles {
		filePath := filepath.Join(baseDir, file)
		f, err := os.Create(filePath)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("test content")
		if err != nil {
			f.Close()
			t.Fatal(err)
		}
		f.Close()
	}
}

func TestWalkWithCallback(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a simple test directory structure
	createTestFileStructure(t, tempDir)

	// Test WalkWithCallback with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var entries []SnapshotEntry
	err = WalkWithCallback(ctx, tempDir, DefaultWalkOptions(), func(entry SnapshotEntry) error {
		entries = append(entries, entry)
		return nil
	})

	if err != nil {
		t.Fatalf("WalkWithCallback returned error: %v", err)
	}

	// Check that we got all entries (4 files + 3 dirs + root = 8)
	expectedCount := 8
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Check that we got all files
	fileCount := 0
	for _, entry := range entries {
		if !entry.IsDir {
			fileCount++
		}
	}
	if fileCount != 4 {
		t.Errorf("Expected 4 files, got %d", fileCount)
	}
}

func TestWalkStreamWithCallback(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a simple test directory structure
	createTestFileStructure(t, tempDir)

	// Test WalkStreamWithCallback with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var entries []SnapshotEntry
	err = WalkStreamWithCallback(ctx, tempDir, DefaultWalkOptions(), func(entry SnapshotEntry) error {
		entries = append(entries, entry)
		return nil
	})

	if err != nil {
		t.Fatalf("WalkStreamWithCallback returned error: %v", err)
	}

	// Check that we got all entries (4 files + 3 dirs + root = 8)
	expectedCount := 8
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Check that we got all files
	fileCount := 0
	for _, entry := range entries {
		if !entry.IsDir {
			fileCount++
		}
	}
	if fileCount != 4 {
		t.Errorf("Expected 4 files, got %d", fileCount)
	}
}

func TestWalkStream(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a simple test directory structure
	createTestFileStructure(t, tempDir)

	// Test WalkStream with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	entryChan, errChan := WalkStream(ctx, tempDir)

	var entries []SnapshotEntry
	for entry := range entryChan {
		entries = append(entries, entry)
	}

	// Check for errors
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("WalkStream returned error: %v", err)
		}
	default:
		// No errors is fine
	}

	// Check that we got all entries (4 files + 3 dirs + root = 8)
	expectedCount := 8
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Check that we got all files
	fileCount := 0
	for _, entry := range entries {
		if !entry.IsDir {
			fileCount++
		}
	}
	if fileCount != 4 {
		t.Errorf("Expected 4 files, got %d", fileCount)
	}
}

func TestWalkWithCallbackCancellation(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a simple test directory structure
	createTestFileStructure(t, tempDir)

	// Test WalkWithCallback with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	err = WalkWithCallback(ctx, tempDir, DefaultWalkOptions(), func(entry SnapshotEntry) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error due to cancellation, got nil")
	}
}

func TestWalkStreamWithCallbackCancellation(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a simple test directory structure
	createTestFileStructure(t, tempDir)

	// Test WalkStreamWithCallback with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	err = WalkStreamWithCallback(ctx, tempDir, DefaultWalkOptions(), func(entry SnapshotEntry) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error due to cancellation, got nil")
	}
}

func BenchmarkWalkWithCallback(b *testing.B) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test directory structure with 100 files
	for i := 0; i < 100; i++ {
		filePath := filepath.Join(tempDir, fmt.Sprintf("file%d.txt", i))
		f, err := os.Create(filePath)
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.WriteString("test content")
		if err != nil {
			f.Close()
			b.Fatal(err)
		}
		f.Close()
	}

	// Create 10 subdirectories with 10 files each
	for i := 0; i < 10; i++ {
		dirPath := filepath.Join(tempDir, fmt.Sprintf("dir%d", i))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 10; j++ {
			filePath := filepath.Join(dirPath, fmt.Sprintf("file%d.txt", j))
			f, err := os.Create(filePath)
			if err != nil {
				b.Fatal(err)
			}
			_, err = f.WriteString("test content")
			if err != nil {
				f.Close()
				b.Fatal(err)
			}
			f.Close()
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var count int
		err := WalkWithCallback(context.Background(), tempDir, DefaultWalkOptions(), func(entry SnapshotEntry) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatalf("WalkWithCallback returned error: %v", err)
		}
	}
}

func BenchmarkWalkStreamWithCallback(b *testing.B) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test directory structure with 100 files
	for i := 0; i < 100; i++ {
		filePath := filepath.Join(tempDir, fmt.Sprintf("file%d.txt", i))
		f, err := os.Create(filePath)
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.WriteString("test content")
		if err != nil {
			f.Close()
			b.Fatal(err)
		}
		f.Close()
	}

	// Create 10 subdirectories with 10 files each
	for i := 0; i < 10; i++ {
		dirPath := filepath.Join(tempDir, fmt.Sprintf("dir%d", i))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 10; j++ {
			filePath := filepath.Join(dirPath, fmt.Sprintf("file%d.txt", j))
			f, err := os.Create(filePath)
			if err != nil {
				b.Fatal(err)
			}
			_, err = f.WriteString("test content")
			if err != nil {
				f.Close()
				b.Fatal(err)
			}
			f.Close()
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var count int
		err := WalkStreamWithCallback(context.Background(), tempDir, DefaultWalkOptions(), func(entry SnapshotEntry) error {
			count++
			return nil
		})
		if err != nil {
			b.Fatalf("WalkStreamWithCallback returned error: %v", err)
		}
	}
}

// BenchmarkWalk runs benchmarks for all walker implementations
func BenchmarkWalk(b *testing.B) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-bench-all")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test directory structure with 100 files
	for i := 0; i < 100; i++ {
		filePath := filepath.Join(tempDir, fmt.Sprintf("file%d.txt", i))
		f, err := os.Create(filePath)
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.WriteString("test content")
		if err != nil {
			f.Close()
			b.Fatal(err)
		}
		f.Close()
	}

	// Create 10 subdirectories with 10 files each
	for i := 0; i < 10; i++ {
		dirPath := filepath.Join(tempDir, fmt.Sprintf("dir%d", i))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 10; j++ {
			filePath := filepath.Join(dirPath, fmt.Sprintf("file%d.txt", j))
			f, err := os.Create(filePath)
			if err != nil {
				b.Fatal(err)
			}
			_, err = f.WriteString("test content")
			if err != nil {
				f.Close()
				b.Fatal(err)
			}
			f.Close()
		}
	}

	// Count total entries for reporting
	var totalEntries int
	err = filepath.WalkDir(tempDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		totalEntries++
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	// Run sub-benchmarks for each implementation
	b.Run("StandardWalkDir", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := filepath.WalkDir(tempDir, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				count++
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(count), "entries")
		}
	})

	b.Run("Walk", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			entries, err := Walk(tempDir)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(entries)), "entries")
		}
	})

	b.Run("WalkWithCallback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := WalkWithCallback(context.Background(), tempDir, DefaultWalkOptions(), func(entry SnapshotEntry) error {
				count++
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(count), "entries")
		}
	})

	b.Run("WalkStream", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			entryChan, errChan := WalkStream(context.Background(), tempDir)

			var count int
			for range entryChan {
				count++
			}

			// Check for errors
			err := <-errChan
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(count), "entries")
		}
	})

	b.Run("WalkStreamWithCallback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := WalkStreamWithCallback(context.Background(), tempDir, DefaultWalkOptions(), func(entry SnapshotEntry) error {
				count++
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(count), "entries")
		}
	})
}

// BenchmarkWalkNoHash runs benchmarks for all walker implementations without computing hashes
func BenchmarkWalkNoHash(b *testing.B) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-bench-nohash")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test directory structure with 100 files
	for i := 0; i < 100; i++ {
		filePath := filepath.Join(tempDir, fmt.Sprintf("file%d.txt", i))
		f, err := os.Create(filePath)
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.WriteString("test content")
		if err != nil {
			f.Close()
			b.Fatal(err)
		}
		f.Close()
	}

	// Create 10 subdirectories with 10 files each
	for i := 0; i < 10; i++ {
		dirPath := filepath.Join(tempDir, fmt.Sprintf("dir%d", i))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 10; j++ {
			filePath := filepath.Join(dirPath, fmt.Sprintf("file%d.txt", j))
			f, err := os.Create(filePath)
			if err != nil {
				b.Fatal(err)
			}
			_, err = f.WriteString("test content")
			if err != nil {
				f.Close()
				b.Fatal(err)
			}
			f.Close()
		}
	}

	// Count total entries for reporting
	var totalEntries int
	err = filepath.WalkDir(tempDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		totalEntries++
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}

	// Create options without hashing
	noHashOptions := DefaultWalkOptions()
	noHashOptions.ComputeHashes = false

	// Run sub-benchmarks for each implementation
	b.Run("Walk", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			entries, err := WalkWithOptions(context.Background(), tempDir, noHashOptions)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(entries)), "entries")
		}
	})

	b.Run("WalkWithCallback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := WalkWithCallback(context.Background(), tempDir, noHashOptions, func(entry SnapshotEntry) error {
				count++
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(count), "entries")
		}
	})

	b.Run("WalkStream", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			entryChan, errChan := WalkStreamWithOptions(context.Background(), tempDir, noHashOptions)

			var count int
			for range entryChan {
				count++
			}

			// Check for errors
			err := <-errChan
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(count), "entries")
		}
	})

	b.Run("WalkStreamWithCallback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var count int
			err := WalkStreamWithCallback(context.Background(), tempDir, noHashOptions, func(entry SnapshotEntry) error {
				count++
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(count), "entries")
		}
	})
}
