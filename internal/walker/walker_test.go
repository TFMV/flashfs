package walker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWalk(t *testing.T) {
	// Create a temporary directory structure for testing
	tempDir, err := os.MkdirTemp("", "flashfs-walker-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test directory structure
	testDirs := []string{
		filepath.Join(tempDir, "dir1"),
		filepath.Join(tempDir, "dir1", "subdir1"),
		filepath.Join(tempDir, "dir2"),
	}

	for _, dir := range testDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	// Create test files
	testFiles := map[string][]byte{
		filepath.Join(tempDir, "file1.txt"):                    []byte("file1 content"),
		filepath.Join(tempDir, "dir1", "file2.txt"):            []byte("file2 content"),
		filepath.Join(tempDir, "dir1", "subdir1", "file3.txt"): []byte("file3 content"),
		filepath.Join(tempDir, "dir2", "file4.txt"):            []byte("file4 content"),
	}

	for path, content := range testFiles {
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatalf("Failed to create file %s: %v", path, err)
		}
	}

	// Run the Walk function
	entries, err := Walk(tempDir)
	if err != nil {
		t.Fatalf("Walk failed: %v", err)
	}

	// Verify the results
	if len(entries) != len(testDirs)+len(testFiles) {
		t.Errorf("Expected %d entries, got %d", len(testDirs)+len(testFiles), len(entries))
	}

	// Create a map of expected entries (using relative paths)
	expectedEntries := make(map[string]bool)
	for _, dir := range testDirs {
		relPath, err := filepath.Rel(tempDir, dir)
		if err != nil {
			t.Fatalf("Failed to get relative path: %v", err)
		}
		expectedEntries[relPath] = true
	}

	for filePath := range testFiles {
		relPath, err := filepath.Rel(tempDir, filePath)
		if err != nil {
			t.Fatalf("Failed to get relative path: %v", err)
		}
		expectedEntries[relPath] = true
	}

	// Check that all expected entries are present
	for _, entry := range entries {
		// The walker returns paths relative to the root directory
		// Make sure we're comparing apples to apples
		entryPath := entry.Path

		// Skip the root directory itself if it's included
		if entryPath == "." || entryPath == "" {
			continue
		}

		// Normalize path separators for cross-platform compatibility
		entryPath = filepath.ToSlash(entryPath)

		if !expectedEntries[entryPath] {
			t.Errorf("Unexpected entry: %s", entryPath)
		} else {
			delete(expectedEntries, entryPath)
		}

		// Verify file metadata
		fullPath := filepath.Join(tempDir, entryPath)
		info, err := os.Stat(fullPath)
		if err != nil {
			t.Errorf("Failed to stat %s: %v", fullPath, err)
			continue
		}

		if entry.IsDir != info.IsDir() {
			t.Errorf("IsDir mismatch for %s: expected %v, got %v", entryPath, info.IsDir(), entry.IsDir)
		}

		if entry.Size != info.Size() {
			t.Errorf("Size mismatch for %s: expected %d, got %d", entryPath, info.Size(), entry.Size)
		}

		// ModTime should be close to the actual file's ModTime
		if !info.IsDir() && entry.ModTime == 0 {
			t.Errorf("ModTime not set for %s", entryPath)
		}

		// Check permissions
		if entry.Permissions != uint32(info.Mode().Perm()) {
			t.Errorf("Permissions mismatch for %s: expected %o, got %o",
				entryPath, info.Mode().Perm(), entry.Permissions)
		}

		// Check hash for files
		if !info.IsDir() {
			if len(entry.Hash) == 0 {
				t.Errorf("Hash not computed for %s", entryPath)
			}
		}
	}

	// Check that all expected entries were found
	if len(expectedEntries) > 0 {
		for path := range expectedEntries {
			t.Errorf("Missing entry: %s", path)
		}
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
