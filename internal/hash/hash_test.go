package hash

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestFile(t *testing.T) {
	// Setup: create a temporary test file.
	tmpfile, err := os.CreateTemp("", "hash_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // Clean up after the test.
	defer tmpfile.Close()

	testData := []byte("This is some test data.")
	if _, err := tmpfile.Write(testData); err != nil {
		t.Fatal(err)
	}
	tmpfile.Seek(0, 0) // Reset file pointer

	// Expected Hashes
	expectedBlake3 := "477487010f611fc4cef99d0ca765636c70d84f743fb059dc5683458ad9603d54"
	expectedMD5 := "0dc937dea57723cd698a8518440b5187"
	expectedSha1 := "57d318c98a28ff1268b54cf4af65d94333f46fa9"
	expectedSha256 := "edd4216bf975061546257417d9bcf5f25e82bdb12f7abfd6bc88f88bc4c7022c"

	tests := []struct {
		name      string
		path      string
		opts      Options
		want      Result
		fileError bool
	}{
		{
			name: "Valid file, BLAKE3, default options",
			path: tmpfile.Name(),
			opts: DefaultOptions(),
			want: Result{Hash: expectedBlake3, Algorithm: BLAKE3, Size: int64(len(testData))},
		},
		{
			name: "Valid file, MD5",
			path: tmpfile.Name(),
			opts: Options{Algorithm: MD5},
			want: Result{Hash: expectedMD5, Algorithm: MD5, Size: int64(len(testData))},
		},
		{
			name: "Valid file, SHA1",
			path: tmpfile.Name(),
			opts: Options{Algorithm: SHA1},
			want: Result{Hash: expectedSha1, Algorithm: SHA1, Size: int64(len(testData))},
		},
		{
			name: "Valid file, SHA256",
			path: tmpfile.Name(),
			opts: Options{Algorithm: SHA256},
			want: Result{Hash: expectedSha256, Algorithm: SHA256, Size: int64(len(testData))},
		},
		{
			name:      "Non-existent file, error returned",
			path:      "nonexistentfile.txt",
			opts:      DefaultOptions(),
			want:      Result{Algorithm: BLAKE3}, // Expect an error.
			fileError: true,
		},
		{
			name:      "Non-existent file, skip errors",
			path:      "nonexistentfile.txt",
			opts:      Options{SkipErrors: true, Algorithm: BLAKE3},
			fileError: true,                      // Expect an error in the result, even with SkipErrors.
			want:      Result{Algorithm: BLAKE3}, // Expect an error.
		},
		{
			name: "Invalid algorithm",
			path: tmpfile.Name(),
			opts: Options{Algorithm: UndefinedAlgorithm},
			want: Result{
				Error:     errors.New("undefined hashing algorithm"),
				Algorithm: UndefinedAlgorithm,
			},
			fileError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := File(tt.path, tt.opts)
			if tt.fileError {
				if got.Error == nil {
					t.Errorf("File() = %v, want error", got)
					return
				}
			} else if got.Error != nil {
				t.Errorf("File() error = %v, wantErr %v", got.Error, tt.want.Error)
				return
			}
			if got.Error == nil && tt.want.Error != nil {
				t.Errorf("Expected error %v", tt.want.Error)
				return
			}
			if got.Error != nil && tt.want.Error != nil {
				if got.Error.Error() != tt.want.Error.Error() {
					t.Errorf("Expected error %v, got %v", tt.want.Error, got.Error)
					return
				}
			}
			// We compare the hash, size, and algorithms and file contents separately.
			if got.Hash != tt.want.Hash && !tt.fileError {
				t.Errorf("File() = %v, want %v", got.Hash, tt.want.Hash)
			}
			if got.Algorithm != tt.want.Algorithm && !tt.fileError {
				t.Errorf("File() = %v, want %v", got.Algorithm, tt.want.Algorithm)
			}
			if got.Size != tt.want.Size && !tt.fileError {
				t.Errorf("File() = %v, want %v", got.Size, tt.want.Size)
			}
		})
	}
}

func TestBytes(t *testing.T) {
	testData := []byte("This is a test string.")
	expectedBlake3 := "bb029e052c87d2e6353f1dceb1f3511744e9a1cf643b240091ede3787e985a57"
	expectedMD5 := "1620d7b066531f9dbad51eee623f7635"
	expectedSHA1 := "3532499280b4e2f32f6417e556901a526d69143c"
	expectedSHA256 := "3eec256a587cccf72f71d2342b6dfab0bbca01697c7e7014540bdd62b72120da"

	tests := []struct {
		name      string
		data      []byte
		algorithm Algorithm
		want      Result
	}{
		{
			name:      "Valid data, BLAKE3",
			data:      testData,
			algorithm: BLAKE3,
			want:      Result{Hash: expectedBlake3, Algorithm: BLAKE3, Size: int64(len(testData))},
		},
		{
			name:      "Valid data, MD5",
			data:      testData,
			algorithm: MD5,
			want:      Result{Hash: expectedMD5, Algorithm: MD5, Size: int64(len(testData))},
		},
		{
			name:      "Valid data, SHA1",
			data:      testData,
			algorithm: SHA1,
			want:      Result{Hash: expectedSHA1, Algorithm: SHA1, Size: int64(len(testData))},
		},
		{
			name:      "Valid data, SHA256",
			data:      testData,
			algorithm: SHA256,
			want:      Result{Hash: expectedSHA256, Algorithm: SHA256, Size: int64(len(testData))},
		},
		{
			name:      "Empty data",
			data:      []byte{},
			algorithm: BLAKE3,
			want:      Result{Hash: "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", Algorithm: BLAKE3, Size: 0},
		},
		{
			name:      "Invalid Algorithm",
			data:      testData,
			algorithm: UndefinedAlgorithm,
			want:      Result{Algorithm: UndefinedAlgorithm, Error: errors.New("undefined hashing algorithm")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Bytes(tt.data, tt.algorithm)
			if got.Error != nil && tt.want.Error != nil {
				if got.Error.Error() != tt.want.Error.Error() {
					t.Errorf("Got %v, Wanted: %v", got.Error, tt.want.Error)
				}
			}
			if got.Error == nil && tt.want.Error != nil {
				t.Errorf("Expected error %v", tt.want.Error)
				return
			}
			if got.Hash != tt.want.Hash {
				t.Errorf("Bytes() = %v, want %v", got.Hash, tt.want.Hash)
			}
			if got.Algorithm != tt.want.Algorithm {
				t.Errorf("Bytes() = %v, want %v", got.Algorithm, tt.want.Algorithm)
			}
			if got.Size != tt.want.Size {
				t.Errorf("Bytes() = %v, want %v", got.Size, tt.want.Size)
			}
		})
	}
}

func TestString(t *testing.T) {
	testData := "This is a test string."
	expectedBlake3 := "bb029e052c87d2e6353f1dceb1f3511744e9a1cf643b240091ede3787e985a57"

	tests := []struct {
		name      string
		data      string
		algorithm Algorithm
		want      Result
	}{
		{
			name:      "Valid string, BLAKE3",
			data:      testData,
			algorithm: BLAKE3,
			want:      Result{Hash: expectedBlake3, Algorithm: BLAKE3, Size: int64(len(testData))},
		},
		{
			name:      "Invalid Algorithm",
			data:      testData,
			algorithm: UndefinedAlgorithm,
			want:      Result{Algorithm: UndefinedAlgorithm, Error: errors.New("undefined hashing algorithm")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := String(tt.data, tt.algorithm)
			if got.Error != nil && tt.want.Error != nil {
				if got.Error.Error() != tt.want.Error.Error() {
					t.Errorf("Got %v, Wanted: %v", got.Error, tt.want.Error)
				}
			}
			if got.Error == nil && tt.want.Error != nil {
				t.Errorf("Expected error %v", tt.want.Error)
				return
			}
			if got.Hash != tt.want.Hash {
				t.Errorf("String() = %v, want %v", got.Hash, tt.want.Hash)
			}
			if got.Algorithm != tt.want.Algorithm {
				t.Errorf("String() = %v, want %v", got.Algorithm, tt.want.Algorithm)
			}
			if got.Size != tt.want.Size {
				t.Errorf("String() = %v, want %v", got.Size, tt.want.Size)
			}
		})
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{
			name: "equal",
			a:    "abc123",
			b:    "abc123",
			want: true,
		},
		{
			name: "not equal",
			a:    "abc123",
			b:    "def456",
			want: false,
		},
		{
			name: "empty strings",
			a:    "",
			b:    "",
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Equal(tt.a, tt.b); got != tt.want {
				t.Errorf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilesConcurrent(t *testing.T) {
	// Create temporary files for testing.
	numFiles := 10
	paths := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		tmpfile, err := os.CreateTemp("", fmt.Sprintf("hash_test_concurrent_%d", i))
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())
		defer tmpfile.Close()

		// Write random data to each file.
		data := make([]byte, 1024)
		for j := range data {
			data[j] = byte(i + j)
		}
		if _, err := tmpfile.Write(data); err != nil {
			t.Fatal(err)
		}
		paths[i] = tmpfile.Name()
	}

	tests := []struct {
		name  string
		paths []string
		opts  Options
	}{
		{
			name:  "Valid files, default options",
			paths: paths,
			opts:  DefaultOptions(),
		},
		{
			name:  "Valid files, custom concurrency",
			paths: paths,
			opts:  Options{Algorithm: BLAKE3, Concurrency: 2}, // Test with lower concurrency.
		},
		{
			name:  "Valid files, custom concurrency",
			paths: paths,
			opts:  Options{Algorithm: BLAKE3, Concurrency: runtime.NumCPU() * 2}, // Test with higher concurrency.
		},
		{
			name:  "Valid files, custom concurrency",
			paths: paths,
			opts:  Options{Algorithm: BLAKE3}, // Use BLAKE3 for all tests
		},
		{
			name:  "Valid files, and an invalid file, skipping errors",
			paths: append(paths, "invalid-file.txt"),
			opts:  Options{Algorithm: BLAKE3, SkipErrors: true},
		},
		{
			name:  "Invalid Algorithm",
			paths: paths,
			opts:  Options{Algorithm: UndefinedAlgorithm},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Generate expected results using the same options
			expectedResults := make(map[string]Result)

			// Skip generating expected results for invalid algorithm
			if tt.opts.Algorithm != UndefinedAlgorithm {
				for _, path := range tt.paths {
					if path != "invalid-file.txt" {
						result := File(path, tt.opts)
						if result.Error == nil {
							expectedResults[path] = result
						}
					}
				}
			}

			got := FilesConcurrent(tt.paths, tt.opts)

			// For invalid algorithm, check that all results have errors
			if tt.opts.Algorithm == UndefinedAlgorithm {
				for path, result := range got {
					if result.Error == nil || result.Error.Error() != "undefined hashing algorithm" {
						t.Errorf("Expected 'undefined hashing algorithm' error for %s, got %v", path, result.Error)
					}
				}
				return
			}

			// Check that we have the expected number of results.
			if len(got) != len(expectedResults) && !tt.opts.SkipErrors {
				t.Errorf("FilesConcurrent() returned %d results, want %d", len(got), len(expectedResults))
			}

			// Check each result.
			for path, want := range expectedResults {
				if got, ok := got[path]; ok {
					if got.Error != nil && want.Error == nil {
						t.Errorf("FilesConcurrent() returned error for %s: %v", path, got.Error)
					} else if got.Error == nil && want.Error != nil {
						t.Errorf("FilesConcurrent() did not return expected error for %s", path)
					} else if got.Error == nil && want.Error == nil {
						// Compare hash, algorithm, and size.
						if got.Hash != want.Hash {
							t.Errorf("Hash mismatch for %s: got %s, want %s", path, got.Hash, want.Hash)
						}
						if got.Algorithm != want.Algorithm {
							t.Errorf("Algorithm mismatch for %s: got %s, want %s", path, got.Algorithm, want.Algorithm)
						}
						if got.Size != want.Size {
							t.Errorf("Size mismatch for %s: got %d, want %d", path, got.Size, want.Size)
						}
					}
				} else {
					t.Errorf("FilesConcurrent() did not return result for %s", path)
				}
			}

			// Check for unexpected results.
			for path := range got {
				if _, ok := expectedResults[path]; !ok && path != "invalid-file.txt" {
					t.Errorf("FilesConcurrent() returned unexpected result for %s", path)
				}
			}
		})
	}
}

func TestReader(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		algorithm Algorithm
		want      Result
		wantErr   bool
	}{
		{
			name:      "valid data",
			data:      []byte("hello world"),
			algorithm: BLAKE3,
			want: Result{
				Hash:      "d74981efa70a0c880b8d8c1985d075dbcbf679b99a5f9914e5aaf96b831a9e24",
				Algorithm: BLAKE3,
				Size:      11,
			},
			wantErr: false,
		},
		{
			name:      "valid data, MD5",
			data:      []byte("hello world"),
			algorithm: MD5,
			want: Result{
				Hash:      "5eb63bbbe01eeed093cb22bb8f5acdc3",
				Algorithm: MD5,
				Size:      11,
			},
			wantErr: false,
		},
		{
			name:      "valid data, SHA1",
			data:      []byte("hello world"),
			algorithm: SHA1,
			want: Result{
				Hash:      "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
				Algorithm: SHA1,
				Size:      11,
			},
			wantErr: false,
		},
		{
			name:      "valid data, SHA256",
			data:      []byte("hello world"),
			algorithm: SHA256,
			want: Result{
				Hash:      "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
				Algorithm: SHA256,
				Size:      11,
			},
			wantErr: false,
		},
		{
			name:      "empty data",
			data:      []byte{},
			algorithm: BLAKE3,
			want: Result{
				Hash:      "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", // Empty BLAKE3 hash
				Algorithm: BLAKE3,
				Size:      0,
			},
			wantErr: false,
		},
		{
			name:      "Invalid Algorithm",
			data:      []byte("hello world"),
			algorithm: UndefinedAlgorithm,
			want:      Result{Algorithm: UndefinedAlgorithm, Error: errors.New("undefined hashing algorithm")},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bytes.NewReader(tt.data)
			got := Reader(reader, tt.algorithm)

			if tt.wantErr {
				if got.Error == nil {
					t.Fatalf("expected error but got none")
				}
				return // Skip further checks if we expect an error
			}

			if got.Error != nil {
				t.Fatalf("unexpected error: %v", got.Error)
			}

			if got.Hash != tt.want.Hash {
				t.Errorf("Hash mismatch: got %v, want %v", got.Hash, tt.want.Hash)
			}

			if got.Algorithm != tt.want.Algorithm {
				t.Errorf("Algorithm mismatch: got %v, want %v", got.Algorithm, tt.want.Algorithm)
			}
			if got.Size != tt.want.Size {
				t.Errorf("Size mismatch: got %v, want %v", got.Size, tt.want.Size)
			}
		})
	}
}

func TestPartialFile(t *testing.T) {
	// Create a temporary large file
	tmpfile, err := os.CreateTemp("", "hash_test_large")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Write 20MB of data to the file (larger than the default threshold)
	// We'll use a pattern that's different at the beginning, middle, and end
	// to ensure our partial hashing captures the differences
	fileSize := int64(20 * 1024 * 1024) // 20MB

	// Write beginning (5MB of 'A')
	beginData := bytes.Repeat([]byte("A"), 5*1024*1024)
	if _, err := tmpfile.Write(beginData); err != nil {
		t.Fatal(err)
	}

	// Write middle (10MB of 'B')
	middleData := bytes.Repeat([]byte("B"), 10*1024*1024)
	if _, err := tmpfile.Write(middleData); err != nil {
		t.Fatal(err)
	}

	// Write end (5MB of 'C')
	endData := bytes.Repeat([]byte("C"), 5*1024*1024)
	if _, err := tmpfile.Write(endData); err != nil {
		t.Fatal(err)
	}

	// Flush to ensure all data is written
	if err := tmpfile.Sync(); err != nil {
		t.Fatal(err)
	}

	// Reset file pointer
	if _, err := tmpfile.Seek(0, 0); err != nil {
		t.Fatal(err)
	}

	// Test cases
	tests := []struct {
		name      string
		path      string
		opts      Options
		wantError bool
	}{
		{
			name: "Valid large file, BLAKE3",
			path: tmpfile.Name(),
			opts: Options{Algorithm: BLAKE3},
		},
		{
			name: "Valid large file, MD5",
			path: tmpfile.Name(),
			opts: Options{Algorithm: MD5},
		},
		{
			name: "Valid large file, SHA1",
			path: tmpfile.Name(),
			opts: Options{Algorithm: SHA1},
		},
		{
			name: "Valid large file, SHA256",
			path: tmpfile.Name(),
			opts: Options{Algorithm: SHA256},
		},
		{
			name:      "Invalid algorithm",
			path:      tmpfile.Name(),
			opts:      Options{Algorithm: UndefinedAlgorithm},
			wantError: true,
		},
		{
			name:      "Non-existent file",
			path:      "non-existent-file.txt",
			opts:      Options{Algorithm: BLAKE3},
			wantError: true,
		},
		{
			name:      "Non-existent file, skip errors",
			path:      "non-existent-file.txt",
			opts:      Options{Algorithm: BLAKE3, SkipErrors: true},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Get result from PartialFile
			got := PartialFile(tt.path, tt.opts)

			// Check error condition
			if tt.wantError {
				if got.Error == nil {
					t.Errorf("PartialFile() error = nil, wantError = true")
				}
				return
			}

			if got.Error != nil {
				t.Errorf("PartialFile() error = %v, wantError = false", got.Error)
				return
			}

			// Verify the hash is not empty
			if got.Hash == "" {
				t.Error("PartialFile() returned empty hash")
			}

			// Verify the algorithm matches
			if got.Algorithm != tt.opts.Algorithm {
				t.Errorf("PartialFile() algorithm = %v, want %v", got.Algorithm, tt.opts.Algorithm)
			}

			// Verify the size matches the file size
			if got.Size != fileSize {
				t.Errorf("PartialFile() size = %v, want %v", got.Size, fileSize)
			}

			// Verify consistency - hashing the same file twice should yield the same result
			got2 := PartialFile(tt.path, tt.opts)
			if got2.Error != nil {
				t.Errorf("Second PartialFile() call error = %v", got2.Error)
				return
			}

			if got.Hash != got2.Hash {
				t.Errorf("PartialFile() inconsistent hashes: %v != %v", got.Hash, got2.Hash)
			}

			// Verify that modifying the file changes the hash
			if tt.path != "non-existent-file.txt" {
				// Modify the beginning of the file (which is definitely sampled)
				if _, err := tmpfile.Seek(0, 0); err != nil {
					t.Fatal(err)
				}

				if _, err := tmpfile.Write([]byte("MODIFIED_BEGIN")); err != nil {
					t.Fatal(err)
				}

				if err := tmpfile.Sync(); err != nil {
					t.Fatal(err)
				}

				// Get hash of modified file
				got3 := PartialFile(tt.path, tt.opts)
				if got3.Error != nil {
					t.Errorf("Third PartialFile() call error = %v", got3.Error)
					return
				}

				// Hash should be different
				if got.Hash == got3.Hash {
					t.Errorf("PartialFile() failed to detect beginning modification: %v == %v", got.Hash, got3.Hash)
				}

				// Reset the file for the next test
				if _, err := tmpfile.Seek(0, 0); err != nil {
					t.Fatal(err)
				}

				// Write back the original data
				beginModifiedSection := bytes.Repeat([]byte("A"), len("MODIFIED_BEGIN"))
				if _, err := tmpfile.Write(beginModifiedSection); err != nil {
					t.Fatal(err)
				}

				if err := tmpfile.Sync(); err != nil {
					t.Fatal(err)
				}

				// Modify the middle of the file (at exactly the middle point)
				middlePoint := fileSize / 2
				if _, err := tmpfile.Seek(middlePoint, 0); err != nil {
					t.Fatal(err)
				}

				if _, err := tmpfile.Write([]byte("MODIFIED_MIDDLE")); err != nil {
					t.Fatal(err)
				}

				if err := tmpfile.Sync(); err != nil {
					t.Fatal(err)
				}

				// Get hash of modified file
				got4 := PartialFile(tt.path, tt.opts)
				if got4.Error != nil {
					t.Errorf("Fourth PartialFile() call error = %v", got4.Error)
					return
				}

				// Hash should be different
				if got.Hash == got4.Hash {
					t.Errorf("PartialFile() failed to detect middle modification: %v == %v", got.Hash, got4.Hash)
				}

				// Reset the file for the next test
				if _, err := tmpfile.Seek(middlePoint, 0); err != nil {
					t.Fatal(err)
				}

				// Write back the original data
				middleModifiedSection := bytes.Repeat([]byte("B"), len("MODIFIED_MIDDLE"))
				if _, err := tmpfile.Write(middleModifiedSection); err != nil {
					t.Fatal(err)
				}

				if err := tmpfile.Sync(); err != nil {
					t.Fatal(err)
				}

				// Modify the end of the file
				endPoint := fileSize - int64(len("MODIFIED_END"))
				if _, err := tmpfile.Seek(endPoint, 0); err != nil {
					t.Fatal(err)
				}

				if _, err := tmpfile.Write([]byte("MODIFIED_END")); err != nil {
					t.Fatal(err)
				}

				if err := tmpfile.Sync(); err != nil {
					t.Fatal(err)
				}

				// Get hash of modified file
				got5 := PartialFile(tt.path, tt.opts)
				if got5.Error != nil {
					t.Errorf("Fifth PartialFile() call error = %v", got5.Error)
					return
				}

				// Hash should be different
				if got.Hash == got5.Hash {
					t.Errorf("PartialFile() failed to detect end modification: %v == %v", got.Hash, got5.Hash)
				}

				// Reset the file for the next test
				if _, err := tmpfile.Seek(endPoint, 0); err != nil {
					t.Fatal(err)
				}

				// Write back the original data
				endModifiedSection := bytes.Repeat([]byte("C"), len("MODIFIED_END"))
				if _, err := tmpfile.Write(endModifiedSection); err != nil {
					t.Fatal(err)
				}

				if err := tmpfile.Sync(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}

	// Test that PartialFile is faster than File for large files
	t.Run("Performance comparison", func(t *testing.T) {
		opts := Options{Algorithm: BLAKE3}

		// Time the full file hash
		fullStart := time.Now()
		fullResult := File(tmpfile.Name(), opts)
		fullDuration := time.Since(fullStart)

		if fullResult.Error != nil {
			t.Errorf("File() error = %v", fullResult.Error)
			return
		}

		// Time the partial file hash
		partialStart := time.Now()
		partialResult := PartialFile(tmpfile.Name(), opts)
		partialDuration := time.Since(partialStart)

		if partialResult.Error != nil {
			t.Errorf("PartialFile() error = %v", partialResult.Error)
			return
		}

		// Partial should be faster
		if partialDuration >= fullDuration {
			t.Logf("PartialFile() took %v, File() took %v", partialDuration, fullDuration)
			t.Error("PartialFile() was not faster than File()")
		} else {
			speedup := float64(fullDuration) / float64(partialDuration)
			t.Logf("PartialFile() was %.2fx faster than File() (%v vs %v)", speedup, partialDuration, fullDuration)
		}

		// Hashes should be different since we're only sampling parts of the file
		if fullResult.Hash == partialResult.Hash {
			// Force the file to be large enough that we're definitely using partial hashing
			// This is to ensure the test is valid
			if fileSize <= DefaultOptions().PartialHashingThreshold {
				t.Logf("File size (%d) is not larger than threshold (%d), partial hashing may not be used",
					fileSize, DefaultOptions().PartialHashingThreshold)
			} else {
				t.Error("PartialFile() and File() produced the same hash, which is unexpected for a large file")
			}
		}
	})
}

func BenchmarkLargeFileHashing(b *testing.B) {
	// Create a temporary large file
	tmpfile, err := os.CreateTemp("", "hash_benchmark_large")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Write 100MB of data to the file
	fileSize := int64(100 * 1024 * 1024) // 100MB

	// Generate random data
	data := make([]byte, 1024*1024) // 1MB chunks
	for i := 0; i < 100; i++ {
		// Fill with random data
		for j := range data {
			data[j] = byte(rand.Intn(256))
		}

		if _, err := tmpfile.Write(data); err != nil {
			b.Fatal(err)
		}
	}

	if err := tmpfile.Sync(); err != nil {
		b.Fatal(err)
	}

	// Reset file pointer
	if _, err := tmpfile.Seek(0, 0); err != nil {
		b.Fatal(err)
	}

	// Benchmark full file hashing
	b.Run("FullFile", func(b *testing.B) {
		opts := DefaultOptions()
		opts.UsePartialHashing = false

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := File(tmpfile.Name(), opts)
			if result.Error != nil {
				b.Fatal(result.Error)
			}
			// Use the hash to prevent compiler optimizations
			b.SetBytes(fileSize)
			runtime.KeepAlive(result.Hash)
		}
	})

	// Benchmark partial file hashing
	b.Run("PartialFile", func(b *testing.B) {
		opts := DefaultOptions()
		opts.UsePartialHashing = true

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := File(tmpfile.Name(), opts)
			if result.Error != nil {
				b.Fatal(result.Error)
			}
			// Use the hash to prevent compiler optimizations
			b.SetBytes(fileSize)
			runtime.KeepAlive(result.Hash)
		}
	})

	// Benchmark different algorithms with partial hashing
	algorithms := []struct {
		name string
		alg  Algorithm
	}{
		{"BLAKE3", BLAKE3},
		{"MD5", MD5},
		{"SHA1", SHA1},
		{"SHA256", SHA256},
	}

	for _, alg := range algorithms {
		b.Run(fmt.Sprintf("PartialFile_%s", alg.name), func(b *testing.B) {
			opts := DefaultOptions()
			opts.UsePartialHashing = true
			opts.Algorithm = alg.alg

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result := File(tmpfile.Name(), opts)
				if result.Error != nil {
					b.Fatal(result.Error)
				}
				// Use the hash to prevent compiler optimizations
				b.SetBytes(fileSize)
				runtime.KeepAlive(result.Hash)
			}
		})
	}
}
