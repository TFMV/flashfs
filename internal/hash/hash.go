package hash

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/zeebo/blake3"
)

// Algorithm represents a supported hash algorithm.  Using an অ্যালগরিদম টাইপ
// instead of a string improves type safety and prevents accidental misuse
// with invalid algorithm names.
type Algorithm int

const (
	// BLAKE3 is the default and recommended algorithm (fast and secure).
	BLAKE3 Algorithm = iota
	// MD5 is provided for compatibility but is NOT CRYPTOGRAPHICALLY SECURE.
	MD5
	// SHA1 is provided for compatibility but is NOT CRYPTOGRAPHICALLY SECURE.
	SHA1
	// SHA256 is a secure but slower algorithm.
	SHA256
	// UndefinedAlgorithm is used for error handling.
	UndefinedAlgorithm
)

// String provides the string representation of the algorithm. Using
// a Stringer interface implementation provides a consistent and user-friendly way to access the algorithm name.
func (a Algorithm) String() string {
	switch a {
	case BLAKE3:
		return "BLAKE3"
	case MD5:
		return "MD5"
	case SHA1:
		return "SHA1"
	case SHA256:
		return "SHA256"
	default:
		return "Undefined"
	}
}

// Options configures the hashing behavior.
type Options struct {
	// Algorithm to use for hashing.
	Algorithm Algorithm
	// BufferSize is the size of the buffer used for reading files.
	BufferSize int
	// SkipErrors determines whether to return an error or an empty hash on failure.
	SkipErrors bool
	// Concurrency determines the number of files to hash concurrently.  A value
	// of 0 or less uses the number of available CPUs.
	Concurrency int
	// UsePartialHashing enables partial hashing for large files.
	UsePartialHashing bool
	// PartialHashingThreshold is the file size threshold in bytes above which
	// partial hashing will be used (if enabled). Default is 10MB.
	PartialHashingThreshold int64
}

// DefaultOptions returns the default hashing options, providing a clear
// starting point for configuration.  This method uses reasonable,
// performance-oriented defaults.
func DefaultOptions() Options {
	return Options{
		Algorithm:               BLAKE3,
		BufferSize:              16 * 1024 * 1024, // 16MB buffer - increased for better pipe throughput.
		SkipErrors:              false,            // Fail fast by default.
		Concurrency:             0,                // Use all available CPUs.
		UsePartialHashing:       false,            // Disabled by default for backward compatibility.
		PartialHashingThreshold: 10 * 1024 * 1024, // 10MB threshold.
	}
}

// Result represents the result of a hashing operation.
type Result struct {
	// Hash is the hex-encoded hash string.
	Hash string
	// Error is any error that occurred during hashing.
	Error error
	// Algorithm is the algorithm used for hashing.
	Algorithm Algorithm
	// Size is the size of the hashed data in bytes.
	Size int64
}

// newHasher creates a new hasher for the specified algorithm, centralizing
// hasher creation and error handling. Returning a new hasher (instead of just a hash.hash interface) makes the returned value more type-safe.
func newHasher(algorithm Algorithm) (hash.Hash, error) {
	switch algorithm {
	case BLAKE3:
		return blake3.New(), nil
	case MD5:
		return md5.New(), nil
	case SHA1:
		return sha1.New(), nil
	case SHA256:
		return sha256.New(), nil
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %s", algorithm)
	}
}

// File computes the hash of a file using the specified algorithm. It's
// important to handle file opening and closing errors robustly.
func File(path string, opts Options) Result {
	// Validate Options Here
	if opts.Algorithm == UndefinedAlgorithm {
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("undefined hashing algorithm")}
	}

	// Check if the file exists and get its size
	info, err := os.Stat(path)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to stat file '%s': %w", path, err)}
	}

	// Use partial hashing for large files if enabled
	if opts.UsePartialHashing && info.Size() > opts.PartialHashingThreshold {
		return PartialFile(path, opts)
	}

	file, err := os.Open(path)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to open file '%s': %w", path, err)}
	}
	defer file.Close() // Ensure file is closed, even if errors occur later.

	hasher, err := newHasher(opts.Algorithm)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: err} // No need to wrap, already informative.
	}

	bufferSize := opts.BufferSize
	if bufferSize <= 0 {
		bufferSize = DefaultOptions().BufferSize // Use default if invalid.
	}
	buffer := make([]byte, bufferSize)

	_, err = io.CopyBuffer(hasher, file, buffer)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to read file '%s': %w", path, err)}
	}

	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)

	return Result{
		Hash:      hashString,
		Algorithm: opts.Algorithm,
		Size:      info.Size(),
	}
}

// Bytes computes the hash of a byte slice.
func Bytes(data []byte, algorithm Algorithm) Result {
	if algorithm == UndefinedAlgorithm {
		return Result{Algorithm: algorithm, Error: fmt.Errorf("undefined hashing algorithm")}
	}

	hasher, err := newHasher(algorithm)
	if err != nil {
		return Result{Algorithm: algorithm, Error: err} // No need to wrap.
	}

	// Using Write is safe for in-memory data.
	_, err = hasher.Write(data)
	if err != nil {
		return Result{Algorithm: algorithm, Error: fmt.Errorf("failed to hash data: %w", err)}
	}

	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)

	return Result{
		Hash:      hashString,
		Algorithm: algorithm,
		Size:      int64(len(data)),
	}
}

// String computes the hash of a string.
func String(data string, algorithm Algorithm) Result {
	return Bytes([]byte(data), algorithm)
}

// Equal compares two hashes for equality. Using constant-time comparison
// isn't necessary here, as the purpose isn't necessarily cryptographic
// security, but correct functionality.
func Equal(a, b string) bool {
	return a == b
}

// FilesConcurrent hashes multiple files concurrently, improving throughput.
func FilesConcurrent(paths []string, opts Options) map[string]Result {
	// Validate Options
	if opts.Algorithm == UndefinedAlgorithm {
		results := make(map[string]Result)
		for _, path := range paths {
			results[path] = Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("undefined hashing algorithm")}
		}
		return results
	}

	results := make(map[string]Result)
	resultsMu := sync.Mutex{} // Protects concurrent map access.

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = runtime.NumCPU() // Default to available CPUs.
	}

	sem := make(chan struct{}, concurrency) // Semaphore to limit goroutines.
	var wg sync.WaitGroup

	for _, path := range paths {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()

			sem <- struct{}{}        // Acquire a semaphore slot.
			defer func() { <-sem }() // Release the slot when done.

			result := File(p, opts) // Calculate the hash.

			resultsMu.Lock()    // Lock the map for writing.
			results[p] = result // Store the result.
			resultsMu.Unlock()  // Unlock the map.
		}(path)
	}

	wg.Wait() // Wait for all goroutines to complete.
	return results
}

// Reader computes the hash of an io.Reader. This allows hashing data from
// various sources (network connections, pipes, etc.).
func Reader(reader io.Reader, algorithm Algorithm) Result {
	// Validate Options
	if algorithm == UndefinedAlgorithm {
		return Result{Algorithm: algorithm, Error: fmt.Errorf("undefined hashing algorithm")}
	}

	hasher, err := newHasher(algorithm)
	if err != nil {
		return Result{Algorithm: algorithm, Error: err}
	}

	size, err := io.Copy(hasher, reader)
	if err != nil {
		return Result{Algorithm: algorithm, Error: fmt.Errorf("failed to hash data: %w", err)}
	}

	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)

	return Result{
		Hash:      hashString,
		Algorithm: algorithm,
		Size:      size,
	}
}

// Verify checks if a file matches the expected hash. This provides a
// convenient way to verify data integrity.
func Verify(path string, expectedHash string, opts Options) (bool, error) {
	result := File(path, opts)
	if result.Error != nil {
		return false, result.Error // Return the error from File directly.
	}
	return Equal(result.Hash, expectedHash), nil
}

// PartialFile computes a hash of a large file by sampling portions of it rather than
// reading the entire file. This is much faster for very large files but provides a
// different hash than hashing the entire file.
//
// The function samples:
// - The first N bytes
// - The middle N bytes
// - The last N bytes
//
// These samples are then combined to create a representative hash of the file.
// This is useful for quick change detection in very large files where full hashing
// would be prohibitively expensive.
func PartialFile(path string, opts Options) Result {
	// Validate algorithm
	if opts.Algorithm == UndefinedAlgorithm {
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("undefined hashing algorithm")}
	}

	// Open the file
	file, err := os.Open(path)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to open file '%s': %w", path, err)}
	}
	defer file.Close()

	// Get file size
	info, err := file.Stat()
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to stat file '%s': %w", path, err)}
	}
	size := info.Size()

	// If file is smaller than the threshold, just hash the whole thing
	threshold := opts.PartialHashingThreshold
	if threshold <= 0 {
		threshold = DefaultOptions().PartialHashingThreshold
	}

	if size < threshold {
		// Use the regular File function but make sure we don't recurse
		noPartialOpts := opts
		noPartialOpts.UsePartialHashing = false
		return File(path, noPartialOpts)
	}

	// Create hasher
	hasher, err := newHasher(opts.Algorithm)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: err}
	}

	// Determine sample size (1MB or 1/10th of file size, whichever is smaller)
	sampleSize := int64(1024 * 1024) // 1MB default
	if size/10 < sampleSize {
		sampleSize = size / 10
	}

	// Allocate buffer
	buffer := make([]byte, sampleSize)

	// Hash the beginning of the file
	_, err = file.Seek(0, 0)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to seek in file '%s': %w", path, err)}
	}

	n, err := io.ReadFull(file, buffer)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to read beginning of file '%s': %w", path, err)}
	}

	// Write file size and position to hasher to ensure uniqueness
	fmt.Fprintf(hasher, "size:%d;pos:0;", size)
	hasher.Write(buffer[:n])

	// Hash the middle of the file
	middleOffset := (size / 2) - (sampleSize / 2)
	_, err = file.Seek(middleOffset, 0)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to seek to middle of file '%s': %w", path, err)}
	}

	n, err = io.ReadFull(file, buffer)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to read middle of file '%s': %w", path, err)}
	}

	// Write position to hasher
	fmt.Fprintf(hasher, "pos:%d;", middleOffset)
	hasher.Write(buffer[:n])

	// Hash the end of the file
	endOffset := size - sampleSize
	if endOffset < 0 {
		endOffset = 0
	}
	_, err = file.Seek(endOffset, 0)
	if err != nil {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to seek to end of file '%s': %w", path, err)}
	}

	n, err = io.ReadFull(file, buffer)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		if opts.SkipErrors {
			return Result{Algorithm: opts.Algorithm, Error: err}
		}
		return Result{Algorithm: opts.Algorithm, Error: fmt.Errorf("failed to read end of file '%s': %w", path, err)}
	}

	// Write position to hasher
	fmt.Fprintf(hasher, "pos:%d;", endOffset)
	hasher.Write(buffer[:n])

	// Compute final hash
	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)

	return Result{
		Hash:      hashString,
		Algorithm: opts.Algorithm,
		Size:      size,
	}
}
