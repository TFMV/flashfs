package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// DataStore handles the storage of actual snapshot and diff data using FlatBuffers
type DataStore struct {
	baseDir    string
	encoder    *zstd.Encoder
	decoder    *zstd.Decoder
	mutex      sync.RWMutex
	bufferPool sync.Pool
}

// NewDataStore creates a new data store
func NewDataStore(baseDir string) (*DataStore, error) {
	// Create the base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data store directory: %w", err)
	}

	// Create the encoder and decoder
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}

	return &DataStore{
		baseDir: baseDir,
		encoder: encoder,
		decoder: decoder,
		bufferPool: sync.Pool{
			New: func() interface{} {
				buffer := make([]byte, 64*1024) // 64KB buffer
				return &buffer
			},
		},
	}, nil
}

// Close closes the data store
func (d *DataStore) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.encoder.Close()
	d.decoder.Close()
	return nil
}

// WriteSnapshotData writes snapshot data to the data store
func (d *DataStore) WriteSnapshotData(id string, data []byte) (string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Compress the data
	compressed := d.encoder.EncodeAll(data, nil)

	// Create the file path
	filePath := filepath.Join(d.baseDir, fmt.Sprintf("snapshot_%s.data", id))

	// Write the data to the file
	if err := os.WriteFile(filePath, compressed, 0644); err != nil {
		return "", fmt.Errorf("failed to write snapshot data: %w", err)
	}

	// Return the reference to the data
	return fmt.Sprintf("snapshot_%s.data:offset=0:len=%d", id, len(data)), nil
}

// ReadSnapshotData reads snapshot data from the data store
func (d *DataStore) ReadSnapshotData(ref string) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Parse the reference
	var fileName string
	var offset, length int

	// Handle different reference formats
	if n, err := fmt.Sscanf(ref, "%s:offset=%d:len=%d", &fileName, &offset, &length); err != nil || n != 3 {
		// Try alternative format without spaces
		parts := strings.Split(ref, ":")
		if len(parts) < 3 {
			return nil, fmt.Errorf("invalid data reference format: %s", ref)
		}

		fileName = parts[0]

		offsetParts := strings.Split(parts[1], "=")
		if len(offsetParts) != 2 {
			return nil, fmt.Errorf("invalid offset format in data reference: %s", parts[1])
		}

		var err error
		offset, err = strconv.Atoi(offsetParts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid offset value in data reference: %w", err)
		}

		lengthParts := strings.Split(parts[2], "=")
		if len(lengthParts) != 2 {
			return nil, fmt.Errorf("invalid length format in data reference: %s", parts[2])
		}

		length, err = strconv.Atoi(lengthParts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid length value in data reference: %w", err)
		}
	}

	// Create the file path
	filePath := filepath.Join(d.baseDir, fileName)

	// Read the data from the file
	compressed, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot data: %w", err)
	}

	// Decompress the data
	data, err := d.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress snapshot data: %w", err)
	}

	// Return the data
	if offset == 0 && length == len(data) {
		return data, nil
	}

	// Handle partial reads
	if offset+length > len(data) {
		return nil, fmt.Errorf("invalid offset or length")
	}

	return data[offset : offset+length], nil
}

// WriteDiffData writes diff data to the data store
func (d *DataStore) WriteDiffData(baseID, targetID string, data []byte) (string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Compress the data
	compressed := d.encoder.EncodeAll(data, nil)

	// Create the file path
	filePath := filepath.Join(d.baseDir, fmt.Sprintf("diff_%s_%s.data", baseID, targetID))

	// Write the data to the file
	if err := os.WriteFile(filePath, compressed, 0644); err != nil {
		return "", fmt.Errorf("failed to write diff data: %w", err)
	}

	// Return the reference to the data
	return fmt.Sprintf("diff_%s_%s.data:offset=0:len=%d", baseID, targetID, len(data)), nil
}

// ReadDiffData reads diff data from the data store
func (d *DataStore) ReadDiffData(ref string) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Parse the reference
	var fileName string
	var offset, length int

	// Handle different reference formats
	if n, err := fmt.Sscanf(ref, "%s:offset=%d:len=%d", &fileName, &offset, &length); err != nil || n != 3 {
		// Try alternative format without spaces
		parts := strings.Split(ref, ":")
		if len(parts) < 3 {
			return nil, fmt.Errorf("invalid data reference format: %s", ref)
		}

		fileName = parts[0]

		offsetParts := strings.Split(parts[1], "=")
		if len(offsetParts) != 2 {
			return nil, fmt.Errorf("invalid offset format in data reference: %s", parts[1])
		}

		var err error
		offset, err = strconv.Atoi(offsetParts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid offset value in data reference: %w", err)
		}

		lengthParts := strings.Split(parts[2], "=")
		if len(lengthParts) != 2 {
			return nil, fmt.Errorf("invalid length format in data reference: %s", parts[2])
		}

		length, err = strconv.Atoi(lengthParts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid length value in data reference: %w", err)
		}
	}

	// Create the file path
	filePath := filepath.Join(d.baseDir, fileName)

	// Read the data from the file
	compressed, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read diff data: %w", err)
	}

	// Decompress the data
	data, err := d.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress diff data: %w", err)
	}

	// Return the data
	if offset == 0 && length == len(data) {
		return data, nil
	}

	// Handle partial reads
	if offset+length > len(data) {
		return nil, fmt.Errorf("invalid offset or length")
	}

	return data[offset : offset+length], nil
}

// DeleteSnapshotData deletes snapshot data from the data store
func (d *DataStore) DeleteSnapshotData(id string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Create the file path
	filePath := filepath.Join(d.baseDir, fmt.Sprintf("snapshot_%s.data", id))

	// Delete the file
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete snapshot data: %w", err)
	}

	return nil
}

// DeleteDiffData deletes diff data from the data store
func (d *DataStore) DeleteDiffData(baseID, targetID string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Create the file path
	filePath := filepath.Join(d.baseDir, fmt.Sprintf("diff_%s_%s.data", baseID, targetID))

	// Delete the file
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete diff data: %w", err)
	}

	return nil
}

// AppendSnapshotData appends data to an existing snapshot data file
func (d *DataStore) AppendSnapshotData(id string, data []byte) (string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Create the file path
	filePath := filepath.Join(d.baseDir, fmt.Sprintf("snapshot_%s.data", id))

	// Open the file for appending
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to open snapshot data file: %w", err)
	}
	defer file.Close()

	// Get the current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to get snapshot data file info: %w", err)
	}
	offset := fileInfo.Size()

	// Compress the data
	compressed := d.encoder.EncodeAll(data, nil)

	// Write the data to the file
	if _, err := file.Write(compressed); err != nil {
		return "", fmt.Errorf("failed to append snapshot data: %w", err)
	}

	// Return the reference to the data
	return fmt.Sprintf("snapshot_%s.data:offset=%d:len=%d", id, offset, len(data)), nil
}

// StreamSnapshotData streams snapshot data from the data store
func (d *DataStore) StreamSnapshotData(ref string, writer io.Writer) error {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Parse the reference
	var fileName string
	var offset, length int
	_, err := fmt.Sscanf(ref, "%s:offset:%d:len:%d", &fileName, &offset, &length)
	if err != nil {
		return fmt.Errorf("invalid data reference: %w", err)
	}

	// Create the file path
	filePath := filepath.Join(d.baseDir, fileName)

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot data file: %w", err)
	}
	defer file.Close()

	// Seek to the offset
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek in snapshot data file: %w", err)
	}

	// Get a buffer from the pool
	bufferPtr := d.bufferPool.Get().(*[]byte)
	buffer := *bufferPtr
	defer d.bufferPool.Put(bufferPtr)

	// Read and decompress the data in chunks
	var totalRead int
	for totalRead < length {
		// Calculate how much to read
		toRead := len(buffer)
		if totalRead+toRead > length {
			toRead = length - totalRead
		}

		// Read a chunk
		n, err := file.Read(buffer[:toRead])
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read snapshot data: %w", err)
		}
		if n == 0 {
			break
		}

		// Decompress the chunk
		decompressed, err := d.decoder.DecodeAll(buffer[:n], nil)
		if err != nil {
			return fmt.Errorf("failed to decompress snapshot data: %w", err)
		}

		// Write the decompressed data
		if _, err := writer.Write(decompressed); err != nil {
			return fmt.Errorf("failed to write decompressed data: %w", err)
		}

		totalRead += n
	}

	return nil
}

// StreamDiffData streams diff data from the data store
func (d *DataStore) StreamDiffData(ref string, writer io.Writer) error {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// Parse the reference
	var fileName string
	var offset, length int
	_, err := fmt.Sscanf(ref, "%s:offset:%d:len:%d", &fileName, &offset, &length)
	if err != nil {
		return fmt.Errorf("invalid data reference: %w", err)
	}

	// Create the file path
	filePath := filepath.Join(d.baseDir, fileName)

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open diff data file: %w", err)
	}
	defer file.Close()

	// Seek to the offset
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek in diff data file: %w", err)
	}

	// Get a buffer from the pool
	bufferPtr := d.bufferPool.Get().(*[]byte)
	buffer := *bufferPtr
	defer d.bufferPool.Put(bufferPtr)

	// Read and decompress the data in chunks
	var totalRead int
	for totalRead < length {
		// Calculate how much to read
		toRead := len(buffer)
		if totalRead+toRead > length {
			toRead = length - totalRead
		}

		// Read a chunk
		n, err := file.Read(buffer[:toRead])
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read diff data: %w", err)
		}
		if n == 0 {
			break
		}

		// Decompress the chunk
		decompressed, err := d.decoder.DecodeAll(buffer[:n], nil)
		if err != nil {
			return fmt.Errorf("failed to decompress diff data: %w", err)
		}

		// Write the decompressed data
		if _, err := writer.Write(decompressed); err != nil {
			return fmt.Errorf("failed to write decompressed data: %w", err)
		}

		totalRead += n
	}

	return nil
}
