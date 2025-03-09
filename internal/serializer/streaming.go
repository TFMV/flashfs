package serializer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/TFMV/flashfs/internal/walker"
	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	// DefaultChunkSize is the default size of chunks for streaming serialization
	DefaultChunkSize = 1000

	// Magic bytes to identify the start of a chunk
	chunkMagic uint32 = 0x464C4653 // "FLFS" in ASCII
)

// StreamingOptions contains options for streaming serialization/deserialization
type StreamingOptions struct {
	// ChunkSize is the number of entries per chunk
	ChunkSize int
	// BufferSize is the size of the buffer used for reading/writing
	BufferSize int
}

// DefaultStreamingOptions returns the default options for streaming
func DefaultStreamingOptions() StreamingOptions {
	return StreamingOptions{
		ChunkSize:  DefaultChunkSize,
		BufferSize: 64 * 1024, // 64KB
	}
}

// SnapshotChunk represents a chunk of a snapshot
type SnapshotChunk struct {
	Entries []walker.SnapshotEntry
	Index   int
	Total   int
}

// StreamingSerializer handles streaming serialization of snapshots
type StreamingSerializer struct {
	options StreamingOptions
	builder *flatbuffers.Builder
	mutex   sync.Mutex
}

// NewStreamingSerializer creates a new streaming serializer
func NewStreamingSerializer(options StreamingOptions) *StreamingSerializer {
	return &StreamingSerializer{
		options: options,
		builder: flatbuffers.NewBuilder(0),
	}
}

// SerializeToWriter serializes a snapshot to a writer in chunks
func (s *StreamingSerializer) SerializeToWriter(entries []walker.SnapshotEntry, writer io.Writer) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Create a buffered writer for better performance
	bufWriter := bufio.NewWriterSize(writer, s.options.BufferSize)
	defer bufWriter.Flush()

	// Calculate the number of chunks
	totalEntries := len(entries)
	totalChunks := (totalEntries + s.options.ChunkSize - 1) / s.options.ChunkSize

	// Serialize each chunk
	for i := 0; i < totalChunks; i++ {
		// Calculate the start and end indices for this chunk
		start := i * s.options.ChunkSize
		end := start + s.options.ChunkSize
		if end > totalEntries {
			end = totalEntries
		}

		// Serialize the chunk
		chunkEntries := entries[start:end]
		data, err := SerializeSnapshotFB(chunkEntries, s.builder)
		if err != nil {
			return fmt.Errorf("failed to serialize chunk %d: %w", i, err)
		}

		// Write the chunk header
		// Format: [Magic(4)] [ChunkIndex(4)] [TotalChunks(4)] [ChunkSize(4)]
		header := make([]byte, 16)
		binary.LittleEndian.PutUint32(header[0:4], chunkMagic)
		binary.LittleEndian.PutUint32(header[4:8], uint32(i))
		binary.LittleEndian.PutUint32(header[8:12], uint32(totalChunks))
		binary.LittleEndian.PutUint32(header[12:16], uint32(len(data)))

		// Write the header
		if _, err := bufWriter.Write(header); err != nil {
			return fmt.Errorf("failed to write chunk header: %w", err)
		}

		// Write the chunk data
		if _, err := bufWriter.Write(data); err != nil {
			return fmt.Errorf("failed to write chunk data: %w", err)
		}
	}

	return nil
}

// DeserializeFromReader deserializes a snapshot from a reader in chunks
func DeserializeFromReader(reader io.Reader, callback func(SnapshotChunk) error) error {
	// Create a buffered reader for better performance
	bufReader := bufio.NewReaderSize(reader, DefaultStreamingOptions().BufferSize)

	// Read chunks until EOF
	for {
		// Read the chunk header
		header := make([]byte, 16)
		_, err := io.ReadFull(bufReader, header)
		if err == io.EOF {
			// End of file, we're done
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read chunk header: %w", err)
		}

		// Parse the header
		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != chunkMagic {
			return fmt.Errorf("invalid chunk magic: %x", magic)
		}

		chunkIndex := binary.LittleEndian.Uint32(header[4:8])
		totalChunks := binary.LittleEndian.Uint32(header[8:12])
		chunkSize := binary.LittleEndian.Uint32(header[12:16])

		// Read the chunk data
		data := make([]byte, chunkSize)
		_, err = io.ReadFull(bufReader, data)
		if err != nil {
			return fmt.Errorf("failed to read chunk data: %w", err)
		}

		// Deserialize the chunk
		entries, err := DeserializeSnapshotFB(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize chunk %d: %w", chunkIndex, err)
		}

		// Call the callback with the chunk
		chunk := SnapshotChunk{
			Entries: entries,
			Index:   int(chunkIndex),
			Total:   int(totalChunks),
		}
		if err := callback(chunk); err != nil {
			return fmt.Errorf("callback error for chunk %d: %w", chunkIndex, err)
		}
	}
}

// StreamingDiffSerializer handles streaming serialization of diffs
type StreamingDiffSerializer struct {
	options StreamingOptions
	builder *flatbuffers.Builder
	mutex   sync.Mutex
}

// DiffChunk represents a chunk of a diff
type DiffChunk struct {
	Entries []DiffEntry
	Index   int
	Total   int
}

// NewStreamingDiffSerializer creates a new streaming diff serializer
func NewStreamingDiffSerializer(options StreamingOptions) *StreamingDiffSerializer {
	return &StreamingDiffSerializer{
		options: options,
		builder: flatbuffers.NewBuilder(0),
	}
}

// SerializeDiffToWriter serializes a diff to a writer in chunks
func (s *StreamingDiffSerializer) SerializeDiffToWriter(entries []DiffEntry, writer io.Writer) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Create a buffered writer for better performance
	bufWriter := bufio.NewWriterSize(writer, s.options.BufferSize)
	defer bufWriter.Flush()

	// Calculate the number of chunks
	totalEntries := len(entries)
	totalChunks := (totalEntries + s.options.ChunkSize - 1) / s.options.ChunkSize

	// Serialize each chunk
	for i := 0; i < totalChunks; i++ {
		// Calculate the start and end indices for this chunk
		start := i * s.options.ChunkSize
		end := start + s.options.ChunkSize
		if end > totalEntries {
			end = totalEntries
		}

		// Serialize the chunk
		chunkEntries := entries[start:end]
		data, err := SerializeDiffFB(chunkEntries, s.builder)
		if err != nil {
			return fmt.Errorf("failed to serialize diff chunk %d: %w", i, err)
		}

		// Write the chunk header
		// Format: [Magic(4)] [ChunkIndex(4)] [TotalChunks(4)] [ChunkSize(4)]
		header := make([]byte, 16)
		binary.LittleEndian.PutUint32(header[0:4], chunkMagic)
		binary.LittleEndian.PutUint32(header[4:8], uint32(i))
		binary.LittleEndian.PutUint32(header[8:12], uint32(totalChunks))
		binary.LittleEndian.PutUint32(header[12:16], uint32(len(data)))

		// Write the header
		if _, err := bufWriter.Write(header); err != nil {
			return fmt.Errorf("failed to write diff chunk header: %w", err)
		}

		// Write the chunk data
		if _, err := bufWriter.Write(data); err != nil {
			return fmt.Errorf("failed to write diff chunk data: %w", err)
		}
	}

	return nil
}

// DeserializeDiffFromReader deserializes a diff from a reader in chunks
func DeserializeDiffFromReader(reader io.Reader, callback func(DiffChunk) error) error {
	// Create a buffered reader for better performance
	bufReader := bufio.NewReaderSize(reader, DefaultStreamingOptions().BufferSize)

	// Read chunks until EOF
	for {
		// Read the chunk header
		header := make([]byte, 16)
		_, err := io.ReadFull(bufReader, header)
		if err == io.EOF {
			// End of file, we're done
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read diff chunk header: %w", err)
		}

		// Parse the header
		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != chunkMagic {
			return fmt.Errorf("invalid diff chunk magic: %x", magic)
		}

		chunkIndex := binary.LittleEndian.Uint32(header[4:8])
		totalChunks := binary.LittleEndian.Uint32(header[8:12])
		chunkSize := binary.LittleEndian.Uint32(header[12:16])

		// Read the chunk data
		data := make([]byte, chunkSize)
		_, err = io.ReadFull(bufReader, data)
		if err != nil {
			return fmt.Errorf("failed to read diff chunk data: %w", err)
		}

		// Deserialize the chunk
		entries, err := DeserializeDiffFB(data)
		if err != nil {
			return fmt.Errorf("failed to deserialize diff chunk %d: %w", chunkIndex, err)
		}

		// Call the callback with the chunk
		chunk := DiffChunk{
			Entries: entries,
			Index:   int(chunkIndex),
			Total:   int(totalChunks),
		}
		if err := callback(chunk); err != nil {
			return fmt.Errorf("callback error for diff chunk %d: %w", chunkIndex, err)
		}
	}
}
