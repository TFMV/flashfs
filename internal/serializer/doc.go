/*
Package serializer provides efficient serialization and deserialization of FlashFS data structures.

This package implements serialization using FlatBuffers, a memory-efficient serialization library
that allows for zero-copy deserialization. The package provides both standard and streaming
serialization options for snapshots and diffs.

# Basic Usage

The basic serialization functions are:

	// Serialize a snapshot
	data, err := serializer.SerializeSnapshotFB(entries, nil)

	// Deserialize a snapshot
	entries, err := serializer.DeserializeSnapshotFB(data)

	// Serialize a diff
	data, err := serializer.SerializeDiffFB(diffEntries, nil)

	// Deserialize a diff
	diffEntries, err := serializer.DeserializeDiffFB(data)

# Memory Optimization

To optimize memory usage, you can reuse a FlatBuffers builder:

	// Create a builder once
	builder := flatbuffers.NewBuilder(0)

	// Reuse it for multiple serializations
	for _, snapshot := range snapshots {
	    data, err := serializer.SerializeSnapshotFB(snapshot, builder)
	    // Process data...
	}

The serializer also provides a builder pool that automatically manages builders
when you pass nil as the builder parameter:

	// This will get a builder from the pool and return it when done
	data, err := serializer.SerializeSnapshotFB(entries, nil)

# Streaming Serialization

For very large snapshots or diffs, the package provides streaming serialization:

	// Create a streaming serializer
	serializer := serializer.NewStreamingSerializer(serializer.DefaultStreamingOptions())

	// Serialize to a writer (file, network, etc.)
	err := serializer.SerializeToWriter(entries, writer)

	// Deserialize from a reader with a callback for each chunk
	err := serializer.DeserializeFromReader(reader, func(chunk SnapshotChunk) error {
	    // Process chunk.Entries
	    // Track progress with chunk.Index and chunk.Total
	    return nil
	})

Streaming serialization breaks the data into chunks, which is useful for:
- Processing very large snapshots that don't fit in memory
- Showing progress during serialization/deserialization
- Parallel processing of chunks
- Network streaming

# Performance Considerations

1. **Builder Reuse**: Always reuse builders for multiple serializations to reduce allocations.

 2. **Chunk Size**: For streaming serialization, adjust the chunk size based on your memory constraints
    and processing needs. Larger chunks are more efficient but use more memory.

 3. **Buffer Size**: The buffer size affects I/O performance. Larger buffers generally improve
    performance but use more memory.

 4. **Parallel Processing**: For large datasets, consider processing chunks in parallel during
    deserialization.

# Examples

## Basic Serialization with Builder Reuse

	// Create a builder to reuse
	builder := flatbuffers.NewBuilder(0)

	// Serialize multiple snapshots
	for _, entries := range snapshots {
	    data, err := serializer.SerializeSnapshotFB(entries, builder)
	    if err != nil {
	        return err
	    }
	    // Store or process data...
	}

## Streaming Serialization to a File

	// Open a file for writing
	file, err := os.Create("snapshot.dat")
	if err != nil {
	    return err
	}
	defer file.Close()

	// Create a streaming serializer
	options := serializer.StreamingOptions{
	    ChunkSize:  5000,    // 5000 entries per chunk
	    BufferSize: 128*1024, // 128KB buffer
	}
	serializer := serializer.NewStreamingSerializer(options)

	// Serialize the snapshot
	err = serializer.SerializeToWriter(entries, file)
	if err != nil {
	    return err
	}

## Streaming Deserialization with Progress Reporting

	// Open a file for reading
	file, err := os.Open("snapshot.dat")
	if err != nil {
	    return err
	}
	defer file.Close()

	// Track progress
	var totalEntries int
	var processedChunks int

	// Deserialize with a callback
	err = serializer.DeserializeFromReader(file, func(chunk SnapshotChunk) error {
	    // Process entries
	    processEntries(chunk.Entries)

	    // Update progress
	    totalEntries += len(chunk.Entries)
	    processedChunks++

	    // Report progress
	    progress := float64(processedChunks) / float64(chunk.Total) * 100
	    fmt.Printf("Progress: %.2f%% (%d/%d chunks, %d entries)\n",
	        progress, processedChunks, chunk.Total, totalEntries)

	    return nil
	})
	if err != nil {
	    return err
	}

## Parallel Processing of Chunks

	// Open a file for reading
	file, err := os.Open("snapshot.dat")
	if err != nil {
	    return err
	}
	defer file.Close()

	// Create a worker pool
	var wg sync.WaitGroup
	chunkChan := make(chan SnapshotChunk, 10)
	errChan := make(chan error, 1)

	// Start worker goroutines
	for i := 0; i < runtime.NumCPU(); i++ {
	    wg.Add(1)
	    go func() {
	        defer wg.Done()
	        for chunk := range chunkChan {
	            // Process chunk in parallel
	            if err := processChunk(chunk); err != nil {
	                select {
	                case errChan <- err:
	                default:
	                }
	                return
	            }
	        }
	    }()
	}

	// Deserialize and send chunks to workers
	go func() {
	    err := serializer.DeserializeFromReader(file, func(chunk SnapshotChunk) error {
	        select {
	        case err := <-errChan:
	            return err
	        default:
	            chunkChan <- chunk
	            return nil
	        }
	    })

	    close(chunkChan)
	    if err != nil {
	        select {
	        case errChan <- err:
	        default:
	        }
	    }
	}()

	// Wait for all workers to finish
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
	    return err
	default:
	    return nil
	}
*/
package serializer
