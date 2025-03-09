# FlashFS Walker Implementations

FlashFS provides multiple implementations for walking directory trees, each with different characteristics and use cases. This document explains the different implementations, their performance characteristics, and when to use each one.

## Walker Implementations

### Standard Walker (`Walk`)

The standard walker is a non-streaming implementation that collects all entries in memory before returning them as a slice.

```go
entries, err := walker.Walk(rootDir)
if err != nil {
    // handle error
}

for _, entry := range entries {
    // process entry
}
```

### Callback-based Streaming Walker (`WalkStreamWithCallback`)

The callback-based streaming walker processes entries as they're discovered, calling a user-provided function for each entry.

```go
err := walker.WalkStreamWithCallback(context.Background(), rootDir, walker.DefaultWalkOptions(), 
    func(entry walker.SnapshotEntry) error {
        // process entry
        return nil // return an error to stop walking
    })
if err != nil {
    // handle error
}
```

### Channel-based Streaming Walker (`WalkStream`)

The channel-based streaming walker returns channels for entries and errors, allowing for concurrent processing.

```go
entryChan, errChan := walker.WalkStream(context.Background(), rootDir)

// Process entries as they arrive
for entry := range entryChan {
    // process entry
}

// Check for errors after all entries have been processed
if err := <-errChan; err != nil {
    // handle error
}
```

## Performance Characteristics

Based on benchmarks, here are the performance characteristics of each implementation:

### With Hashing Enabled

| Implementation | Operations/sec | Time/op | Memory/op | Allocations/op |
|----------------|---------------|---------|-----------|----------------|
| StandardWalkDir (Go stdlib) | 4,286 | 261.9 µs | 63.2 KB | 757 |
| Walk | 631 | 1.86 ms | 12.3 MB | 4,678 |
| WalkStreamWithCallback | 579 | 2.09 ms | 13.0 MB | 4,437 |
| WalkStream | 579 | 2.11 ms | 13.7 MB | 4,441 |

### Without Hashing

| Implementation | Operations/sec | Time/op | Memory/op | Allocations/op |
|----------------|---------------|---------|-----------|----------------|
| Walk | 1,642 | 728.7 µs | 277.1 KB | 2,077 |
| WalkStreamWithCallback | 1,101 | 1.09 ms | 185.4 KB | 1,636 |
| WalkStream | 1,056 | 1.11 ms | 188.1 KB | 1,641 |

## When to Use Each Implementation

### Use the Standard Walker (`Walk`) when

- You need all entries before processing can begin
- The directory structure is small to medium-sized
- Simplicity is preferred over advanced features
- You want slightly better performance for small to medium-sized directories

### Use the Callback-based Streaming Walker (`WalkStreamWithCallback`) when

- You want to process entries as they're discovered
- You prefer a callback-based programming style
- You need to handle very large directory structures
- You want to provide progress updates during the walk
- Memory efficiency is important

### Use the Channel-based Streaming Walker (`WalkStream`) when

- You want to process entries as they're discovered
- You prefer a channel-based programming style
- You need to integrate with other Go concurrency patterns
- You want to process entries concurrently with other operations
- You need to handle very large directory structures
- Memory efficiency is important

## Scalability Considerations

The streaming walker implementations (both callback and channel-based) offer significant advantages for large directory structures:

1. **Memory Efficiency**: Since entries are processed as they're discovered, the memory footprint remains relatively constant regardless of the directory size.

2. **Responsiveness**: Users can start processing entries immediately, rather than waiting for the entire walk to complete.

3. **Cancellation**: Operations can be cancelled mid-walk using context cancellation.

4. **Progress Reporting**: Real-time progress updates can be provided during the walk.

For very large directory structures (millions of files), the streaming implementations are strongly recommended to avoid out-of-memory errors and provide better user experience.

## API Design Considerations

The FlashFS walker API is designed to provide flexibility while maintaining simplicity:

1. **Options-based Configuration**: All walkers support the same set of options through the `WalkOptions` struct, allowing for consistent configuration across implementations.

2. **Context Support**: All streaming implementations accept a context for cancellation and timeout support.

3. **Error Handling**: Errors are propagated consistently across all implementations.

4. **Consistent Entry Structure**: All implementations use the same `SnapshotEntry` struct, ensuring consistency.

5. **Naming Consistency**: Function names clearly indicate their behavior:
   - `Walk` - Standard non-streaming walker
   - `WalkStream` - Channel-based streaming walker
   - `WalkStreamWithCallback` - Callback-based streaming walker

## Example: Processing a Large Directory Tree

```go
// Using the callback-based streaming walker
processedCount := 0
totalCount := 0

err := walker.WalkStreamWithCallback(ctx, rootDir, walker.DefaultWalkOptions(), 
    func(entry walker.SnapshotEntry) error {
        processedCount++
        
        // Process the entry
        if !entry.IsDir {
            // Do something with the file
            fmt.Printf("Processing file %s (%d/%d)\n", entry.Path, processedCount, totalCount)
        }
        
        return nil
    })

if err != nil {
    fmt.Printf("Error walking directory: %v\n", err)
}
```

## Example: Concurrent Processing with Channels

```go
// Using the channel-based streaming walker
entryChan, errChan := walker.WalkStream(ctx, rootDir)

// Create a worker pool
const numWorkers = 4
var wg sync.WaitGroup
wg.Add(numWorkers)

// Start workers
for i := 0; i < numWorkers; i++ {
    go func() {
        defer wg.Done()
        for entry := range entryChan {
            if !entry.IsDir {
                // Process the file
                processFile(entry)
            }
        }
    }()
}

// Wait for all entries to be processed
wg.Wait()

// Check for errors
if err := <-errChan; err != nil {
    fmt.Printf("Error walking directory: %v\n", err)
}
```

## Conclusion

FlashFS provides multiple walker implementations to suit different use cases and programming styles. For most applications, the streaming implementations offer the best balance of features, scalability, and usability, especially for large directory structures. The standard walker provides slightly better performance for small to medium-sized directories when all entries need to be collected before processing.
