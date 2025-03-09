package hash

import (
	"sync"
	"testing"
	"time"
)

func TestBufferPool(t *testing.T) {
	// Create a buffer pool with a specific size
	bufferSize := 1024
	pool := NewBufferPool(BufferPoolOptions{
		BufferSize: bufferSize,
		PoolName:   "test-pool",
	})

	// Test getting a buffer
	buffer, err := pool.Get()
	if err != nil {
		t.Fatalf("Failed to get buffer: %v", err)
	}
	if len(buffer) != bufferSize {
		t.Errorf("Expected buffer size %d, got %d", bufferSize, len(buffer))
	}

	// Modify the buffer
	for i := 0; i < len(buffer); i++ {
		buffer[i] = byte(i % 256)
	}

	// Put it back
	pool.Put(buffer)

	// Get another buffer (should be the same one)
	buffer2, err := pool.Get()
	if err != nil {
		t.Fatalf("Failed to get second buffer: %v", err)
	}
	if len(buffer2) != bufferSize {
		t.Errorf("Expected buffer size %d, got %d", bufferSize, len(buffer2))
	}

	// Check if the buffer was reset (should still have our data since we don't clear it)
	for i := 0; i < len(buffer2); i++ {
		if buffer2[i] != byte(i%256) {
			t.Errorf("Buffer data at index %d was changed: expected %d, got %d",
				i, byte(i%256), buffer2[i])
			break
		}
	}

	// Test putting a smaller buffer
	smallBuffer := make([]byte, bufferSize/2)
	pool.Put(smallBuffer) // This should be ignored

	// Test putting a larger buffer
	largeBuffer := make([]byte, bufferSize*2)
	pool.Put(largeBuffer)

	// Get another buffer, should be either the original or the large one
	buffer3, err := pool.Get()
	if err != nil {
		t.Fatalf("Failed to get third buffer: %v", err)
	}
	if len(buffer3) != bufferSize {
		t.Errorf("Expected buffer size %d, got %d", bufferSize, len(buffer3))
	}

	// Check metrics
	metrics := pool.Metrics()
	if metrics.Gets < 3 {
		t.Errorf("Expected at least 3 Gets in metrics, got %d", metrics.Gets)
	}
	if metrics.Puts < 3 {
		t.Errorf("Expected at least 3 Puts in metrics, got %d", metrics.Puts)
	}
}

func TestDefaultBufferPool(t *testing.T) {
	// Test the default buffer pool
	buffer, err := GetBuffer()
	if err != nil {
		t.Fatalf("Failed to get buffer from default pool: %v", err)
	}
	expectedSize := DefaultOptions().BufferSize
	if len(buffer) != expectedSize {
		t.Errorf("Expected buffer size %d, got %d", expectedSize, len(buffer))
	}

	// Put it back
	PutBuffer(buffer)

	// Get another buffer
	buffer2, err := GetBuffer()
	if err != nil {
		t.Fatalf("Failed to get second buffer from default pool: %v", err)
	}
	if len(buffer2) != expectedSize {
		t.Errorf("Expected buffer size %d, got %d", expectedSize, len(buffer2))
	}
}

func TestConcurrentBufferPool(t *testing.T) {
	// Test concurrent access to the buffer pool
	pool := NewBufferPool(BufferPoolOptions{
		BufferSize: 1024,
		PoolName:   "concurrent-test-pool",
	})
	const numGoroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errChan := make(chan error, numGoroutines*iterations)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				buffer, err := pool.Get()
				if err != nil {
					errChan <- err
					continue
				}
				if len(buffer) != 1024 {
					errChan <- err
					continue
				}

				// Write something to the buffer
				for k := 0; k < 10; k++ {
					buffer[k] = byte(id)
				}

				pool.Put(buffer)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Error in concurrent test: %v", err)
	}

	// Check metrics
	metrics := pool.Metrics()
	if metrics.Gets != numGoroutines*iterations {
		t.Errorf("Expected %d Gets in metrics, got %d", numGoroutines*iterations, metrics.Gets)
	}
}

func TestBufferPoolTimeout(t *testing.T) {
	// Create a buffer pool with a timeout
	pool := NewBufferPool(BufferPoolOptions{
		BufferSize: 1024,
		PoolName:   "timeout-test-pool",
		GetTimeout: 1 * time.Millisecond, // Very short timeout for testing
	})

	// Get a buffer
	buffer, err := pool.Get()
	if err != nil {
		t.Fatalf("Failed to get first buffer: %v", err)
	}

	// This should work fine
	pool.Put(buffer)

	// Check metrics
	metrics := pool.Metrics()
	if metrics.Timeouts > 0 {
		t.Errorf("Expected 0 timeouts, got %d", metrics.Timeouts)
	}
}

func BenchmarkBufferPool(b *testing.B) {
	pool := NewBufferPool(BufferPoolOptions{
		BufferSize: 1024 * 1024, // 1MB buffer
		PoolName:   "benchmark-pool",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer, err := pool.Get()
		if err != nil {
			b.Fatalf("Failed to get buffer: %v", err)
		}
		pool.Put(buffer)
	}
}

func BenchmarkNoBufferPool(b *testing.B) {
	size := 1024 * 1024 // 1MB buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, size)
		_ = buffer
	}
}

func BenchmarkConcurrentBufferPool(b *testing.B) {
	pool := NewBufferPool(BufferPoolOptions{
		BufferSize: 1024 * 1024, // 1MB buffer
		PoolName:   "concurrent-benchmark-pool",
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buffer, err := pool.Get()
			if err != nil {
				b.Fatalf("Failed to get buffer: %v", err)
			}
			// Do something with the buffer
			for i := 0; i < 1000; i++ {
				buffer[i] = byte(i)
			}
			pool.Put(buffer)
		}
	})
}
