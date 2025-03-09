package hash

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// BufferPoolMetrics holds metrics for a BufferPool.
type BufferPoolMetrics struct {
	Gets              uint64
	Puts              uint64
	NewAllocations    uint64
	ResizeAllocations uint64
	RejectedBuffers   uint64 // Buffers too small to be put back
	Size              int    // Current buffer size
	Timeouts          uint64
	Name              string
}

// String provides a string representation of the buffer pool metrics.
func (m *BufferPoolMetrics) String() string {
	return fmt.Sprintf(
		"BufferPool '%s' Metrics:\n"+
			"  Gets:               %d\n"+
			"  Puts:               %d\n"+
			"  New Allocations:    %d\n"+
			"  Resize Allocations: %d\n"+
			"  Rejected Buffers:   %d\n"+
			"  Size:               %d\n"+
			"  Timeouts:           %d\n",
		m.Name,
		atomic.LoadUint64(&m.Gets),
		atomic.LoadUint64(&m.Puts),
		atomic.LoadUint64(&m.NewAllocations),
		atomic.LoadUint64(&m.ResizeAllocations),
		atomic.LoadUint64(&m.RejectedBuffers),
		m.Size,
		atomic.LoadUint64(&m.Timeouts),
	)
}

// BufferPoolOptions allows for configuring the BufferPool.
type BufferPoolOptions struct {
	BufferSize           int           // Initial and default buffer size
	MinBufferSize        int           // Minimum buffer size to accept back into the pool
	MaxBufferSize        int           // Maximum buffer size to create
	PoolName             string        // Name for the pool (for metrics/logging)
	ShortageNotification chan struct{} // Channel for communicating buffer supply shortages
	GetTimeout           time.Duration // Timeout for waiting on a buffer
}

// DefaultBufferPoolOptions returns a set of default options for the BufferPool.
func DefaultBufferPoolOptions() BufferPoolOptions {
	return BufferPoolOptions{
		BufferSize:    DefaultOptions().BufferSize,
		MinBufferSize: DefaultOptions().BufferSize / 2, // Accept buffers half the default size
		MaxBufferSize: DefaultOptions().BufferSize * 2, // Allow buffers up to twice the default size
		PoolName:      "default",
		GetTimeout:    0, // No timeout by default
	}
}

// BufferPool provides a pool of reusable byte slices to reduce memory allocations
// and garbage collection pressure during hashing operations.
type BufferPool struct {
	pool                 sync.Pool
	size                 int
	name                 string
	metrics              *BufferPoolMetrics
	minSize              int
	maxSize              int
	shortageNotification chan struct{}
	getTimeout           time.Duration
}

// NewBufferPool creates a new buffer pool with the specified options.
func NewBufferPool(options BufferPoolOptions) *BufferPool {
	if options.BufferSize <= 0 {
		options.BufferSize = DefaultOptions().BufferSize
	}
	if options.MinBufferSize <= 0 {
		options.MinBufferSize = options.BufferSize / 2
	}
	if options.MaxBufferSize < options.BufferSize {
		options.MaxBufferSize = options.BufferSize * 2
	}

	metrics := &BufferPoolMetrics{
		Name: options.PoolName,
		Size: options.BufferSize,
	}

	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				atomic.AddUint64(&metrics.NewAllocations, 1)
				buffer := make([]byte, options.BufferSize)
				return &buffer // Store pointer to avoid copying large slices
			},
		},
		size:                 options.BufferSize,
		name:                 options.PoolName,
		metrics:              metrics,
		minSize:              options.MinBufferSize,
		maxSize:              options.MaxBufferSize,
		shortageNotification: options.ShortageNotification,
		getTimeout:           options.GetTimeout,
	}
}

// Get retrieves a buffer from the pool or creates a new one if none are available.
// The returned buffer is guaranteed to be at least the size specified when the pool was created.
func (bp *BufferPool) Get() ([]byte, error) {
	atomic.AddUint64(&bp.metrics.Gets, 1)

	var bufferPtr *[]byte
	if bp.getTimeout > 0 {
		select {
		case <-time.After(bp.getTimeout):
			atomic.AddUint64(&bp.metrics.Timeouts, 1)
			return nil, fmt.Errorf("timeout getting buffer from pool %s", bp.name)
		case bufferPtr = <-bp.getWithTimeout():
		}
	} else {
		bufferPtr = bp.pool.Get().(*[]byte)
	}

	buffer := *bufferPtr

	// Ensure the buffer is the correct size
	if cap(buffer) < bp.size {
		if bp.size <= bp.maxSize {
			atomic.AddUint64(&bp.metrics.ResizeAllocations, 1)
			*bufferPtr = make([]byte, bp.size)
			buffer = *bufferPtr
		} else {
			atomic.AddUint64(&bp.metrics.ResizeAllocations, 1)
			*bufferPtr = make([]byte, bp.maxSize)
			buffer = *bufferPtr
			if bp.shortageNotification != nil {
				select {
				case bp.shortageNotification <- struct{}{}:
				default:
					// Don't block if channel is full
				}
			}
		}
	} else if len(buffer) != bp.size {
		// Restore the original size if it was changed
		buffer = buffer[:bp.size]
		*bufferPtr = buffer
	}

	return buffer, nil
}

// getWithTimeout attempts to retrieve a buffer pointer from the pool using a channel.
// This enables a select-based timeout mechanism in Get().
func (bp *BufferPool) getWithTimeout() <-chan *[]byte {
	result := make(chan *[]byte, 1) // Buffered channel to prevent blocking if Get() times out
	go func() {
		bufferPtr := bp.pool.Get().(*[]byte)
		result <- bufferPtr
	}()
	return result
}

// Put returns a buffer to the pool for reuse.
// The buffer should not be used after calling Put.
func (bp *BufferPool) Put(buffer []byte) {
	atomic.AddUint64(&bp.metrics.Puts, 1)

	// Only store buffers that are the right size or larger
	if cap(buffer) < bp.minSize {
		atomic.AddUint64(&bp.metrics.RejectedBuffers, 1)
		return // Let it be garbage collected
	}

	// Ensure the buffer is the correct size for future use
	if cap(buffer) > bp.maxSize {
		buffer = buffer[:bp.maxSize]
	} else if cap(buffer) >= bp.size {
		buffer = buffer[:bp.size]
	} else {
		// Buffer capacity is smaller than the expected size
		// This shouldn't happen in normal operation, but we handle it gracefully
		atomic.AddUint64(&bp.metrics.RejectedBuffers, 1)
		return // Let it be garbage collected
	}

	bufferPtr := &buffer
	bp.pool.Put(bufferPtr)
}

// Metrics returns a copy of the current metrics for the pool.
func (bp *BufferPool) Metrics() BufferPoolMetrics {
	return *bp.metrics // Return a copy to prevent modification
}

// SetSize changes the requested buffer size for the pool.
// This does not affect existing buffers in the pool, only future allocations.
func (bp *BufferPool) SetSize(size int) {
	if size <= 0 {
		return
	}
	if size > bp.maxSize {
		bp.size = bp.maxSize // Do not exceed the maximum
	} else {
		bp.size = size
	}
	bp.metrics.Size = bp.size
}

// DefaultBufferPool is a shared buffer pool with the default buffer size.
var DefaultBufferPool = NewBufferPool(DefaultBufferPoolOptions())

// GetBuffer retrieves a buffer from the default pool.
func GetBuffer() ([]byte, error) {
	return DefaultBufferPool.Get()
}

// PutBuffer returns a buffer to the default pool.
func PutBuffer(buffer []byte) {
	DefaultBufferPool.Put(buffer)
}
