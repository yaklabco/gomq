package storage

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Errors returned by MFile operations.
var (
	ErrClosed           = errors.New("mfile: closed")
	ErrCapacityExceeded = errors.New("mfile: capacity exceeded")
	ErrReadOnly         = errors.New("mfile: read-only")
	ErrBoundsCheck      = errors.New("mfile: out of bounds")
)

// Advice represents madvise hints for memory-mapped regions.
type Advice int

// Madvise hints matching POSIX constants.
const (
	AdviceNormal     Advice = 0
	AdviceRandom     Advice = 1
	AdviceSequential Advice = 2
	AdviceWillNeed   Advice = 3
	AdviceDontNeed   Advice = 4
)

// MFile is a memory-mapped file that supports append-only writes and random reads.
// The file is mapped at a fixed capacity; on Close, it is truncated to the logical size.
type MFile struct {
	mu       sync.RWMutex
	data     []byte
	size     atomic.Int64
	capacity int64
	path     string
	closed   atomic.Bool
	deleted  atomic.Bool
	readOnly bool
}

// Path returns the file path.
func (mf *MFile) Path() string { return mf.path }

// Capacity returns the mapped capacity in bytes.
func (mf *MFile) Capacity() int64 { return mf.capacity }

// Size returns the current logical size (bytes written).
func (mf *MFile) Size() int64 { return mf.size.Load() }

// Resize sets the logical size without changing the file on disk.
func (mf *MFile) Resize(newSize int64) {
	if newSize <= mf.capacity {
		mf.size.Store(newSize)
	}
}
