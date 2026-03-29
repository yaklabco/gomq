//go:build unix

package storage

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// OpenMFile creates or opens a memory-mapped file with the given capacity.
// If the file already exists, its current size becomes the logical size and capacity
// is set to the larger of the requested capacity and the file size.
func OpenMFile(path string, capacity int64) (*MFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open mfile %s: %w", path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat mfile %s: %w", path, err)
	}

	fileSize := info.Size()
	if capacity < fileSize {
		capacity = fileSize
	}

	if capacity > fileSize {
		if err := file.Truncate(capacity); err != nil {
			return nil, fmt.Errorf("truncate mfile %s to capacity %d: %w", path, capacity, err)
		}
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(capacity), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap %s: %w", path, err)
	}

	mf := &MFile{
		data:     data,
		capacity: capacity,
		path:     path,
	}
	mf.size.Store(fileSize)
	return mf, nil
}

// OpenMFileReadOnly opens an existing file as a read-only memory map.
// The logical size and capacity are both set to the file's current size.
func OpenMFileReadOnly(path string) (*MFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open mfile read-only %s: %w", path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat mfile %s: %w", path, err)
	}

	fileSize := info.Size()
	if fileSize == 0 {
		// Cannot mmap an empty file; return a valid MFile with nil data.
		mf := &MFile{
			capacity: 0,
			path:     path,
			readOnly: true,
		}
		return mf, nil
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap read-only %s: %w", path, err)
	}

	mf := &MFile{
		data:     data,
		capacity: fileSize,
		path:     path,
		readOnly: true,
	}
	mf.size.Store(fileSize)
	return mf, nil
}

// Write appends p to the memory-mapped region. It is safe for concurrent use.
// Write uses a RLock so multiple writers can proceed concurrently with atomic size updates;
// however, each individual Write call reserves space atomically so data does not overlap.
func (mf *MFile) Write(buf []byte) (int, error) {
	if mf.closed.Load() {
		return 0, ErrClosed
	}
	if mf.readOnly {
		return 0, ErrReadOnly
	}

	writeLen := int64(len(buf))

	// Atomically reserve space.
	newSize := mf.size.Add(writeLen)
	oldSize := newSize - writeLen

	if newSize > mf.capacity {
		// Roll back the reservation.
		mf.size.Add(-writeLen)
		return 0, fmt.Errorf("write %d bytes at offset %d exceeds capacity %d: %w", writeLen, oldSize, mf.capacity, ErrCapacityExceeded)
	}

	mf.mu.RLock()
	defer mf.mu.RUnlock()

	if mf.closed.Load() {
		// Roll back reservation if closed between check and lock.
		mf.size.Add(-writeLen)
		return 0, ErrClosed
	}

	copy(mf.data[oldSize:newSize], buf)
	return int(writeLen), nil
}

// ReadAt reads len(p) bytes starting at byte offset off. It returns the number of
// bytes copied, which is capped at the current logical size.
func (mf *MFile) ReadAt(buf []byte, off int64) (int, error) {
	if mf.closed.Load() {
		return 0, ErrClosed
	}

	mf.mu.RLock()
	defer mf.mu.RUnlock()

	if mf.closed.Load() {
		return 0, ErrClosed
	}

	size := mf.size.Load()
	if off >= size {
		return 0, fmt.Errorf("offset %d beyond size %d: %w", off, size, ErrBoundsCheck)
	}

	end := off + int64(len(buf))
	if end > size {
		end = size
	}
	copied := copy(buf, mf.data[off:end])
	return copied, nil
}

// ReadFunc calls fn with a slice of the mapped memory from off to off+length.
// The slice is only valid for the duration of fn. This enables zero-copy reads.
func (mf *MFile) ReadFunc(off, length int64, fn func([]byte) error) error {
	if mf.closed.Load() {
		return ErrClosed
	}

	mf.mu.RLock()
	defer mf.mu.RUnlock()

	if mf.closed.Load() {
		return ErrClosed
	}

	size := mf.size.Load()
	end := off + length
	if off < 0 || end > size {
		return fmt.Errorf("read range [%d, %d) beyond size %d: %w", off, end, size, ErrBoundsCheck)
	}

	return fn(mf.data[off:end])
}

// Close unmaps the file and truncates it to the logical size.
// It is safe to call Close multiple times; subsequent calls are no-ops.
func (mf *MFile) Close() error {
	if mf.closed.Swap(true) {
		return nil
	}

	mf.mu.Lock()
	defer mf.mu.Unlock()

	if mf.data != nil {
		if err := syscall.Munmap(mf.data); err != nil {
			return fmt.Errorf("munmap %s: %w", mf.path, err)
		}
		mf.data = nil
	}

	if !mf.readOnly && !mf.deleted.Load() {
		size := mf.size.Load()
		if err := os.Truncate(mf.path, size); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("truncate %s to size %d: %w", mf.path, size, err)
		}
	}

	return nil
}

// Delete marks the file as deleted and removes it from the filesystem.
// The memory mapping remains valid until Close is called.
func (mf *MFile) Delete() {
	if mf.deleted.Swap(true) {
		return
	}
	_ = os.Remove(mf.path) // best-effort removal; file may already be gone
}

// Advise issues an madvise hint for the entire mapped region.
func (mf *MFile) Advise(hint Advice) error {
	if mf.closed.Load() {
		return ErrClosed
	}

	mf.mu.RLock()
	defer mf.mu.RUnlock()

	if mf.data == nil {
		return ErrClosed
	}

	if err := unix.Madvise(mf.data, int(hint)); err != nil {
		return fmt.Errorf("madvise %s hint=%d: %w", mf.path, hint, err)
	}
	return nil
}
