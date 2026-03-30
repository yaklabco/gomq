# GoMQ Phase A Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a high-performance AMQP 0-9-1 message broker in Go, faithfully porting LavinMQ's mmap-based architecture, usable as both a standalone binary and embeddable library.

**Architecture:** Bottom-up construction: storage engine (mmap'd segment files) → AMQP 0-9-1 codec (code-generated from spec XML) → broker core (server, vhosts, exchanges, queues, channels, consumers). Each layer is independently testable. The storage engine uses `syscall.Mmap` for zero-copy persistence. The broker uses goroutine-per-consumer with channel-based signaling.

**Tech Stack:** Go 1.24+, `syscall.Mmap`/`syscall.Munmap` for storage, `encoding/xml` for code generation, `net` for TCP, `sync/atomic` for lock-free counters, no external dependencies for core broker.

**Reference source:** `/Volumes/Code/gomq/lavinmq/src/lavinmq/` (Crystal)

---

## Task 1: Project Scaffold

**Files:**
- Create: `go.mod`
- Create: `cmd/gomq/main.go`
- Create: `config/config.go`
- Create: `config/config_test.go`

**Step 1: Initialize Go module**

Run: `cd /Volumes/Code/gomq && go mod init github.com/yaklabco/gomq`
Expected: `go.mod` created

**Step 2: Write config test**

```go
// config/config_test.go
package config

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := Default()

	if cfg.DataDir != "/var/lib/gomq" {
		t.Errorf("DataDir = %q, want /var/lib/gomq", cfg.DataDir)
	}
	if cfg.Heartbeat != 300*time.Second {
		t.Errorf("Heartbeat = %v, want 300s", cfg.Heartbeat)
	}
	if cfg.FrameMax != 131072 {
		t.Errorf("FrameMax = %d, want 131072", cfg.FrameMax)
	}
	if cfg.ChannelMax != 2048 {
		t.Errorf("ChannelMax = %d, want 2048", cfg.ChannelMax)
	}
	if cfg.MaxMessageSize != 128*1024*1024 {
		t.Errorf("MaxMessageSize = %d, want %d", cfg.MaxMessageSize, 128*1024*1024)
	}
	if cfg.SegmentSize != 8*1024*1024 {
		t.Errorf("SegmentSize = %d, want %d", cfg.SegmentSize, 8*1024*1024)
	}
	if cfg.SocketBufferSize != 16384 {
		t.Errorf("SocketBufferSize = %d, want 16384", cfg.SocketBufferSize)
	}
	if cfg.DefaultConsumerPrefetch != 65535 {
		t.Errorf("DefaultConsumerPrefetch = %d, want 65535", cfg.DefaultConsumerPrefetch)
	}
	if cfg.AMQPPort != 5672 {
		t.Errorf("AMQPPort = %d, want 5672", cfg.AMQPPort)
	}
	if cfg.AMQPBind != "127.0.0.1" {
		t.Errorf("AMQPBind = %q, want 127.0.0.1", cfg.AMQPBind)
	}
}
```

**Step 3: Run test to verify it fails**

Run: `cd /Volumes/Code/gomq && go test ./config/`
Expected: FAIL — package not found

**Step 4: Write config implementation**

```go
// config/config.go
package config

import "time"

type Config struct {
	DataDir                 string
	AMQPBind                string
	AMQPPort                int
	Heartbeat               time.Duration
	FrameMax                uint32
	ChannelMax              uint16
	MaxMessageSize          uint32
	SegmentSize             int64
	SocketBufferSize        int
	DefaultConsumerPrefetch uint16
	TCPNodelay              bool
	TCPKeepalive            bool
	TCPKeepaliveIdle        time.Duration
	TCPKeepaliveInterval    time.Duration
	TCPKeepaliveCount       int
	FreeDiskMin             int64
	SetTimestamp            bool
	DefaultUser             string
	DefaultPasswordHash     string
}

func Default() *Config {
	return &Config{
		DataDir:                 "/var/lib/gomq",
		AMQPBind:                "127.0.0.1",
		AMQPPort:                5672,
		Heartbeat:               300 * time.Second,
		FrameMax:                131072,
		ChannelMax:              2048,
		MaxMessageSize:          128 * 1024 * 1024,
		SegmentSize:             8 * 1024 * 1024,
		SocketBufferSize:        16384,
		DefaultConsumerPrefetch: 65535,
		TCPNodelay:              false,
		TCPKeepalive:            true,
		TCPKeepaliveIdle:        60 * time.Second,
		TCPKeepaliveInterval:    10 * time.Second,
		TCPKeepaliveCount:       3,
		FreeDiskMin:             0,
		SetTimestamp:            false,
		DefaultUser:             "guest",
		DefaultPasswordHash:     "+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+",
	}
}
```

**Step 5: Run test to verify it passes**

Run: `cd /Volumes/Code/gomq && go test ./config/`
Expected: PASS

**Step 6: Write main.go stub**

```go
// cmd/gomq/main.go
package main

import (
	"fmt"
	"os"

	"github.com/yaklabco/gomq/config"
)

func main() {
	cfg := config.Default()
	fmt.Fprintf(os.Stderr, "gomq starting (data_dir=%s, amqp=%s:%d)\n",
		cfg.DataDir, cfg.AMQPBind, cfg.AMQPPort)
}
```

**Step 7: Verify it compiles**

Run: `cd /Volumes/Code/gomq && go build ./cmd/gomq/`
Expected: Binary compiles without errors

**Step 8: Commit**

```
feat: project scaffold with config defaults
```

---

## Task 2: SegmentPosition Value Type

**Files:**
- Create: `storage/segment_position.go`
- Create: `storage/segment_position_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/segment_position.cr`

**Step 1: Write test**

```go
// storage/segment_position_test.go
package storage

import (
	"testing"
)

func TestSegmentPositionComparison(t *testing.T) {
	sp1 := SegmentPosition{Segment: 1, Position: 100, Size: 50}
	sp2 := SegmentPosition{Segment: 1, Position: 200, Size: 50}
	sp3 := SegmentPosition{Segment: 2, Position: 50, Size: 50}

	if sp1.Compare(sp2) >= 0 {
		t.Error("sp1 should be less than sp2")
	}
	if sp2.Compare(sp1) <= 0 {
		t.Error("sp2 should be greater than sp1")
	}
	if sp1.Compare(sp3) >= 0 {
		t.Error("sp1 should be less than sp3 (different segment)")
	}
	if sp1.Compare(sp1) != 0 {
		t.Error("sp1 should equal itself")
	}
}

func TestSegmentPositionEquality(t *testing.T) {
	sp1 := SegmentPosition{Segment: 1, Position: 100, Size: 50}
	sp2 := SegmentPosition{Segment: 1, Position: 100, Size: 99}

	// Equality is based on segment + position only, not size
	if sp1 != sp2 {
		t.Error("sp1 should equal sp2 (same segment and position)")
	}
}

func TestSegmentPositionString(t *testing.T) {
	sp := SegmentPosition{Segment: 1, Position: 42, Size: 100}
	want := "0000000001:0000000042"
	if got := sp.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Volumes/Code/gomq && go test ./storage/`
Expected: FAIL — package not found

**Step 3: Write implementation**

```go
// storage/segment_position.go
package storage

import "fmt"

// SegmentPosition identifies a message within the segment store.
// Equality is based on Segment and Position only.
type SegmentPosition struct {
	Segment  uint32
	Position uint32
	Size     uint32
}

func (sp SegmentPosition) Compare(other SegmentPosition) int {
	if sp.Segment != other.Segment {
		if sp.Segment < other.Segment {
			return -1
		}
		return 1
	}
	if sp.Position != other.Position {
		if sp.Position < other.Position {
			return -1
		}
		return 1
	}
	return 0
}

func (sp SegmentPosition) String() string {
	return fmt.Sprintf("%010d:%010d", sp.Segment, sp.Position)
}
```

**Step 4: Run test to verify it passes**

Run: `cd /Volumes/Code/gomq && go test ./storage/`
Expected: PASS

**Step 5: Commit**

```
feat(storage): add SegmentPosition value type
```

---

## Task 3: MFile — Memory-Mapped File (Unix)

**Files:**
- Create: `storage/mfile.go` (interface + errors)
- Create: `storage/mfile_unix.go` (mmap implementation, `//go:build unix`)
- Create: `storage/mfile_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/mfile.cr`

**Step 1: Write tests**

```go
// storage/mfile_test.go
package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func TestMFileCreateAndWrite(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer mf.Close()

	data := []byte("hello world")
	n, err := mf.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Errorf("Write returned %d, want %d", n, len(data))
	}
	if mf.Size() != int64(len(data)) {
		t.Errorf("Size = %d, want %d", mf.Size(), len(data))
	}
	if mf.Capacity() != 4096 {
		t.Errorf("Capacity = %d, want 4096", mf.Capacity())
	}
}

func TestMFileReadAt(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer mf.Close()

	data := []byte("hello world")
	mf.Write(data)

	buf := make([]byte, 5)
	n, err := mf.ReadAt(buf, 6) // "world"
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("ReadAt returned %d, want 5", n)
	}
	if string(buf) != "world" {
		t.Errorf("ReadAt got %q, want %q", buf, "world")
	}
}

func TestMFileReadFunc(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer mf.Close()

	data := []byte("hello world")
	mf.Write(data)

	var got string
	err = mf.ReadFunc(0, int64(len(data)), func(b []byte) error {
		got = string(b) // copy within callback
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello world" {
		t.Errorf("ReadFunc got %q, want %q", got, "hello world")
	}
}

func TestMFileCloseAndTruncate(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello")
	mf.Write(data)
	mf.Close()

	// After close, file should be truncated to size (5 bytes)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 5 {
		t.Errorf("File size after close = %d, want 5", info.Size())
	}
}

func TestMFileCapacityExceeded(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	mf, err := OpenMFile(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer mf.Close()

	data := make([]byte, 20)
	_, err = mf.Write(data)
	if err != ErrCapacityExceeded {
		t.Errorf("expected ErrCapacityExceeded, got %v", err)
	}
}

func TestMFileWriteAfterClose(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	mf.Close()

	_, err = mf.Write([]byte("test"))
	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestMFileReadOnly(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	// Write some data first
	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	mf.Write([]byte("readonly test"))
	mf.Close()

	// Open read-only (capacity = 0 means read-only)
	mf, err = OpenMFileReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer mf.Close()

	buf := make([]byte, 13)
	n, err := mf.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 13 || string(buf) != "readonly test" {
		t.Errorf("ReadAt got %q, want %q", buf[:n], "readonly test")
	}
}

func TestMFileConcurrentReadWrite(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	mf, err := OpenMFile(path, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer mf.Close()

	// Write initial data
	data := []byte("initial data padding for reads")
	mf.Write(data)

	var wg sync.WaitGroup
	// Concurrent readers
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 7)
			for range 1000 {
				mf.ReadAt(buf, 0)
			}
		}()
	}
	// Concurrent writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		small := []byte("x")
		for range 1000 {
			mf.Write(small)
		}
	}()
	wg.Wait()
}

func TestMFileDelete(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "test.dat")

	mf, err := OpenMFile(path, 4096)
	if err != nil {
		t.Fatal(err)
	}

	mf.Write([]byte("test"))
	mf.Delete()
	mf.Close()

	// File should be deleted from disk
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("expected file to be deleted")
	}
}
```

**Step 2: Run test to verify it fails**

Run: `cd /Volumes/Code/gomq && go test ./storage/ -v`
Expected: FAIL — types not defined

**Step 3: Write MFile interface and errors**

```go
// storage/mfile.go
package storage

import "errors"

var (
	ErrClosed           = errors.New("mfile: closed")
	ErrCapacityExceeded = errors.New("mfile: capacity exceeded")
	ErrReadOnly         = errors.New("mfile: read only")
	ErrBoundsCheck      = errors.New("mfile: out of bounds")
)

// Advice mirrors madvise hints.
type Advice int

const (
	AdviceNormal     Advice = 0
	AdviceRandom     Advice = 1
	AdviceSequential Advice = 2
	AdviceWillNeed   Advice = 3
	AdviceDontNeed   Advice = 4
)
```

**Step 4: Write unix mmap implementation**

```go
// storage/mfile_unix.go
//go:build unix

package storage

import (
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// MFile is a memory-mapped file with capacity/size semantics.
// Capacity is the preallocated file size on disk.
// Size is the logical end of written data.
// On Close, the file is truncated to Size.
// On crash, the file remains at Capacity size and is recovered on next open.
type MFile struct {
	mu       sync.RWMutex
	data     []byte
	size     int64
	capacity int64
	path     string
	closed   atomic.Bool
	deleted  atomic.Bool
	readOnly bool
}

// OpenMFile creates or opens a memory-mapped file with the given capacity.
// The file is created if it doesn't exist. If the file exists and is smaller
// than capacity, it is expanded. The file is mapped read-write.
func OpenMFile(path string, capacity int64) (*MFile, error) {
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR, 0644)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}

	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		syscall.Close(fd)
		return nil, &os.PathError{Op: "fstat", Path: path, Err: err}
	}

	fileSize := stat.Size
	if capacity < fileSize {
		capacity = fileSize
	}

	if capacity > fileSize {
		if err := syscall.Ftruncate(fd, capacity); err != nil {
			syscall.Close(fd)
			return nil, &os.PathError{Op: "ftruncate", Path: path, Err: err}
		}
	}

	data, err := syscall.Mmap(fd, 0, int(capacity), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	// Close FD after mmap — we don't need it anymore (matching LavinMQ)
	syscall.Close(fd)
	if err != nil {
		return nil, &os.PathError{Op: "mmap", Path: path, Err: err}
	}

	return &MFile{
		data:     data,
		size:     fileSize,
		capacity: capacity,
		path:     path,
	}, nil
}

// OpenMFileReadOnly opens an existing file as read-only.
// Capacity is set to the current file size.
func OpenMFileReadOnly(path string) (*MFile, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}

	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		syscall.Close(fd)
		return nil, &os.PathError{Op: "fstat", Path: path, Err: err}
	}

	fileSize := stat.Size
	if fileSize == 0 {
		syscall.Close(fd)
		return &MFile{
			data:     nil,
			size:     0,
			capacity: 0,
			path:     path,
			readOnly: true,
		}, nil
	}

	data, err := syscall.Mmap(fd, 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	syscall.Close(fd)
	if err != nil {
		return nil, &os.PathError{Op: "mmap", Path: path, Err: err}
	}

	return &MFile{
		data:     data,
		size:     fileSize,
		capacity: fileSize,
		path:     path,
		readOnly: true,
	}, nil
}

func (m *MFile) Path() string    { return m.path }
func (m *MFile) Capacity() int64 { return m.capacity }

func (m *MFile) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

// Write appends data to the file. Append-only — writes always go at Size.
func (m *MFile) Write(p []byte) (int, error) {
	if m.closed.Load() {
		return 0, ErrClosed
	}
	if m.readOnly {
		return 0, ErrReadOnly
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed.Load() {
		return 0, ErrClosed
	}

	size := atomic.LoadInt64(&m.size)
	end := size + int64(len(p))
	if end > m.capacity {
		return 0, ErrCapacityExceeded
	}

	copy(m.data[size:end], p)
	atomic.StoreInt64(&m.size, end)
	return len(p), nil
}

// ReadAt reads len(p) bytes from offset into p. Returns bytes read.
// Copies out of the mmap region — caller's buffer is safe after return.
func (m *MFile) ReadAt(p []byte, off int64) (int, error) {
	if m.closed.Load() {
		return 0, ErrClosed
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed.Load() {
		return 0, ErrClosed
	}

	size := atomic.LoadInt64(&m.size)
	if off < 0 || off >= size {
		return 0, ErrBoundsCheck
	}

	n := int64(len(p))
	if off+n > size {
		n = size - off
	}

	copy(p, m.data[off:off+n])
	return int(n), nil
}

// ReadFunc provides zero-copy access to the mmap'd region under the read lock.
// The slice passed to fn MUST NOT escape the callback.
func (m *MFile) ReadFunc(off, length int64, fn func([]byte) error) error {
	if m.closed.Load() {
		return ErrClosed
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed.Load() {
		return ErrClosed
	}

	size := atomic.LoadInt64(&m.size)
	if off < 0 || off+length > size {
		return ErrBoundsCheck
	}

	return fn(m.data[off : off+length])
}

// Advise issues an madvise hint for the mapped region.
func (m *MFile) Advise(advice Advice) error {
	if m.closed.Load() {
		return ErrClosed
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.data == nil {
		return nil
	}
	return syscall.Madvise(m.data, int(advice))
}

// Flush asynchronously flushes dirty pages to disk.
func (m *MFile) Flush() error {
	if m.closed.Load() {
		return ErrClosed
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.data == nil {
		return nil
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe_pointer(m.data)),
		uintptr(atomic.LoadInt64(&m.size)),
		uintptr(syscall.MS_ASYNC))
	if errno != 0 {
		return errno
	}
	return nil
}

// Delete marks the file for deletion and removes it from disk.
// The mmap region remains valid until Close.
func (m *MFile) Delete() {
	if m.deleted.Swap(true) {
		return // already deleted
	}
	os.Remove(m.path)
}

// Resize sets the logical size without changing the file on disk.
// Used during recovery to mark where valid data ends.
func (m *MFile) Resize(newSize int64) {
	if newSize > m.capacity {
		return
	}
	atomic.StoreInt64(&m.size, newSize)
}

// Close unmaps the file and truncates it to Size on disk.
// If Delete was called, the file is not truncated.
func (m *MFile) Close() error {
	if m.closed.Swap(true) {
		return nil // already closed
	}

	m.mu.Lock()
	data := m.data
	m.data = nil
	m.mu.Unlock()

	if data != nil {
		if err := syscall.Munmap(data); err != nil {
			return &os.PathError{Op: "munmap", Path: m.path, Err: err}
		}
	}

	// Truncate to logical size on graceful close (not if deleted or read-only)
	if !m.deleted.Load() && !m.readOnly {
		size := atomic.LoadInt64(&m.size)
		if err := syscall.Truncate(m.path, size); err != nil {
			// Ignore ENOENT — file may have been deleted by another process
			if !os.IsNotExist(err) {
				return &os.PathError{Op: "truncate", Path: m.path, Err: err}
			}
		}
	}

	return nil
}
```

Note: The `Flush` method uses `unsafe_pointer` — we need to add an import for `unsafe`. Replace the Flush method with a simpler approach using the msync syscall directly. We'll refine this when we encounter the build.

**Step 5: Run tests**

Run: `cd /Volumes/Code/gomq && go test ./storage/ -v -race`
Expected: PASS (all tests including race detector)

**Step 6: Commit**

```
feat(storage): add MFile mmap'd file implementation
```

---

## Task 4: MessageStore — Segment-Based Append Log

**Files:**
- Create: `storage/message.go` (Message and BytesMessage types)
- Create: `storage/message_test.go`
- Create: `storage/message_store.go`
- Create: `storage/message_store_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/message_store.cr`, `/Volumes/Code/gomq/lavinmq/src/lavinmq/message.cr`

**Step 1: Write message serialization tests**

```go
// storage/message_test.go
package storage

import (
	"bytes"
	"testing"
	"time"
)

func TestMessageMarshalRoundTrip(t *testing.T) {
	now := time.Now().UnixMilli()
	msg := &Message{
		Timestamp:    now,
		ExchangeName: "test-exchange",
		RoutingKey:   "test.key",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	var buf bytes.Buffer
	n, err := msg.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	if int(n) != len(data) {
		t.Errorf("WriteTo returned %d, buf has %d bytes", n, len(data))
	}

	got, err := ReadBytesMessage(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Timestamp != now {
		t.Errorf("Timestamp = %d, want %d", got.Timestamp, now)
	}
	if got.ExchangeName != "test-exchange" {
		t.Errorf("ExchangeName = %q, want %q", got.ExchangeName, "test-exchange")
	}
	if got.RoutingKey != "test.key" {
		t.Errorf("RoutingKey = %q, want %q", got.RoutingKey, "test.key")
	}
	if got.BodySize != 5 {
		t.Errorf("BodySize = %d, want 5", got.BodySize)
	}
	if !bytes.Equal(got.Body, []byte("hello")) {
		t.Errorf("Body = %q, want %q", got.Body, "hello")
	}
}

func TestMessageByteSize(t *testing.T) {
	msg := &Message{
		Timestamp:    1234567890,
		ExchangeName: "ex",
		RoutingKey:   "rk",
		BodySize:     3,
		Body:         []byte("abc"),
	}
	// 8 (timestamp) + 1 (ex len) + 2 (ex) + 1 (rk len) + 2 (rk) +
	// 2 (empty properties flags) + 8 (body size) + 3 (body) = 27
	if got := msg.ByteSize(); got != 27 {
		t.Errorf("ByteSize = %d, want 27", got)
	}
}
```

**Step 2: Write message types**

```go
// storage/message.go
package storage

import (
	"encoding/binary"
	"errors"
	"io"
)

// Message is an in-flight message from a publisher.
type Message struct {
	Timestamp    int64
	ExchangeName string
	RoutingKey   string
	Properties   Properties // AMQP content properties (placeholder for now)
	BodySize     uint64
	Body         []byte
}

// Properties is a placeholder for AMQP content properties.
// Will be replaced by the generated codec in the amqp package.
type Properties struct {
	Flags   uint16
	Headers map[string]interface{}
}

func (p *Properties) ByteSize() int {
	return 2 // just the flags field for now
}

func (p *Properties) MarshalTo(buf []byte) int {
	binary.BigEndian.PutUint16(buf, p.Flags)
	return 2
}

func (p *Properties) UnmarshalFrom(buf []byte) (int, error) {
	if len(buf) < 2 {
		return 0, errors.New("properties: too short")
	}
	p.Flags = binary.BigEndian.Uint16(buf)
	return 2, nil
}

// ByteSize returns the total serialized size of the message.
func (m *Message) ByteSize() int {
	return 8 + 1 + len(m.ExchangeName) + 1 + len(m.RoutingKey) +
		m.Properties.ByteSize() + 8 + int(m.BodySize)
}

// WriteTo serializes the message to w in the LavinMQ binary format.
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	size := m.ByteSize()
	buf := make([]byte, size)
	n := m.MarshalTo(buf)
	written, err := w.Write(buf[:n])
	return int64(written), err
}

// MarshalTo writes the message into buf and returns bytes written.
// buf must be at least ByteSize() bytes.
func (m *Message) MarshalTo(buf []byte) int {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.Timestamp))
	pos += 8

	buf[pos] = byte(len(m.ExchangeName))
	pos++
	pos += copy(buf[pos:], m.ExchangeName)

	buf[pos] = byte(len(m.RoutingKey))
	pos++
	pos += copy(buf[pos:], m.RoutingKey)

	pos += m.Properties.MarshalTo(buf[pos:])

	binary.LittleEndian.PutUint64(buf[pos:], m.BodySize)
	pos += 8

	pos += copy(buf[pos:], m.Body)
	return pos
}

// BytesMessage is a message read from the segment store.
// All fields reference the underlying byte slice — no copies.
type BytesMessage struct {
	Timestamp    int64
	ExchangeName string
	RoutingKey   string
	Properties   Properties
	BodySize     uint64
	Body         []byte
}

// ReadBytesMessage deserializes a message from buf.
func ReadBytesMessage(buf []byte) (*BytesMessage, error) {
	if len(buf) < MinMessageSize {
		return nil, errors.New("message: buffer too small")
	}

	pos := 0
	ts := int64(binary.LittleEndian.Uint64(buf[pos:]))
	pos += 8

	exLen := int(buf[pos])
	pos++
	if pos+exLen > len(buf) {
		return nil, errors.New("message: exchange name overflow")
	}
	ex := string(buf[pos : pos+exLen])
	pos += exLen

	rkLen := int(buf[pos])
	pos++
	if pos+rkLen > len(buf) {
		return nil, errors.New("message: routing key overflow")
	}
	rk := string(buf[pos : pos+rkLen])
	pos += rkLen

	var props Properties
	pn, err := props.UnmarshalFrom(buf[pos:])
	if err != nil {
		return nil, err
	}
	pos += pn

	if pos+8 > len(buf) {
		return nil, errors.New("message: body size overflow")
	}
	bodySize := binary.LittleEndian.Uint64(buf[pos:])
	pos += 8

	if pos+int(bodySize) > len(buf) {
		return nil, errors.New("message: body overflow")
	}
	body := buf[pos : pos+int(bodySize)]

	return &BytesMessage{
		Timestamp:    ts,
		ExchangeName: ex,
		RoutingKey:   rk,
		Properties:   props,
		BodySize:     bodySize,
		Body:         body,
	}, nil
}

// SkipMessage returns the total byte size of the message at buf,
// without fully deserializing it. Used for scanning segments.
func SkipMessage(buf []byte) (int, error) {
	if len(buf) < MinMessageSize {
		return 0, errors.New("message: buffer too small")
	}
	pos := 8 // skip timestamp
	exLen := int(buf[pos])
	pos += 1 + exLen
	if pos >= len(buf) {
		return 0, errors.New("message: truncated")
	}
	rkLen := int(buf[pos])
	pos += 1 + rkLen
	if pos+2 > len(buf) {
		return 0, errors.New("message: truncated")
	}
	// Skip properties (just flags for now)
	pos += 2
	if pos+8 > len(buf) {
		return 0, errors.New("message: truncated")
	}
	bodySize := int(binary.LittleEndian.Uint64(buf[pos:]))
	pos += 8 + bodySize
	return pos, nil
}

// MinMessageSize is the smallest valid serialized message.
// 8 (ts) + 1 (ex_len) + 1 (rk_len) + 2 (props flags) + 8 (body_size)
const MinMessageSize = 20
```

**Step 3: Run message tests**

Run: `cd /Volumes/Code/gomq && go test ./storage/ -run TestMessage -v`
Expected: PASS

**Step 4: Write MessageStore tests**

```go
// storage/message_store_test.go
package storage

import (
	"testing"
	"time"
)

func TestMessageStorePushAndShift(t *testing.T) {
	dir := tempDir(t)

	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	msg := &Message{
		Timestamp:    time.Now().UnixMilli(),
		ExchangeName: "test",
		RoutingKey:   "key",
		BodySize:     5,
		Body:         []byte("hello"),
	}

	sp, err := store.Push(msg)
	if err != nil {
		t.Fatal(err)
	}
	if sp.Segment != 1 {
		t.Errorf("Segment = %d, want 1", sp.Segment)
	}
	if store.Len() != 1 {
		t.Errorf("Len = %d, want 1", store.Len())
	}

	env, ok := store.Shift()
	if !ok {
		t.Fatal("Shift returned false")
	}
	if env.Message.ExchangeName != "test" {
		t.Errorf("ExchangeName = %q, want %q", env.Message.ExchangeName, "test")
	}
	if env.Redelivered {
		t.Error("expected Redelivered = false")
	}
}

func TestMessageStoreDelete(t *testing.T) {
	dir := tempDir(t)

	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	msg := &Message{
		Timestamp:    time.Now().UnixMilli(),
		ExchangeName: "test",
		RoutingKey:   "key",
		BodySize:     3,
		Body:         []byte("abc"),
	}

	sp, _ := store.Push(msg)
	store.Shift() // consume it

	err = store.Delete(sp)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMessageStoreMultipleMessages(t *testing.T) {
	dir := tempDir(t)

	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for i := range 10 {
		msg := &Message{
			Timestamp:    time.Now().UnixMilli(),
			ExchangeName: "ex",
			RoutingKey:   "rk",
			BodySize:     1,
			Body:         []byte{byte(i)},
		}
		store.Push(msg)
	}

	if store.Len() != 10 {
		t.Errorf("Len = %d, want 10", store.Len())
	}

	for i := range 10 {
		env, ok := store.Shift()
		if !ok {
			t.Fatalf("Shift %d returned false", i)
		}
		if env.Message.Body[0] != byte(i) {
			t.Errorf("message %d: body = %d, want %d", i, env.Message.Body[0], i)
		}
	}

	if store.Len() != 0 {
		t.Errorf("Len after draining = %d, want 0", store.Len())
	}
}

func TestMessageStoreSegmentRotation(t *testing.T) {
	dir := tempDir(t)

	// Tiny segments to force rotation
	store, err := OpenMessageStore(dir, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Each message is ~27 bytes, so 256 bytes fits ~9 messages
	// Write enough to force at least one segment rotation
	for range 20 {
		msg := &Message{
			Timestamp:    time.Now().UnixMilli(),
			ExchangeName: "ex",
			RoutingKey:   "rk",
			BodySize:     5,
			Body:         []byte("hello"),
		}
		store.Push(msg)
	}

	// Should be able to read all back
	count := 0
	for {
		_, ok := store.Shift()
		if !ok {
			break
		}
		count++
	}
	if count != 20 {
		t.Errorf("read %d messages, want 20", count)
	}
}

func TestMessageStoreReopen(t *testing.T) {
	dir := tempDir(t)

	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}

	msg := &Message{
		Timestamp:    time.Now().UnixMilli(),
		ExchangeName: "persist",
		RoutingKey:   "key",
		BodySize:     4,
		Body:         []byte("data"),
	}

	store.Push(msg)
	store.Close()

	// Reopen
	store2, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	if store2.Len() != 1 {
		t.Errorf("Len after reopen = %d, want 1", store2.Len())
	}

	env, ok := store2.Shift()
	if !ok {
		t.Fatal("Shift returned false after reopen")
	}
	if env.Message.ExchangeName != "persist" {
		t.Errorf("ExchangeName = %q, want %q", env.Message.ExchangeName, "persist")
	}
}

func TestMessageStorePurge(t *testing.T) {
	dir := tempDir(t)

	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for range 5 {
		msg := &Message{
			Timestamp:    time.Now().UnixMilli(),
			ExchangeName: "ex",
			RoutingKey:   "rk",
			BodySize:     1,
			Body:         []byte("x"),
		}
		store.Push(msg)
	}

	purged := store.Purge(3)
	if purged != 3 {
		t.Errorf("Purge returned %d, want 3", purged)
	}
	if store.Len() != 2 {
		t.Errorf("Len after purge = %d, want 2", store.Len())
	}
}

func TestMessageStoreEmpty(t *testing.T) {
	dir := tempDir(t)

	store, err := OpenMessageStore(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	_, ok := store.Shift()
	if ok {
		t.Error("Shift on empty store should return false")
	}
	if !store.Empty() {
		t.Error("Empty should return true")
	}
}
```

**Step 5: Write MessageStore implementation**

The implementation follows LavinMQ's `MessageStore` closely:
- Segments named `msgs.XXXXXXXXXX`, ack files named `acks.XXXXXXXXXX`
- Schema version (uint32) at the start of each segment (first 4 bytes)
- Write pointer tracks current segment + offset
- Read pointer tracks current read position for sequential consumption
- Ack files store deleted positions as uint32s
- Deleted set per segment for binary search skip during reads

```go
// storage/message_store.go
package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const schemaVersion uint32 = 1

// Envelope wraps a message with its position and redelivery status.
type Envelope struct {
	SegmentPosition SegmentPosition
	Message         *BytesMessage
	Redelivered     bool
}

// MessageStore manages segment files for persistent message storage.
type MessageStore struct {
	mu       sync.Mutex
	dir      string
	segSize  int64
	segments map[uint32]*MFile
	acks     map[uint32]*MFile
	deleted  map[uint32][]uint32
	msgCount map[uint32]uint32

	wfileID uint32
	wfile   *MFile
	rfileID uint32
	rfile   *MFile
	rpos    int64 // read position within rfile

	size     uint32
	bytesize uint64
	closed   bool
}

// OpenMessageStore opens or creates a message store in dir.
func OpenMessageStore(dir string, segmentSize int64) (*MessageStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	ms := &MessageStore{
		dir:      dir,
		segSize:  segmentSize,
		segments: make(map[uint32]*MFile),
		acks:     make(map[uint32]*MFile),
		deleted:  make(map[uint32][]uint32),
		msgCount: make(map[uint32]uint32),
	}

	if err := ms.loadSegments(); err != nil {
		return nil, err
	}
	if err := ms.loadAcks(); err != nil {
		return nil, err
	}
	ms.loadStats()

	return ms, nil
}

func (ms *MessageStore) Len() uint32 {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.size
}

func (ms *MessageStore) Empty() bool {
	return ms.Len() == 0
}

func (ms *MessageStore) ByteSize() uint64 {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.bytesize
}

// Push appends a message to the store and returns its position.
func (ms *MessageStore) Push(msg *Message) (SegmentPosition, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed {
		return SegmentPosition{}, ErrClosed
	}

	msgSize := msg.ByteSize()

	// Rotate segment if needed
	if ms.wfile.Size()+int64(msgSize) > ms.wfile.Capacity() {
		if err := ms.openNewSegment(int64(msgSize)); err != nil {
			return SegmentPosition{}, err
		}
	}

	pos := uint32(ms.wfile.Size())
	buf := make([]byte, msgSize)
	msg.MarshalTo(buf)

	if _, err := ms.wfile.Write(buf); err != nil {
		return SegmentPosition{}, err
	}

	ms.msgCount[ms.wfileID]++

	sp := SegmentPosition{
		Segment:  ms.wfileID,
		Position: pos,
		Size:     uint32(msgSize),
	}

	ms.bytesize += uint64(sp.Size)
	ms.size++

	return sp, nil
}

// Shift reads and removes the next message from the read position.
func (ms *MessageStore) Shift() (*Envelope, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed || ms.size == 0 {
		return nil, false
	}

	for {
		rfile := ms.rfile
		seg := ms.rfileID
		pos := ms.rpos

		if pos >= rfile.Size() {
			if !ms.selectNextReadSegment() {
				return nil, false
			}
			continue
		}

		// Read from mmap
		var env *Envelope
		err := rfile.ReadFunc(pos, rfile.Size()-pos, func(data []byte) error {
			if ms.isDeleted(seg, uint32(pos)) {
				skip, err := SkipMessage(data)
				if err != nil {
					return err
				}
				ms.rpos += int64(skip)
				return nil // signal to continue
			}

			bm, err := ReadBytesMessage(data)
			if err != nil {
				return err
			}
			sp := SegmentPosition{
				Segment:  seg,
				Position: uint32(pos),
				Size:     uint32(bm.ByteSize()),
			}
			// Copy body out of mmap region
			body := make([]byte, len(bm.Body))
			copy(body, bm.Body)
			bm.Body = body

			ms.rpos += int64(sp.Size)
			ms.bytesize -= uint64(sp.Size)
			ms.size--
			env = &Envelope{
				SegmentPosition: sp,
				Message:         bm,
				Redelivered:     false,
			}
			return nil
		})

		if err != nil {
			return nil, false
		}
		if env != nil {
			return env, true
		}
		// If env is nil, we skipped a deleted message — loop again
	}
}

// GetMessage reads a message at a specific position.
func (ms *MessageStore) GetMessage(sp SegmentPosition) (*BytesMessage, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	seg, ok := ms.segments[sp.Segment]
	if !ok {
		return nil, fmt.Errorf("segment %d not found", sp.Segment)
	}

	var result *BytesMessage
	err := seg.ReadFunc(int64(sp.Position), int64(sp.Size), func(data []byte) error {
		bm, err := ReadBytesMessage(data)
		if err != nil {
			return err
		}
		body := make([]byte, len(bm.Body))
		copy(body, bm.Body)
		bm.Body = body
		result = bm
		return nil
	})
	return result, err
}

// Delete marks a message as acknowledged. Appends to the ack file.
func (ms *MessageStore) Delete(sp SegmentPosition) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed {
		return ErrClosed
	}

	afile, err := ms.getOrOpenAckFile(sp.Segment)
	if err != nil {
		return err
	}

	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], sp.Position)
	if _, err := afile.Write(buf[:]); err != nil {
		return err
	}

	// Track in-memory deleted set
	del := ms.deleted[sp.Segment]
	idx := sort.Search(len(del), func(i int) bool { return del[i] >= sp.Position })
	if idx < len(del) && del[idx] == sp.Position {
		return nil // already deleted
	}
	ms.deleted[sp.Segment] = append(del, 0)
	copy(ms.deleted[sp.Segment][idx+1:], ms.deleted[sp.Segment][idx:])
	ms.deleted[sp.Segment][idx] = sp.Position

	// Clean up fully acked segments (not the current write segment)
	if sp.Segment != ms.wfileID {
		ackCount := uint32(afile.Size() / 4)
		if ackCount >= ms.msgCount[sp.Segment] {
			ms.deleteSegment(sp.Segment)
		}
	}

	return nil
}

// Purge deletes up to maxCount ready messages.
func (ms *MessageStore) Purge(maxCount int) int {
	count := 0
	for count < maxCount {
		env, ok := ms.Shift()
		if !ok {
			break
		}
		ms.Delete(env.SegmentPosition)
		count++
	}
	return count
}

// Close closes all segment and ack files.
func (ms *MessageStore) Close() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if ms.closed {
		return nil
	}
	ms.closed = true

	for _, f := range ms.segments {
		f.Close()
	}
	for _, f := range ms.acks {
		f.Close()
	}
	return nil
}

func (ms *MessageStore) segmentPath(id uint32) string {
	return filepath.Join(ms.dir, fmt.Sprintf("msgs.%010d", id))
}

func (ms *MessageStore) ackPath(id uint32) string {
	return filepath.Join(ms.dir, fmt.Sprintf("acks.%010d", id))
}

func (ms *MessageStore) isDeleted(seg, pos uint32) bool {
	del, ok := ms.deleted[seg]
	if !ok {
		return false
	}
	idx := sort.Search(len(del), func(i int) bool { return del[i] >= pos })
	return idx < len(del) && del[idx] == pos
}

func (ms *MessageStore) getOrOpenAckFile(seg uint32) (*MFile, error) {
	if f, ok := ms.acks[seg]; ok {
		return f, nil
	}
	path := ms.ackPath(seg)
	// Ack file capacity: segment can hold at most segSize/MinMessageSize messages,
	// each ack entry is 4 bytes
	capacity := (ms.segSize / int64(MinMessageSize)) * 4
	f, err := OpenMFile(path, capacity)
	if err != nil {
		return nil, err
	}
	ms.acks[seg] = f
	return f, nil
}

func (ms *MessageStore) openNewSegment(nextMsgSize int64) error {
	// Truncate current write segment to its actual size
	ms.wfile.Resize(ms.wfile.Size())

	nextID := ms.wfileID + 1
	path := ms.segmentPath(nextID)
	capacity := ms.segSize
	if nextMsgSize+4 > capacity {
		capacity = nextMsgSize + 4
	}

	wfile, err := OpenMFile(path, capacity)
	if err != nil {
		return err
	}

	// Write schema version header
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], schemaVersion)
	if _, err := wfile.Write(header[:]); err != nil {
		return err
	}

	ms.wfileID = nextID
	ms.wfile = wfile
	ms.segments[nextID] = wfile

	return nil
}

func (ms *MessageStore) selectNextReadSegment() bool {
	// Find the next segment ID after current rfileID
	var nextID uint32
	found := false
	for id := range ms.segments {
		if id > ms.rfileID && (!found || id < nextID) {
			nextID = id
			found = true
		}
	}
	if !found {
		return false
	}
	ms.rfileID = nextID
	ms.rfile = ms.segments[nextID]
	ms.rpos = 4 // skip schema version header
	return true
}

func (ms *MessageStore) deleteSegment(seg uint32) {
	if seg == ms.rfileID {
		ms.selectNextReadSegment()
	}
	if f, ok := ms.acks[seg]; ok {
		f.Delete()
		f.Close()
		delete(ms.acks, seg)
	}
	if f, ok := ms.segments[seg]; ok {
		f.Delete()
		f.Close()
		delete(ms.segments, seg)
	}
	delete(ms.msgCount, seg)
	delete(ms.deleted, seg)
}

func (ms *MessageStore) loadSegments() error {
	entries, err := os.ReadDir(ms.dir)
	if err != nil {
		if os.IsNotExist(err) {
			entries = nil
		} else {
			return err
		}
	}

	var ids []uint32
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "msgs.") && len(name) == 15 {
			var id uint32
			if _, err := fmt.Sscanf(name[5:], "%d", &id); err == nil {
				ids = append(ids, id)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if len(ids) == 0 {
		// Create initial segment
		ids = append(ids, 1)
		path := ms.segmentPath(1)
		f, err := OpenMFile(path, ms.segSize)
		if err != nil {
			return err
		}
		var header [4]byte
		binary.LittleEndian.PutUint32(header[:], schemaVersion)
		if _, err := f.Write(header[:]); err != nil {
			return err
		}
		ms.segments[1] = f
		ms.wfileID = 1
		ms.wfile = f
		ms.rfileID = 1
		ms.rfile = f
		ms.rpos = 4
		return nil
	}

	for i, id := range ids {
		path := ms.segmentPath(id)
		var f *MFile
		var err error
		if i == len(ids)-1 {
			// Last segment: expand to full capacity
			f, err = OpenMFile(path, ms.segSize)
		} else {
			// Earlier segments: open read-only at current size
			f, err = OpenMFileReadOnly(path)
		}
		if err != nil {
			return err
		}
		ms.segments[id] = f
	}

	ms.wfileID = ids[len(ids)-1]
	ms.wfile = ms.segments[ms.wfileID]
	ms.rfileID = ids[0]
	ms.rfile = ms.segments[ms.rfileID]
	ms.rpos = 4 // skip schema version header

	return nil
}

func (ms *MessageStore) loadAcks() error {
	entries, err := os.ReadDir(ms.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "acks.") || len(name) != 15 {
			continue
		}

		var seg uint32
		if _, err := fmt.Sscanf(name[5:], "%d", &seg); err != nil {
			continue
		}

		// Check that corresponding segment exists
		if _, ok := ms.segments[seg]; !ok {
			os.Remove(filepath.Join(ms.dir, name))
			continue
		}

		path := filepath.Join(ms.dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var acked []uint32
		for i := 0; i+4 <= len(data); i += 4 {
			pos := binary.LittleEndian.Uint32(data[i:])
			if pos == 0 {
				break // invalid position (0 is schema header)
			}
			acked = append(acked, pos)
		}

		if len(acked) > 0 {
			sort.Slice(acked, func(i, j int) bool { return acked[i] < acked[j] })
			ms.deleted[seg] = acked
		}

		// Open ack file for appending
		capacity := (ms.segSize / int64(MinMessageSize)) * 4
		af, err := OpenMFile(path, capacity)
		if err != nil {
			return err
		}
		ms.acks[seg] = af
	}

	return nil
}

func (ms *MessageStore) loadStats() {
	// Scan each segment to count messages and compute sizes
	for seg, mfile := range ms.segments {
		var count uint32
		pos := int64(4) // skip schema header

		for pos < mfile.Size() {
			var msgSize int
			err := mfile.ReadFunc(pos, mfile.Size()-pos, func(data []byte) error {
				skip, err := SkipMessage(data)
				if err != nil {
					return err
				}
				msgSize = skip
				return nil
			})
			if err != nil {
				break
			}

			count++
			if !ms.isDeleted(seg, uint32(pos)) {
				ms.size++
				ms.bytesize += uint64(msgSize)
			}

			pos += int64(msgSize)
		}

		ms.msgCount[seg] = count
	}
}

func (bm *BytesMessage) ByteSize() int {
	return 8 + 1 + len(bm.ExchangeName) + 1 + len(bm.RoutingKey) +
		bm.Properties.ByteSize() + 8 + int(bm.BodySize)
}
```

**Step 6: Run all storage tests**

Run: `cd /Volumes/Code/gomq && go test ./storage/ -v -race`
Expected: PASS

**Step 7: Commit**

```
feat(storage): add MessageStore with segment-based persistence
```

---

## Task 5: AMQP 0-9-1 Spec XML & Code Generator

**Files:**
- Create: `amqp/spec.xml` (download AMQP 0-9-1 spec)
- Create: `amqp/gen/main.go` (code generator)
- Create: `amqp/constants.go` (generated)
- Create: `amqp/methods.go` (generated)

This is the foundation for all AMQP protocol handling. The generator reads the spec XML and produces Go types.

**Step 1: Download the AMQP 0-9-1 spec XML**

Run: `curl -sL https://www.rabbitmq.com/resources/specs/amqp0-9-1.xml -o /Volumes/Code/gomq/amqp/spec.xml`

If unavailable, the spec can be extracted from the rabbitmq/amqp091-go repository.

**Step 2: Write the code generator**

Create `amqp/gen/main.go` — a Go program that:
1. Parses `spec.xml` using `encoding/xml`
2. Extracts all classes (connection, channel, exchange, queue, basic, tx, confirm)
3. Extracts all methods within each class
4. Extracts all fields within each method
5. Generates `constants.go` with:
   - Frame type constants (METHOD=1, HEADER=2, BODY=3, HEARTBEAT=8)
   - Reply code constants (200, 311, 320, 402, 403, 404, 405, 406, 501-541)
   - Class ID constants
   - Method ID constants (classID<<16 | methodID)
6. Generates `methods.go` with:
   - One struct per method (e.g., `ConnectionStart`, `BasicPublish`)
   - `MethodID() uint32` returning the combined class+method ID
   - `Marshal(buf []byte) (int, error)` for serialization
   - `Unmarshal(buf []byte) error` for deserialization
   - `MethodName() string` for debugging

The generator handles AMQP field types:
- `octet` → `uint8`
- `short` → `uint16`
- `long` → `uint32`
- `longlong` → `uint64`
- `bit` → `bool` (packed into octets)
- `shortstr` → `string` (1-byte length prefix)
- `longstr` → `[]byte` (4-byte length prefix)
- `table` → `Table` (field table, needs custom marshal/unmarshal)
- `timestamp` → `uint64`

**Step 3: Run the generator**

Run: `cd /Volumes/Code/gomq && go run ./amqp/gen/ -spec amqp/spec.xml -out amqp/`
Expected: `amqp/constants.go` and `amqp/methods.go` generated

**Step 4: Write tests for generated code**

```go
// amqp/methods_test.go
package amqp

import "testing"

func TestConnectionStartMarshalRoundTrip(t *testing.T) {
	cs := &ConnectionStart{
		VersionMajor: 0,
		VersionMinor: 9,
		Mechanisms:   []byte("PLAIN"),
		Locales:      []byte("en_US"),
	}

	buf := make([]byte, 1024)
	n, err := cs.Marshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	cs2 := &ConnectionStart{}
	if err := cs2.Unmarshal(buf[:n]); err != nil {
		t.Fatal(err)
	}

	if cs2.VersionMajor != 0 || cs2.VersionMinor != 9 {
		t.Errorf("version = %d.%d, want 0.9", cs2.VersionMajor, cs2.VersionMinor)
	}
	if string(cs2.Mechanisms) != "PLAIN" {
		t.Errorf("mechanisms = %q, want PLAIN", cs2.Mechanisms)
	}
}

func TestBasicPublishMarshal(t *testing.T) {
	bp := &BasicPublish{
		Exchange:   "test-exchange",
		RoutingKey: "test.key",
		Mandatory:  true,
		Immediate:  false,
	}

	buf := make([]byte, 256)
	n, err := bp.Marshal(buf)
	if err != nil {
		t.Fatal(err)
	}

	bp2 := &BasicPublish{}
	if err := bp2.Unmarshal(buf[:n]); err != nil {
		t.Fatal(err)
	}

	if bp2.Exchange != "test-exchange" {
		t.Errorf("exchange = %q, want test-exchange", bp2.Exchange)
	}
	if bp2.RoutingKey != "test.key" {
		t.Errorf("routing_key = %q, want test.key", bp2.RoutingKey)
	}
	if !bp2.Mandatory {
		t.Error("mandatory should be true")
	}
	if bp2.Immediate {
		t.Error("immediate should be false")
	}
}

func TestMethodIDs(t *testing.T) {
	tests := []struct {
		name string
		m    Method
		want uint32
	}{
		{"ConnectionStart", &ConnectionStart{}, 0x000A000A},
		{"ConnectionStartOk", &ConnectionStartOk{}, 0x000A000B},
		{"ConnectionTune", &ConnectionTune{}, 0x000A001E},
		{"ConnectionTuneOk", &ConnectionTuneOk{}, 0x000A001F},
		{"ConnectionOpen", &ConnectionOpen{}, 0x000A0028},
		{"ConnectionOpenOk", &ConnectionOpenOk{}, 0x000A0029},
		{"ConnectionClose", &ConnectionClose{}, 0x000A0032},
		{"ConnectionCloseOk", &ConnectionCloseOk{}, 0x000A0033},
		{"ChannelOpen", &ChannelOpen{}, 0x0014000A},
		{"BasicPublish", &BasicPublish{}, 0x003C0028},
		{"BasicDeliver", &BasicDeliver{}, 0x003C003C},
		{"BasicAck", &BasicAck{}, 0x003C0050},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.m.MethodID(); got != tt.want {
				t.Errorf("MethodID() = 0x%08X, want 0x%08X", got, tt.want)
			}
		})
	}
}
```

**Step 5: Run tests**

Run: `cd /Volumes/Code/gomq && go test ./amqp/ -v`
Expected: PASS

**Step 6: Commit**

```
feat(amqp): code generator and AMQP 0-9-1 method types
```

---

## Task 6: AMQP Frame Reader & Writer

**Files:**
- Create: `amqp/frame.go` (frame types)
- Create: `amqp/reader.go`
- Create: `amqp/writer.go`
- Create: `amqp/table.go` (AMQP field table encoding)
- Create: `amqp/properties.go` (content header properties)
- Create: `amqp/frame_test.go`
- Create: `amqp/table_test.go`
- Create: `amqp/properties_test.go`

Reference: AMQP 0-9-1 spec section 2.3 (Framing)

**Step 1: Write frame round-trip tests**

Tests for: method frames, header frames, body frames, heartbeat frames, field table encoding, properties encoding. Test that Reader/Writer produce identical byte sequences. Test max frame size validation. Test the FrameEnd byte (0xCE).

**Step 2: Implement frame types, reader, writer, table, properties**

Frame wire format:
```
type(1) | channel(2) | size(4) | payload(size) | frame_end(1)
```

**Reader**: reads from `io.Reader` into reusable `[]byte`, returns typed frames.
**Writer**: encodes frames into reusable `[]byte`, writes to `*bufio.Writer`.
**Table**: recursive AMQP field table encoding (`t` bool, `b` int8, ... `F` table, etc.).
**Properties**: 14 basic properties with presence bitfield.

**Step 3: Run tests**

Run: `cd /Volumes/Code/gomq && go test ./amqp/ -v -race`
Expected: PASS

**Step 4: Commit**

```
feat(amqp): frame reader, writer, field tables, and properties
```

---

## Task 7: Auth Package

**Files:**
- Create: `auth/user.go`
- Create: `auth/password.go`
- Create: `auth/store.go`
- Create: `auth/password_test.go`
- Create: `auth/store_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/auth/`

**Step 1: Write password hashing tests**

SHA256 password hashing matching LavinMQ's format. Test that the default `guest` password hash verifies correctly.

**Step 2: Write user store tests**

Test creating users, verifying permissions (configure, read, write per vhost), persisting to disk, reloading.

**Step 3: Implement**

- `Password` type with `Verify(plaintext string) bool`
- `User` struct with name, password hash, tags, permissions per vhost
- `Permission` struct with configure/read/write regex patterns
- `UserStore` backed by JSON file in data dir

**Step 4: Run tests, commit**

```
feat(auth): user store with password hashing and permissions
```

---

## Task 8: Broker — Exchange Interface & Implementations

**Files:**
- Create: `broker/exchange.go`
- Create: `broker/exchange_direct.go`
- Create: `broker/exchange_fanout.go`
- Create: `broker/exchange_topic.go`
- Create: `broker/exchange_headers.go`
- Create: `broker/exchange_test.go`
- Create: `broker/message.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/amqp/exchange/`

**Step 1: Write exchange routing tests**

```go
func TestDirectExchangeRouting(t *testing.T) {
	ex := NewDirectExchange("test.direct", true, false)
	q1 := &Queue{name: "q1"}
	q2 := &Queue{name: "q2"}
	ex.Bind(q1, "key.a", nil)
	ex.Bind(q2, "key.b", nil)

	results := make(map[*Queue]struct{})
	ex.Route(&Message{RoutingKey: "key.a"}, results)
	if _, ok := results[q1]; !ok {
		t.Error("q1 should match key.a")
	}
	if _, ok := results[q2]; ok {
		t.Error("q2 should not match key.a")
	}
}

func TestFanoutExchangeRouting(t *testing.T) { ... }

func TestTopicExchangeRouting(t *testing.T) {
	ex := NewTopicExchange("test.topic", true, false)
	q1 := &Queue{name: "q1"}
	q2 := &Queue{name: "q2"}
	q3 := &Queue{name: "q3"}
	ex.Bind(q1, "stock.#", nil)
	ex.Bind(q2, "stock.*.nyse", nil)
	ex.Bind(q3, "#", nil)

	results := make(map[*Queue]struct{})
	ex.Route(&Message{RoutingKey: "stock.usd.nyse"}, results)
	// q1: stock.# matches stock.usd.nyse ✓
	// q2: stock.*.nyse matches stock.usd.nyse ✓
	// q3: # matches everything ✓
	if len(results) != 3 {
		t.Errorf("expected 3 matches, got %d", len(results))
	}
}

func TestHeadersExchangeRouting(t *testing.T) { ... }
```

**Step 2: Implement exchanges**

Topic exchange uses LavinMQ's compiled segment matcher algorithm:
- Split binding key by `.`
- Each segment is a `StringSegment`, `StarSegment`, or `HashSegment`
- Matching is recursive from left to right
- `#` matches zero or more words
- `*` matches exactly one word

**Step 3: Run tests, commit**

```
feat(broker): exchange implementations (direct, fanout, topic, headers)
```

---

## Task 9: Broker — Queue

**Files:**
- Create: `broker/queue.go`
- Create: `broker/queue_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/amqp/queue/queue.cr`

**Step 1: Write queue tests**

Test publish/consume cycle, message ordering, queue limits (max-length, max-length-bytes), overflow modes (drop-head, reject-publish), TTL expiration, durable vs transient.

**Step 2: Implement**

Queue wraps `storage.MessageStore` and adds:
- Consumer list with signaling channels
- Max-length and max-length-bytes overflow
- Message TTL checking
- Queue expiry timer
- Durable/transient flag
- Auto-delete on last consumer disconnect
- Exclusive queue (one connection only)
- Atomic counters for publish/deliver/ack stats

**Step 3: Run tests, commit**

```
feat(broker): queue with message store, limits, and TTL
```

---

## Task 10: Broker — VHost

**Files:**
- Create: `broker/vhost.go`
- Create: `broker/vhost_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/vhost.cr`

**Step 1: Write vhost tests**

Test exchange/queue declaration, binding, publish routing, definitions persistence and recovery, default exchange setup.

**Step 2: Implement**

VHost manages:
- Exchange map with default exchanges (`""`, `amq.direct`, `amq.fanout`, `amq.topic`, `amq.headers`)
- Queue map with data directories per vhost (SHA1 hash of name)
- Definitions file (append-only log of declare/bind/delete frames)
- Publish routing: lookup exchange, call Route, publish to matched queues
- Connection tracking and limits

**Step 3: Run tests, commit**

```
feat(broker): vhost with exchange/queue management and definitions persistence
```

---

## Task 11: Broker — Connection & Handshake

**Files:**
- Create: `broker/connection.go`
- Create: `broker/connection_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/amqp/connection_factory.cr`, `/Volumes/Code/gomq/lavinmq/src/lavinmq/amqp/client.cr`

**Step 1: Write connection handshake tests**

Use `net.Pipe()` to simulate a client. Test the full handshake sequence:
1. Client sends protocol header `AMQP\x00\x00\x09\x01`
2. Server sends Connection.Start
3. Client sends Connection.StartOk (with PLAIN credentials)
4. Server sends Connection.Tune
5. Client sends Connection.TuneOk
6. Client sends Connection.Open (vhost="/")
7. Server sends Connection.OpenOk

Test invalid protocol header, bad credentials, unknown vhost.

**Step 2: Implement**

Connection handles:
- Protocol header validation
- SASL PLAIN authentication
- Tune negotiation (frame_max, channel_max, heartbeat)
- Vhost selection and permission check
- Read loop goroutine with frame dispatch
- Heartbeat timer (send on first timeout, close on second)
- Write mutex for socket serialization
- Graceful close with Connection.Close/CloseOk

**Step 3: Run tests, commit**

```
feat(broker): AMQP connection handshake and read loop
```

---

## Task 12: Broker — Channel

**Files:**
- Create: `broker/channel.go`
- Create: `broker/channel_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/amqp/channel.cr`

**Step 1: Write channel tests**

Test: open/close channel, exchange declare/delete, queue declare/delete/bind/unbind, basic.publish (multi-frame body accumulation), basic.get, basic.ack/nack/reject, confirm mode, transaction mode.

**Step 2: Implement**

Channel handles:
- Open/Close with channel ID
- Exchange operations → VHost
- Queue operations → VHost
- Publish: StartPublish → AddContent (may span frames) → FinishPublish → VHost.Publish
- Basic.Get: one-shot fetch from queue
- Ack/Nack/Reject: lookup in unacked deque, binary search
- Confirm mode: atomic confirm counter, send Basic.Ack/Nack after publish
- Transaction mode: buffer messages until Tx.Commit
- Unacked tracking: `[]Unack` sorted by delivery tag

**Step 3: Run tests, commit**

```
feat(broker): AMQP channel with publish, consume, and ack
```

---

## Task 13: Broker — Consumer & Delivery Loop

**Files:**
- Create: `broker/consumer.go`
- Create: `broker/consumer_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/amqp/consumer.cr`

**Step 1: Write consumer tests**

Test: consumer creation, delivery of published messages, prefetch limiting, ack releasing prefetch capacity, consumer cancellation, no-ack mode, multiple consumers on same queue (round-robin), consumer priority.

**Step 2: Implement**

Consumer runs a goroutine:
```go
func (c *Consumer) deliverLoop(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        case <-c.capacityReady:
        }
        // Additional waits: queueReady, flowEnabled
        env, ok := c.queue.ConsumeGet(ctx)
        if !ok {
            return
        }
        c.deliver(env)
    }
}
```

Signaling:
- `capacityReady chan struct{}` — signaled when ack brings unacked below prefetch
- Queue signals consumers when new message arrives
- Flow control channel for channel.flow

**Step 3: Run tests, commit**

```
feat(broker): consumer with goroutine delivery loop and prefetch
```

---

## Task 14: Broker — Server (TCP Listener)

**Files:**
- Create: `broker/server.go`
- Create: `broker/server_test.go`

Reference: `/Volumes/Code/gomq/lavinmq/src/lavinmq/server.cr`

**Step 1: Write server tests**

Test: server starts and accepts connections, client can complete handshake, server shuts down gracefully (in-flight connections drain), socket options applied.

**Step 2: Implement**

Server:
- `New(cfg *config.Config) *Server`
- `ListenAndServe(ctx context.Context) error`
- Accept loop: one goroutine per accepted connection
- Set TCP_NODELAY, keepalive, buffer sizes
- Create Connection, run handshake, start read loop
- Graceful shutdown: stop accepting, wait for connections to close

**Step 3: Run tests, commit**

```
feat(broker): TCP server with connection management
```

---

## Task 15: Integration Test — Publish & Consume

**Files:**
- Create: `integration_test.go`

**Step 1: Write end-to-end test**

Use the `amqp091-go` client library to connect to our broker, declare a queue, publish a message, consume it, and verify the body.

```go
func TestPublishConsume(t *testing.T) {
	// Start server on random port
	// Connect with amqp091-go client
	// Declare queue
	// Publish message
	// Consume message
	// Assert body matches
	// Close
}
```

This validates that our frame codec, connection handshake, channel, exchange routing, queue storage, and consumer delivery all work together with a real AMQP client.

**Step 2: Fix any issues found**

**Step 3: Commit**

```
test: end-to-end publish and consume with amqp091-go client
```

---

## Task 16: Integration Test — Publisher Confirms & Transactions

**Files:**
- Modify: `integration_test.go`

**Step 1: Write confirm mode test**

```go
func TestPublisherConfirms(t *testing.T) {
	// Connect, open channel, enable confirm mode
	// Publish message
	// Wait for confirm
	// Assert confirmed
}
```

**Step 2: Write transaction test**

```go
func TestTransactions(t *testing.T) {
	// Connect, open channel, tx.select
	// Publish message (not yet visible)
	// tx.commit
	// Consume message
	// Assert body
}
```

**Step 3: Fix issues, commit**

```
test: publisher confirms and transactions integration tests
```

---

## Task 17: Integration Test — Exchange Types

**Files:**
- Modify: `integration_test.go`

Write integration tests for each exchange type using the amqp091-go client:
- Direct exchange routing
- Fanout exchange routing
- Topic exchange with wildcards
- Headers exchange with all/any matching
- Default exchange (publish to queue name directly)

**Commit:**

```
test: exchange routing integration tests
```

---

## Task 18: Integration Test — Queue Features

**Files:**
- Modify: `integration_test.go`

Test:
- Durable queue survives restart
- Message TTL expiry
- Queue max-length overflow (drop-head)
- Dead-letter exchange routing
- Auto-delete queue
- Exclusive queue

**Commit:**

```
test: queue features integration tests
```

---

## Task 19: Standalone Binary

**Files:**
- Modify: `cmd/gomq/main.go`

Wire up the full server:
- Parse command-line flags for config overrides
- Create data directory
- Initialize UserStore with default guest user
- Create Server
- Start listener
- Handle SIGINT/SIGTERM for graceful shutdown

**Commit:**

```
feat: standalone gomq binary with signal handling
```

---

## Task 20: Embeddable Library API

**Files:**
- Create: `gomq.go` (package-level API)
- Create: `gomq_test.go`

Provide a clean top-level API for embedding:

```go
// gomq.go
package gomq

// Broker is the embeddable message broker.
type Broker struct {
    server *broker.Server
}

func New(opts ...Option) (*Broker, error) { ... }
func (b *Broker) ListenAndServe(ctx context.Context) error { ... }
func (b *Broker) Addr() net.Addr { ... }
func (b *Broker) Close() error { ... }
```

Test: start embedded broker, connect with client, publish/consume, shut down.

**Commit:**

```
feat: embeddable library API
```

---

## Implementation Order Summary

| Task | Component | Dependencies |
|------|-----------|-------------|
| 1 | Project scaffold + config | None |
| 2 | SegmentPosition | None |
| 3 | MFile (mmap) | None |
| 4 | MessageStore | 2, 3 |
| 5 | AMQP codec generator | None |
| 6 | Frame reader/writer | 5 |
| 7 | Auth | None |
| 8 | Exchanges | None (uses broker.Message stub) |
| 9 | Queue | 4, 8 |
| 10 | VHost | 7, 8, 9 |
| 11 | Connection | 6, 10 |
| 12 | Channel | 11 |
| 13 | Consumer | 9, 12 |
| 14 | Server | 11, 12, 13 |
| 15 | Integration: publish/consume | 14 |
| 16 | Integration: confirms/tx | 15 |
| 17 | Integration: exchanges | 15 |
| 18 | Integration: queue features | 15 |
| 19 | Standalone binary | 14 |
| 20 | Embeddable API | 14 |

**Parallelizable groups:**
- Tasks 1-3 + 5 + 7 can all run in parallel (no dependencies)
- Tasks 8 can start alongside 4 and 6
- Tasks 15-18 are sequential integration tests after 14

**Estimated commits:** 20 (one per task)
