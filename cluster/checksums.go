// Package cluster provides leader-follower clustering with file-level
// replication, modelled after LavinMQ's clustering design.
package cluster

import (
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// FileIndex tracks CRC32 checksums for files relative to a data directory.
// It is safe for concurrent use.
type FileIndex struct {
	dataDir string
	files   map[string]uint32 // relative path -> CRC32
	mu      sync.RWMutex
}

// NewFileIndex scans dataDir recursively and computes a CRC32 checksum
// for every regular file found. Paths stored in the index are relative
// to dataDir.
func NewFileIndex(dataDir string) (*FileIndex, error) {
	fi := &FileIndex{
		dataDir: dataDir,
		files:   make(map[string]uint32),
	}

	err := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return fmt.Errorf("compute relative path %s: %w", path, err)
		}

		crc, err := checksumFile(path)
		if err != nil {
			return fmt.Errorf("checksum %s: %w", rel, err)
		}

		fi.files[rel] = crc
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan data dir %s: %w", dataDir, err)
	}

	return fi, nil
}

// Checksum returns the cached CRC32 for the given relative path and
// whether the path is tracked.
func (fi *FileIndex) Checksum(path string) (uint32, bool) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	crc, ok := fi.files[path]
	return crc, ok
}

// Update recomputes the CRC32 checksum for the given relative path.
func (fi *FileIndex) Update(path string) error {
	abs := filepath.Join(fi.dataDir, path)

	crc, err := checksumFile(abs)
	if err != nil {
		return fmt.Errorf("update checksum %s: %w", path, err)
	}

	fi.mu.Lock()
	fi.files[path] = crc
	fi.mu.Unlock()

	return nil
}

// Remove deletes a path from the index.
func (fi *FileIndex) Remove(path string) {
	fi.mu.Lock()
	delete(fi.files, path)
	fi.mu.Unlock()
}

// Diff compares this index (the leader) against other (the follower)
// and returns which files the leader should send and which the follower
// should delete.
//
// toSend contains paths that are in fi but missing or different in other.
// toDelete contains paths that are in other but not in fi.
func (fi *FileIndex) Diff(other *FileIndex) ([]string, []string) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	var send, del []string

	for path, crc := range fi.files {
		otherCRC, ok := other.files[path]
		if !ok || otherCRC != crc {
			send = append(send, path)
		}
	}

	for path := range other.files {
		if _, ok := fi.files[path]; !ok {
			del = append(del, path)
		}
	}

	return send, del
}

// checksumFile computes the CRC32 (IEEE) checksum of the file at the
// given absolute path.
func checksumFile(path string) (uint32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(data), nil
}
