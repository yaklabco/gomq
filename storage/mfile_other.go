//go:build !unix

package storage

import "fmt"

// OpenMFile is not supported on non-Unix platforms.
func OpenMFile(path string, capacity int64) (*MFile, error) {
	return nil, fmt.Errorf("mmap not supported on this platform")
}

// OpenMFileReadOnly is not supported on non-Unix platforms.
func OpenMFileReadOnly(path string) (*MFile, error) {
	return nil, fmt.Errorf("mmap not supported on this platform")
}
