package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
)

// Follower connects to a leader's replication server and applies
// file-level actions to bring the local data directory into sync.
type Follower struct {
	leaderAddr string
	dataDir    string
	password   string
	fileIndex  *FileIndex
}

// NewFollower creates a follower that will replicate from the leader
// at leaderAddr into the local dataDir.
func NewFollower(leaderAddr, dataDir, password string) *Follower {
	return &Follower{
		leaderAddr: leaderAddr,
		dataDir:    dataDir,
		password:   password,
	}
}

// Connect establishes a TCP connection to the leader, performs the
// protocol handshake and authentication, and builds the local file
// index.
func (f *Follower) Connect(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", f.leaderAddr)
	if err != nil {
		return nil, fmt.Errorf("dial leader %s: %w", f.leaderAddr, err)
	}

	if err := f.handshake(conn); err != nil {
		_ = conn.Close() //nolint:errcheck // cleanup after handshake failure
		return nil, fmt.Errorf("handshake with leader: %w", err)
	}

	fi, err := NewFileIndex(f.dataDir)
	if err != nil {
		_ = conn.Close() //nolint:errcheck // cleanup after index failure
		return nil, fmt.Errorf("build file index: %w", err)
	}
	f.fileIndex = fi

	return conn, nil
}

// StreamActions reads replication actions from the connection and
// applies them to the local data directory until the context is
// cancelled or the connection is closed.
func (f *Follower) StreamActions(ctx context.Context, conn net.Conn) error {
	go func() {
		<-ctx.Done()
		_ = conn.Close() //nolint:errcheck // unblock reads on cancellation
	}()

	for {
		actions, err := readActions(conn)
		if err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // graceful shutdown
			}
			return fmt.Errorf("read actions from leader: %w", err)
		}

		for _, action := range actions {
			if err := f.applyAction(action); err != nil {
				return fmt.Errorf("apply action %q on %s: %w", action.Type, action.Path, err)
			}
		}
	}
}

// handshake sends the protocol header and password to the leader.
func (f *Follower) handshake(conn net.Conn) error {
	// Send protocol header.
	if _, err := conn.Write(protocolHeader[:]); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Send password length + password.
	pw := []byte(f.password)
	if err := binary.Write(conn, binary.LittleEndian, uint8(len(pw))); err != nil {
		return fmt.Errorf("write password length: %w", err)
	}
	if _, err := conn.Write(pw); err != nil {
		return fmt.Errorf("write password: %w", err)
	}

	// Read auth response.
	var resp [1]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}
	if resp[0] != 0 {
		return errAuthRejected
	}

	return nil
}

// applyAction writes a single replication action to the local filesystem.
func (f *Follower) applyAction(action Action) error {
	absPath := filepath.Join(f.dataDir, action.Path)

	switch action.Type {
	case ActionAppend:
		if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
			return fmt.Errorf("mkdir for append %s: %w", action.Path, err)
		}

		file, err := os.OpenFile(absPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("open for append %s: %w", action.Path, err)
		}

		_, writeErr := file.Write(action.Data)
		closeErr := file.Close()

		if writeErr != nil {
			return fmt.Errorf("append to %s: %w", action.Path, writeErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close after append %s: %w", action.Path, closeErr)
		}

	case ActionReplace:
		if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
			return fmt.Errorf("mkdir for replace %s: %w", action.Path, err)
		}

		if err := os.WriteFile(absPath, action.Data, 0o600); err != nil {
			return fmt.Errorf("replace %s: %w", action.Path, err)
		}

	case ActionDelete:
		if err := os.Remove(absPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("delete %s: %w", action.Path, err)
		}

	default:
		return fmt.Errorf("unknown action type %d", action.Type)
	}

	// Update the file index.
	if action.Type == ActionDelete {
		f.fileIndex.Remove(action.Path)
	} else {
		if err := f.fileIndex.Update(action.Path); err != nil {
			return fmt.Errorf("update index for %s: %w", action.Path, err)
		}
	}

	return nil
}

// readActions reads a single batch of encoded actions from the
// connection.
func readActions(conn net.Conn) ([]Action, error) {
	// Read the 4-byte action count to determine how much to read.
	var countBuf [actionCountSize]byte
	if _, err := io.ReadFull(conn, countBuf[:]); err != nil {
		return nil, fmt.Errorf("read action count: %w", err)
	}

	count := binary.LittleEndian.Uint32(countBuf[:])
	if count == 0 {
		return nil, nil
	}

	// Read the rest of the batch. We reconstruct the full message
	// by reading each action's components individually to avoid
	// needing to know the total size upfront.
	//
	// Re-encode with the count prefix for DecodeActions.
	buf := make([]byte, 0, actionCountSize+int(count)*64)
	buf = append(buf, countBuf[:]...)

	for range count {
		// Type (1 byte).
		typeBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, typeBuf); err != nil {
			return nil, fmt.Errorf("read action type: %w", err)
		}
		buf = append(buf, typeBuf...)

		// Path length (4 bytes).
		pathLenBuf := make([]byte, actionCountSize)
		if _, err := io.ReadFull(conn, pathLenBuf); err != nil {
			return nil, fmt.Errorf("read path length: %w", err)
		}
		buf = append(buf, pathLenBuf...)
		pathLen := binary.LittleEndian.Uint32(pathLenBuf)

		// Path.
		pathBuf := make([]byte, pathLen)
		if _, err := io.ReadFull(conn, pathBuf); err != nil {
			return nil, fmt.Errorf("read path: %w", err)
		}
		buf = append(buf, pathBuf...)

		// Data length (4 bytes).
		dataLenBuf := make([]byte, actionCountSize)
		if _, err := io.ReadFull(conn, dataLenBuf); err != nil {
			return nil, fmt.Errorf("read data length: %w", err)
		}
		buf = append(buf, dataLenBuf...)
		dataLen := binary.LittleEndian.Uint32(dataLenBuf)

		// Data.
		if dataLen > 0 {
			dataBuf := make([]byte, dataLen)
			if _, err := io.ReadFull(conn, dataBuf); err != nil {
				return nil, fmt.Errorf("read data: %w", err)
			}
			buf = append(buf, dataBuf...)
		}
	}

	return DecodeActions(buf)
}
