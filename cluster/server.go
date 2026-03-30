package cluster

import (
	"context"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// actionChannelSize is the buffer size for the action broadcast channel.
const actionChannelSize = 256

// protocolHeader is the magic bytes sent by followers to identify the
// GoMQ replication protocol. Version 1.0.0.
//
//nolint:gochecknoglobals // protocol constant, effectively immutable
var protocolHeader = [8]byte{'G', 'O', 'M', 'Q', 'R', 0x01, 0x00, 0x00}

var (
	errInvalidHeader = errors.New("invalid protocol header")
	errAuthFailed    = errors.New("authentication failed")
	errAuthRejected  = errors.New("authentication rejected by leader")
)

// ReplicationServer accepts follower connections and broadcasts
// file-level replication actions. It is the leader-side component
// of the clustering protocol.
type ReplicationServer struct {
	listener  net.Listener
	dataDir   string
	password  string
	fileIndex *FileIndex

	followers map[*FollowerConn]struct{}
	actions   chan Action
	mu        sync.RWMutex
	done      chan struct{}
}

// FollowerConn represents a connected follower.
type FollowerConn struct {
	conn net.Conn
	mu   sync.Mutex // serialises writes to conn
}

// NewReplicationServer creates a replication server that listens on
// bind:port. The dataDir is used for full-sync file serving and the
// password authenticates followers.
func NewReplicationServer(ctx context.Context, bind string, port int, dataDir, password string) (*ReplicationServer, error) {
	addr := fmt.Sprintf("%s:%d", bind, port)

	lc := net.ListenConfig{}

	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	fi, err := NewFileIndex(dataDir)
	if err != nil {
		_ = listener.Close() //nolint:errcheck // cleanup on init failure
		return nil, fmt.Errorf("build file index: %w", err)
	}

	return &ReplicationServer{
		listener:  listener,
		dataDir:   dataDir,
		password:  password,
		fileIndex: fi,
		followers: make(map[*FollowerConn]struct{}),
		actions:   make(chan Action, actionChannelSize),
		done:      make(chan struct{}),
	}, nil
}

// Addr returns the listener address. Useful when port 0 is used.
func (rs *ReplicationServer) Addr() net.Addr {
	return rs.listener.Addr()
}

// Start accepts follower connections and broadcasts actions until the
// context is cancelled.
func (rs *ReplicationServer) Start(ctx context.Context) error {
	go rs.broadcastLoop(ctx)

	go func() {
		<-ctx.Done()
		_ = rs.listener.Close() //nolint:errcheck // unblocks Accept
	}()

	for {
		conn, err := rs.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				close(rs.done)
				return nil //nolint:nilerr // graceful shutdown
			}
			return fmt.Errorf("accept follower: %w", err)
		}

		go rs.handleFollower(ctx, conn)
	}
}

// BroadcastAction enqueues an action for delivery to all connected
// followers. Non-blocking: if the action channel is full, the action
// is dropped (followers will re-sync on reconnect).
func (rs *ReplicationServer) BroadcastAction(action Action) {
	select {
	case rs.actions <- action:
	default:
		// Channel full; follower will resync on reconnect.
	}
}

// Close shuts down the server and disconnects all followers.
func (rs *ReplicationServer) Close() error {
	err := rs.listener.Close()

	rs.mu.Lock()
	for fc := range rs.followers {
		_ = fc.conn.Close() //nolint:errcheck // best-effort cleanup of follower connections
	}
	clear(rs.followers)
	rs.mu.Unlock()

	return err
}

// handleFollower performs the handshake with a single follower, then
// registers it for action broadcast.
func (rs *ReplicationServer) handleFollower(_ context.Context, conn net.Conn) {
	defer conn.Close()

	if err := rs.negotiate(conn); err != nil {
		return
	}

	fc := &FollowerConn{conn: conn}

	rs.mu.Lock()
	rs.followers[fc] = struct{}{}
	rs.mu.Unlock()

	defer func() {
		rs.mu.Lock()
		delete(rs.followers, fc)
		rs.mu.Unlock()
	}()

	// Keep connection open until it errors or closes.
	buf := make([]byte, 1)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
	}
}

// negotiate validates the protocol header and password from a follower.
func (rs *ReplicationServer) negotiate(conn net.Conn) error {
	// Read protocol header.
	var header [8]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if header != protocolHeader {
		return errInvalidHeader
	}

	// Read password length and password.
	var pwLen uint8
	if err := binary.Read(conn, binary.LittleEndian, &pwLen); err != nil {
		return fmt.Errorf("read password length: %w", err)
	}

	pw := make([]byte, pwLen)
	if _, err := io.ReadFull(conn, pw); err != nil {
		return fmt.Errorf("read password: %w", err)
	}

	if subtle.ConstantTimeCompare([]byte(rs.password), pw) != 1 {
		if _, writeErr := conn.Write([]byte{1}); writeErr != nil {
			return fmt.Errorf("write auth failure: %w", writeErr)
		}
		return errAuthFailed
	}

	if _, err := conn.Write([]byte{0}); err != nil {
		return fmt.Errorf("write auth success: %w", err)
	}

	return nil
}

// broadcastLoop reads actions from the channel and sends them to all
// connected followers.
func (rs *ReplicationServer) broadcastLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case action := <-rs.actions:
			rs.sendToFollowers(action)
		}
	}
}

// sendToFollowers encodes the action and writes it to every connected
// follower, removing any that fail.
func (rs *ReplicationServer) sendToFollowers(action Action) {
	encoded, err := EncodeActions([]Action{action})
	if err != nil {
		return
	}

	rs.mu.RLock()
	followers := make([]*FollowerConn, 0, len(rs.followers))
	for fc := range rs.followers {
		followers = append(followers, fc)
	}
	rs.mu.RUnlock()

	var failed []*FollowerConn

	for _, fc := range followers {
		fc.mu.Lock()
		_, writeErr := fc.conn.Write(encoded)
		fc.mu.Unlock()

		if writeErr != nil {
			failed = append(failed, fc)
		}
	}

	if len(failed) > 0 {
		rs.mu.Lock()
		for _, fc := range failed {
			delete(rs.followers, fc)
			_ = fc.conn.Close() //nolint:errcheck // best-effort cleanup of failed follower
		}
		rs.mu.Unlock()
	}
}
