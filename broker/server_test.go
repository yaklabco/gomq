package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/yaklabco/gomq/amqp"
	"github.com/yaklabco/gomq/config"
)

func TestServer_StartAndShutdown(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Heartbeat = 0

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}

	// Use a random port via a pre-created listener.
	listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		t.Fatalf("net.Listen() error: %v", listenErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ctx, listener)
	}()

	// Verify we can get the address.
	addr := listener.Addr()
	if addr == nil {
		t.Fatal("listener Addr() is nil")
	}

	// Stop the server.
	cancel()

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve() did not return after cancel")
	}

	if closeErr := srv.Close(); closeErr != nil {
		t.Fatalf("Close() error: %v", closeErr)
	}
}

func TestServer_AcceptAndHandshake(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Heartbeat = 0

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}

	listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		t.Fatalf("net.Listen() error: %v", listenErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ctx, listener)
	}()

	// Connect as a client.
	clientConn, dialErr := net.DialTimeout("tcp", listener.Addr().String(), 2*time.Second)
	if dialErr != nil {
		t.Fatalf("dial error: %v", dialErr)
	}
	defer func() { _ = clientConn.Close() }()

	if err := clientConn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	// Perform client-side handshake.
	doClientHandshake(t, clientConn)

	// Open a channel to verify the connection is fully operational.
	writer := amqp.NewWriter(clientConn, int(cfg.FrameMax))
	reader := amqp.NewReader(clientConn, cfg.FrameMax)

	if err := writer.WriteMethod(1, &amqp.ChannelOpen{}); err != nil {
		t.Fatalf("write ChannelOpen: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush ChannelOpen: %v", err)
	}

	expectFrameMethod(t, reader, "ChannelOpenOk", func(method amqp.Method) bool {
		_, match := method.(*amqp.ChannelOpenOk)
		return match
	})

	// Clean shutdown.
	cancel()

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve() did not return after cancel")
	}

	if closeErr := srv.Close(); closeErr != nil {
		t.Fatalf("Close() error: %v", closeErr)
	}
}

func TestServer_GracefulClose(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Heartbeat = 0

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}

	listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
	if listenErr != nil {
		t.Fatalf("net.Listen() error: %v", listenErr)
	}

	ctx, cancel := context.WithCancel(context.Background())

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ctx, listener)
	}()

	// Connect two clients sequentially.
	clients := make([]net.Conn, 0, 2)
	for range 2 {
		conn, dialErr := net.DialTimeout("tcp", listener.Addr().String(), 2*time.Second)
		if dialErr != nil {
			t.Fatalf("dial error: %v", dialErr)
		}
		clients = append(clients, conn)
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatalf("set deadline: %v", err)
		}
		doClientHandshake(t, conn)
	}

	// Wait for both connections to be registered.
	deadline := time.After(5 * time.Second)
	for {
		srv.mu.Lock()
		connCount := len(srv.conns)
		srv.mu.Unlock()
		if connCount >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: connection count = %d, want 2", connCount)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	_ = clients // keep alive

	cancel()

	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("Serve() error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve() did not return after cancel")
	}

	if closeErr := srv.Close(); closeErr != nil {
		t.Fatalf("Close() error: %v", closeErr)
	}
}
