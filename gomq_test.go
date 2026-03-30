package gomq_test

import (
	"context"
	"net"
	"testing"

	"github.com/jamesainslie/gomq"
)

func TestEmbeddedBroker(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	brk, err := gomq.New(gomq.WithDataDir(dir), gomq.WithHTTPPort(-1), gomq.WithMQTTPort(-1))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	// Addr should be nil before serving.
	if brk.Addr() != nil {
		t.Errorf("Addr() before serve = %v, want nil", brk.Addr())
	}

	// Start on a random port using an existing listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- brk.Serve(ctx, ln)
	}()

	// Wait for the broker to be ready.
	addr := brk.WaitForAddr(ctx)
	if addr == nil {
		t.Fatal("WaitForAddr() = nil, want non-nil")
	}

	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		t.Fatalf("Addr() type = %T, want *net.TCPAddr", addr)
	}
	if tcpAddr.Port == 0 {
		t.Error("Addr().Port = 0, want non-zero")
	}

	// Close the broker.
	if err := brk.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	cancel()

	// Serve should return without error on graceful shutdown.
	if err := <-serveErr; err != nil {
		t.Errorf("Serve() error: %v", err)
	}
}

func TestEmbeddedBroker_ListenAndServe(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	brk, err := gomq.New(
		gomq.WithDataDir(dir),
		gomq.WithBind("127.0.0.1"),
		gomq.WithAMQPPort(0),
		gomq.WithHTTPPort(-1),
		gomq.WithMQTTPort(-1),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- brk.ListenAndServe(ctx)
	}()

	// Wait for the broker to start listening.
	addr := brk.WaitForAddr(ctx)
	if addr == nil {
		t.Fatal("WaitForAddr() = nil, want non-nil")
	}

	if err := brk.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	cancel()

	if err := <-serveErr; err != nil {
		t.Errorf("ListenAndServe() error: %v", err)
	}
}
