package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReplicationServer_StartAndClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "test.dat"), []byte("data"))

	ctx, cancel := context.WithCancel(context.Background())

	rs, err := NewReplicationServer(ctx, "127.0.0.1", 0, dir, "secret")
	if err != nil {
		t.Fatalf("NewReplicationServer: %v", err)
	}
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- rs.Start(ctx)
	}()

	// Give the server time to start accepting.
	time.Sleep(50 * time.Millisecond)

	if rs.Addr() == nil {
		t.Fatal("expected non-nil address")
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestReplicationServer_FollowerConnectsAndReceivesActions(t *testing.T) {
	t.Parallel()

	leaderDir := t.TempDir()
	followerDir := t.TempDir()
	writeFile(t, filepath.Join(leaderDir, "init.dat"), []byte("initial"))

	password := "test-password"

	ctx, cancel := context.WithCancel(context.Background())

	rs, err := NewReplicationServer(ctx, "127.0.0.1", 0, leaderDir, password)
	if err != nil {
		t.Fatalf("NewReplicationServer: %v", err)
	}
	defer cancel()

	go func() {
		_ = rs.Start(ctx) //nolint:errcheck // test goroutine
	}()

	// Wait for listener to be ready.
	time.Sleep(50 * time.Millisecond)

	follower := NewFollower(rs.Addr().String(), followerDir, password)

	conn, err := follower.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Start streaming in a goroutine.
	streamDone := make(chan error, 1)
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	go func() {
		streamDone <- follower.StreamActions(streamCtx, conn)
	}()

	// Give time for follower to register with server.
	time.Sleep(50 * time.Millisecond)

	// Broadcast a replace action.
	rs.BroadcastAction(Action{
		Type: ActionReplace,
		Path: "replicated.dat",
		Data: []byte("hello from leader"),
	})

	// Wait for the follower to apply the action.
	deadline := time.After(5 * time.Second)
	replPath := filepath.Join(followerDir, "replicated.dat")

	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for replicated file")
		default:
		}

		data, readErr := os.ReadFile(replPath)
		if readErr == nil {
			if string(data) != "hello from leader" {
				t.Fatalf("replicated file content = %q, want %q", data, "hello from leader")
			}

			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	// Broadcast a delete action.
	rs.BroadcastAction(Action{
		Type: ActionDelete,
		Path: "replicated.dat",
	})

	// Wait for delete.
	deadline = time.After(5 * time.Second)

	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for file deletion")
		default:
		}

		if _, err := os.Stat(replPath); os.IsNotExist(err) {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	streamCancel()

	select {
	case err := <-streamDone:
		if err != nil {
			t.Fatalf("StreamActions returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StreamActions did not return after cancel")
	}

	cancel()
}

func TestReplicationServer_RejectsWrongPassword(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())

	rs, err := NewReplicationServer(ctx, "127.0.0.1", 0, dir, "correct")
	if err != nil {
		t.Fatalf("NewReplicationServer: %v", err)
	}
	defer cancel()

	go func() {
		_ = rs.Start(ctx) //nolint:errcheck // test goroutine
	}()

	time.Sleep(50 * time.Millisecond)

	follower := NewFollower(rs.Addr().String(), t.TempDir(), "wrong")

	_, err = follower.Connect(ctx)
	if err == nil {
		t.Fatal("expected error for wrong password")
	}

	cancel()
}
