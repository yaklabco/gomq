package cluster

import (
	"context"
	"testing"
	"time"
)

func TestElection_StartsAsLeader(t *testing.T) {
	t.Parallel()

	election := NewElection(nil, "", "node-1")

	elected := make(chan struct{}, 1)
	election.OnElected(func() {
		elected <- struct{}{}
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = election.Start(ctx) //nolint:errcheck // test goroutine
	}()

	select {
	case <-elected:
		// Success.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	if !election.IsLeader() {
		t.Fatal("expected IsLeader() to be true")
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	if election.IsLeader() {
		t.Fatal("expected IsLeader() to be false after cancel")
	}
}

func TestElection_Close(t *testing.T) {
	t.Parallel()

	election := NewElection(nil, "", "node-1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = election.Start(ctx) //nolint:errcheck // test goroutine
	}()

	time.Sleep(50 * time.Millisecond)

	if err := election.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if election.IsLeader() {
		t.Fatal("expected IsLeader() to be false after Close")
	}
}
