package broker

import (
	"errors"
	"testing"
)

func TestVHost_MaxQueuesLimit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	vh.SetLimits(0, 2) // max 2 queues

	// First two queues should succeed.
	if _, err := vh.DeclareQueue("q1", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q1) error: %v", err)
	}
	if _, err := vh.DeclareQueue("q2", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q2) error: %v", err)
	}

	// Third queue should fail.
	_, err = vh.DeclareQueue("q3", false, false, false, nil)
	if err == nil {
		t.Fatal("DeclareQueue(q3) expected error, got nil")
	}
	if !errors.Is(err, ErrMaxQueuesReached) {
		t.Errorf("DeclareQueue(q3) error = %v, want %v", err, ErrMaxQueuesReached)
	}
}

func TestVHost_MaxQueuesLimit_RedeclareAllowed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	vh.SetLimits(0, 1)

	if _, err := vh.DeclareQueue("q1", false, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q1) error: %v", err)
	}

	// Re-declaring the same queue should succeed (idempotent).
	if _, err := vh.DeclareQueue("q1", false, false, false, nil); err != nil {
		t.Errorf("DeclareQueue(q1 again) error: %v, want nil (idempotent)", err)
	}
}

func TestVHost_MaxQueuesLimit_Unlimited(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	// Default: no limit.
	for i := range 100 {
		name := "q" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if _, err := vh.DeclareQueue(name, false, false, false, nil); err != nil {
			t.Fatalf("DeclareQueue(%q) error: %v", name, err)
		}
	}
}

func TestVHost_ConnectionLimit(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	vh.SetLimits(5, 0) // max 5 connections

	// Below limit: should be allowed.
	if err := vh.CheckConnectionLimit(4); err != nil {
		t.Errorf("CheckConnectionLimit(4) error = %v, want nil", err)
	}

	// At limit: should be rejected.
	err = vh.CheckConnectionLimit(5)
	if err == nil {
		t.Fatal("CheckConnectionLimit(5) expected error, got nil")
	}
	if !errors.Is(err, ErrMaxConnsReached) {
		t.Errorf("CheckConnectionLimit(5) error = %v, want %v", err, ErrMaxConnsReached)
	}

	// Above limit: should be rejected.
	err = vh.CheckConnectionLimit(10)
	if err == nil {
		t.Fatal("CheckConnectionLimit(10) expected error, got nil")
	}
}

func TestVHost_ConnectionLimit_Unlimited(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	// Default: no limit.
	if err := vh.CheckConnectionLimit(10000); err != nil {
		t.Errorf("CheckConnectionLimit(10000) error = %v, want nil (unlimited)", err)
	}
}

func TestVHost_SetAndGetLimits(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	vh.SetLimits(100, 50)

	maxConn, maxQ := vh.Limits()
	if maxConn != 100 {
		t.Errorf("maxConnections = %d, want 100", maxConn)
	}
	if maxQ != 50 {
		t.Errorf("maxQueues = %d, want 50", maxQ)
	}

	// Clear limits.
	vh.SetLimits(0, 0)
	maxConn, maxQ = vh.Limits()
	if maxConn != 0 || maxQ != 0 {
		t.Errorf("after clear: maxConnections=%d, maxQueues=%d, want 0,0", maxConn, maxQ)
	}
}
