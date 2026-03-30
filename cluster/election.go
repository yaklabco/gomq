package cluster

import (
	"context"
	"sync/atomic"
)

// Election manages leader state for a single-leader cluster.
// In this simplified mode (no etcd), one node is configured as
// the leader at startup and followers connect to its replication
// port. There is no automatic failover.
type Election struct {
	nodeID   string
	isLeader atomic.Bool

	onElected func()
	onLost    func()
}

// NewElection creates an election instance. In single-leader mode,
// the endpoints parameter is unused.
func NewElection(_ []string, _, nodeID string) *Election {
	return &Election{
		nodeID: nodeID,
	}
}

// OnElected sets the callback invoked when this node becomes leader.
func (e *Election) OnElected(fn func()) {
	e.onElected = fn
}

// OnLost sets the callback invoked when this node loses leadership.
func (e *Election) OnLost(fn func()) {
	e.onLost = fn
}

// Start marks this node as leader and calls the onElected callback.
// In single-leader mode, the node is always the leader once Start
// is called. It blocks until the context is cancelled.
func (e *Election) Start(ctx context.Context) error {
	e.isLeader.Store(true)

	if e.onElected != nil {
		e.onElected()
	}

	<-ctx.Done()

	e.isLeader.Store(false)

	if e.onLost != nil {
		e.onLost()
	}

	return nil
}

// IsLeader reports whether this node currently holds the leader role.
func (e *Election) IsLeader() bool {
	return e.isLeader.Load()
}

// Close releases the leader role.
func (e *Election) Close() error {
	e.isLeader.Store(false)
	return nil
}
