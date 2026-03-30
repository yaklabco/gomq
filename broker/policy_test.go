package broker

import (
	"testing"
)

func TestPolicyStore_SetAndList(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	policy := &Policy{
		Name:     "test-policy",
		Pattern:  "^test-.*",
		ApplyTo:  PolicyApplyQueues,
		Priority: 10,
		Definition: map[string]interface{}{
			"max-length": 100,
		},
	}

	if err := ps.SetPolicy(policy.Name, policy); err != nil {
		t.Fatalf("SetPolicy() error: %v", err)
	}

	policies := ps.ListPolicies()
	if len(policies) != 1 {
		t.Fatalf("ListPolicies() len = %d, want 1", len(policies))
	}

	if policies[0].Name != "test-policy" {
		t.Errorf("policy name = %q, want %q", policies[0].Name, "test-policy")
	}
}

func TestPolicyStore_Delete(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	policy := &Policy{
		Name:    "delete-me",
		Pattern: ".*",
		ApplyTo: PolicyApplyAll,
		Definition: map[string]interface{}{
			"message-ttl": 60000,
		},
	}

	if err := ps.SetPolicy(policy.Name, policy); err != nil {
		t.Fatalf("SetPolicy() error: %v", err)
	}

	if err := ps.DeletePolicy("delete-me"); err != nil {
		t.Fatalf("DeletePolicy() error: %v", err)
	}

	if len(ps.ListPolicies()) != 0 {
		t.Error("ListPolicies() should be empty after delete")
	}
}

func TestPolicyStore_DeleteNotFound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	if err := ps.DeletePolicy("nonexistent"); err == nil {
		t.Error("DeletePolicy() expected error for nonexistent policy")
	}
}

func TestPolicyStore_ApplyToQueue(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	policy := &Policy{
		Name:     "max-length-policy",
		Pattern:  "^policy-.*",
		ApplyTo:  PolicyApplyQueues,
		Priority: 5,
		Definition: map[string]interface{}{
			"max-length":           int64(100),
			"dead-letter-exchange": "dlx",
		},
	}

	if err := ps.SetPolicy(policy.Name, policy); err != nil {
		t.Fatalf("SetPolicy() error: %v", err)
	}

	// Create a queue that matches the pattern.
	queue, err := vh.DeclareQueue("policy-test-queue", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	ps.ApplyToQueue(queue)

	// Verify policy was applied by checking the queue's effective arguments.
	args := queue.EffectiveArgs()
	if args == nil {
		t.Fatal("EffectiveArgs() returned nil")
	}

	if v, ok := args["x-max-length"]; !ok || v != int64(100) {
		t.Errorf("x-max-length = %v, want 100", v)
	}

	if v, ok := args["x-dead-letter-exchange"]; !ok || v != "dlx" {
		t.Errorf("x-dead-letter-exchange = %v, want %q", v, "dlx")
	}
}

func TestPolicyStore_ApplyToQueue_NoMatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	policy := &Policy{
		Name:     "limited-policy",
		Pattern:  "^special-.*",
		ApplyTo:  PolicyApplyQueues,
		Priority: 1,
		Definition: map[string]interface{}{
			"max-length": int64(50),
		},
	}

	if err := ps.SetPolicy(policy.Name, policy); err != nil {
		t.Fatalf("SetPolicy() error: %v", err)
	}

	// This queue name does NOT match "^special-.*".
	queue, err := vh.DeclareQueue("other-queue", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	ps.ApplyToQueue(queue)

	// Should have no effective args from policy.
	args := queue.EffectiveArgs()
	if len(args) != 0 {
		t.Errorf("EffectiveArgs() = %v, want empty", args)
	}
}

func TestPolicyStore_HighestPriorityWins(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	// Low priority policy.
	low := &Policy{
		Name:     "low-priority",
		Pattern:  "^queue-.*",
		ApplyTo:  PolicyApplyQueues,
		Priority: 1,
		Definition: map[string]interface{}{
			"max-length": int64(10),
		},
	}

	// High priority policy.
	high := &Policy{
		Name:     "high-priority",
		Pattern:  "^queue-.*",
		ApplyTo:  PolicyApplyQueues,
		Priority: 10,
		Definition: map[string]interface{}{
			"max-length": int64(1000),
		},
	}

	if err := ps.SetPolicy(low.Name, low); err != nil {
		t.Fatalf("SetPolicy(low) error: %v", err)
	}
	if err := ps.SetPolicy(high.Name, high); err != nil {
		t.Fatalf("SetPolicy(high) error: %v", err)
	}

	queue, err := vh.DeclareQueue("queue-priority-test", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	ps.ApplyToQueue(queue)

	args := queue.EffectiveArgs()
	if v, ok := args["x-max-length"]; !ok || v != int64(1000) {
		t.Errorf("x-max-length = %v, want 1000 (high priority)", v)
	}
}

func TestPolicyStore_ApplyToExchanges(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	policy := &Policy{
		Name:     "exchange-only",
		Pattern:  ".*",
		ApplyTo:  PolicyApplyExchanges,
		Priority: 1,
		Definition: map[string]interface{}{
			"max-length": int64(100),
		},
	}

	if err := ps.SetPolicy(policy.Name, policy); err != nil {
		t.Fatalf("SetPolicy() error: %v", err)
	}

	// Should not match queues.
	queue, err := vh.DeclareQueue("exchange-policy-test", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	ps.ApplyToQueue(queue)

	args := queue.EffectiveArgs()
	if len(args) != 0 {
		t.Errorf("EffectiveArgs() = %v, want empty (policy is exchanges-only)", args)
	}
}

func TestPolicyStore_SetReapplies(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	// Create queue first.
	queue, err := vh.DeclareQueue("reapply-test", false, false, false, nil)
	if err != nil {
		t.Fatalf("DeclareQueue() error: %v", err)
	}

	// Set a policy that matches the queue.
	policy := &Policy{
		Name:     "reapply-policy",
		Pattern:  "^reapply-.*",
		ApplyTo:  PolicyApplyQueues,
		Priority: 1,
		Definition: map[string]interface{}{
			"max-length": int64(50),
		},
	}

	if err := ps.SetPolicy(policy.Name, policy); err != nil {
		t.Fatalf("SetPolicy() error: %v", err)
	}

	// Setting the policy should have reapplied to matching queues.
	args := queue.EffectiveArgs()
	if v, ok := args["x-max-length"]; !ok || v != int64(50) {
		t.Errorf("after set: x-max-length = %v, want 50", v)
	}

	// Update the policy with a new value.
	updated := &Policy{
		Name:     "reapply-policy",
		Pattern:  "^reapply-.*",
		ApplyTo:  PolicyApplyQueues,
		Priority: 1,
		Definition: map[string]interface{}{
			"max-length": int64(200),
		},
	}

	if err := ps.SetPolicy(updated.Name, updated); err != nil {
		t.Fatalf("SetPolicy(update) error: %v", err)
	}

	args = queue.EffectiveArgs()
	if v, ok := args["x-max-length"]; !ok || v != int64(200) {
		t.Errorf("after update: x-max-length = %v, want 200", v)
	}
}

func TestPolicy_InvalidPattern(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	vh, err := NewVHost("/", dir)
	if err != nil {
		t.Fatalf("NewVHost() error: %v", err)
	}
	defer vh.Close()

	ps := NewPolicyStore(vh)

	policy := &Policy{
		Name:    "bad-pattern",
		Pattern: "[invalid", // unclosed bracket
		ApplyTo: PolicyApplyQueues,
		Definition: map[string]interface{}{
			"max-length": int64(100),
		},
	}

	if err := ps.SetPolicy(policy.Name, policy); err == nil {
		t.Error("SetPolicy() expected error for invalid regex pattern")
	}
}
