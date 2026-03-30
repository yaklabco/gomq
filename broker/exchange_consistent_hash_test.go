package broker

import (
	"fmt"
	"testing"
)

func TestConsistentHashExchange_Type(t *testing.T) {
	t.Parallel()

	exchange := NewConsistentHashExchange("ch-test", false, false)

	if got := exchange.Type(); got != ExchangeConsistentHash {
		t.Errorf("Type() = %q, want %q", got, ExchangeConsistentHash)
	}
}

func TestConsistentHashExchange_DistributeAcrossQueues(t *testing.T) {
	t.Parallel()

	exchange := NewConsistentHashExchange("ch-dist", false, false)
	dest1 := &testDest{name: "queue-1"}
	dest2 := &testDest{name: "queue-2"}
	dest3 := &testDest{name: "queue-3"}

	// Bind with weight "1" (routing key is the weight for consistent hash).
	for _, dest := range []*testDest{dest1, dest2, dest3} {
		if err := exchange.Bind(dest, "1", nil); err != nil {
			t.Fatalf("Bind(%s) error: %v", dest.Name(), err)
		}
	}

	// Route many messages with different routing keys and verify distribution.
	counts := make(map[string]int)
	for msgIdx := range 1000 {
		results := make(map[Destination]struct{})
		exchange.Route(&Message{RoutingKey: fmt.Sprintf("message-%d", msgIdx)}, results)

		if len(results) != 1 {
			t.Fatalf("expected exactly 1 destination, got %d", len(results))
		}

		for dest := range results {
			counts[dest.Name()]++
		}
	}

	// All 3 queues should receive some messages.
	for _, dest := range []*testDest{dest1, dest2, dest3} {
		if counts[dest.Name()] == 0 {
			t.Errorf("queue %q received 0 messages, expected some distribution", dest.Name())
		}
	}

	t.Logf("distribution: q1=%d, q2=%d, q3=%d", counts["queue-1"], counts["queue-2"], counts["queue-3"])
}

func TestConsistentHashExchange_SameKeyGoesToSameQueue(t *testing.T) {
	t.Parallel()

	exchange := NewConsistentHashExchange("ch-same", false, false)
	dest1 := &testDest{name: "queue-1"}
	dest2 := &testDest{name: "queue-2"}

	if err := exchange.Bind(dest1, "1", nil); err != nil {
		t.Fatalf("Bind(dest1) error: %v", err)
	}
	if err := exchange.Bind(dest2, "1", nil); err != nil {
		t.Fatalf("Bind(dest2) error: %v", err)
	}

	// Same routing key should always go to the same queue.
	var firstDest string
	for range 100 {
		results := make(map[Destination]struct{})
		exchange.Route(&Message{RoutingKey: "stable-key"}, results)

		if len(results) != 1 {
			t.Fatalf("expected 1 destination, got %d", len(results))
		}

		for dest := range results {
			if firstDest == "" {
				firstDest = dest.Name()
			} else if dest.Name() != firstDest {
				t.Fatalf("routing key 'stable-key' went to %q then %q, expected consistent routing", firstDest, dest.Name())
			}
		}
	}
}

func TestConsistentHashExchange_WeightAffectsDistribution(t *testing.T) {
	t.Parallel()

	exchange := NewConsistentHashExchange("ch-weight", false, false)
	heavy := &testDest{name: "heavy-queue"}
	light := &testDest{name: "light-queue"}

	// Heavy queue gets weight 10, light gets weight 1.
	if err := exchange.Bind(heavy, "10", nil); err != nil {
		t.Fatalf("Bind(heavy) error: %v", err)
	}
	if err := exchange.Bind(light, "1", nil); err != nil {
		t.Fatalf("Bind(light) error: %v", err)
	}

	counts := make(map[string]int)
	for msgIdx := range 10000 {
		results := make(map[Destination]struct{})
		exchange.Route(&Message{RoutingKey: fmt.Sprintf("key-%d", msgIdx)}, results)
		for dest := range results {
			counts[dest.Name()]++
		}
	}

	heavyCount := counts["heavy-queue"]
	lightCount := counts["light-queue"]

	// Heavy should get significantly more messages than light.
	if heavyCount <= lightCount {
		t.Errorf("heavy queue (%d) should receive more messages than light queue (%d)", heavyCount, lightCount)
	}

	t.Logf("heavy=%d, light=%d, ratio=%.1f", heavyCount, lightCount, float64(heavyCount)/float64(lightCount))
}

func TestConsistentHashExchange_UnbindRebalances(t *testing.T) {
	t.Parallel()

	exchange := NewConsistentHashExchange("ch-unbind", false, false)
	dest1 := &testDest{name: "queue-1"}
	dest2 := &testDest{name: "queue-2"}

	if err := exchange.Bind(dest1, "1", nil); err != nil {
		t.Fatalf("Bind(dest1) error: %v", err)
	}
	if err := exchange.Bind(dest2, "1", nil); err != nil {
		t.Fatalf("Bind(dest2) error: %v", err)
	}

	// Verify both queues receive messages.
	countsBefore := make(map[string]int)
	for msgIdx := range 100 {
		results := make(map[Destination]struct{})
		exchange.Route(&Message{RoutingKey: fmt.Sprintf("key-%d", msgIdx)}, results)
		for dest := range results {
			countsBefore[dest.Name()]++
		}
	}

	if countsBefore["queue-1"] == 0 || countsBefore["queue-2"] == 0 {
		t.Fatal("expected both queues to receive messages before unbind")
	}

	// Unbind queue-2.
	if err := exchange.Unbind(dest2, "1", nil); err != nil {
		t.Fatalf("Unbind(dest2) error: %v", err)
	}

	// All messages should now go to queue-1.
	for msgIdx := range 100 {
		results := make(map[Destination]struct{})
		exchange.Route(&Message{RoutingKey: fmt.Sprintf("key-%d", msgIdx)}, results)

		if len(results) != 1 {
			t.Fatalf("expected 1 destination after unbind, got %d", len(results))
		}

		for dest := range results {
			if dest.Name() != "queue-1" {
				t.Errorf("after unbinding queue-2, got %q, want queue-1", dest.Name())
			}
		}
	}
}

func TestConsistentHashExchange_EmptyNoRoute(t *testing.T) {
	t.Parallel()

	exchange := NewConsistentHashExchange("ch-empty", false, false)

	results := make(map[Destination]struct{})
	exchange.Route(&Message{RoutingKey: "any-key"}, results)

	if len(results) != 0 {
		t.Errorf("expected 0 destinations with no bindings, got %d", len(results))
	}
}

func TestConsistentHashExchange_InvalidWeight(t *testing.T) {
	t.Parallel()

	exchange := NewConsistentHashExchange("ch-invalid", false, false)
	dest := &testDest{name: "queue-1"}

	// Non-numeric routing key should return an error.
	err := exchange.Bind(dest, "not-a-number", nil)
	if err == nil {
		t.Error("Bind() with non-numeric weight should return error")
	}
}

func TestConsistentHashExchange_Bindings(t *testing.T) {
	t.Parallel()

	exchange := NewConsistentHashExchange("ch-bindings", false, false)
	dest1 := &testDest{name: "queue-1"}
	dest2 := &testDest{name: "queue-2"}

	if err := exchange.Bind(dest1, "3", nil); err != nil {
		t.Fatalf("Bind(dest1) error: %v", err)
	}
	if err := exchange.Bind(dest2, "5", nil); err != nil {
		t.Fatalf("Bind(dest2) error: %v", err)
	}

	bindings := exchange.Bindings()
	if len(bindings) != 2 {
		t.Errorf("Bindings() returned %d, want 2", len(bindings))
	}
}
