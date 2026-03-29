package broker

import "testing"

func TestDefaultExchange_Type(t *testing.T) {
	t.Parallel()

	queues := make(map[string]*Queue)
	ex := NewDefaultExchange(queues)

	if got := ex.Type(); got != "direct" {
		t.Errorf("Type() = %q, want %q", got, "direct")
	}
}

func TestDefaultExchange_Name(t *testing.T) {
	t.Parallel()

	queues := make(map[string]*Queue)
	ex := NewDefaultExchange(queues)

	if got := ex.Name(); got != "" {
		t.Errorf("Name() = %q, want %q", got, "")
	}
}

func TestDefaultExchange_RoutesToQueueByName(t *testing.T) {
	t.Parallel()

	queues := make(map[string]*Queue)
	// Use a testDest stand-in; we only need Name() for routing lookup,
	// but DefaultExchange looks up in the queue map, so we need a real *Queue pointer.
	// We'll use a nil-safe approach: just put a placeholder in the map.
	orderQueue := &Queue{name: "orders"}
	queues["orders"] = orderQueue

	ex := NewDefaultExchange(queues)

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "orders"}, results)

	if _, ok := results[orderQueue]; !ok {
		t.Error("expected queue 'orders' in results")
	}

	if len(results) != 1 {
		t.Errorf("expected 1 destination, got %d", len(results))
	}
}

func TestDefaultExchange_NoMatchReturnsEmpty(t *testing.T) {
	t.Parallel()

	queues := make(map[string]*Queue)
	queues["orders"] = &Queue{name: "orders"}

	ex := NewDefaultExchange(queues)

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "nonexistent"}, results)

	if len(results) != 0 {
		t.Errorf("expected 0 destinations, got %d", len(results))
	}
}

func TestDefaultExchange_BindReturnsError(t *testing.T) {
	t.Parallel()

	queues := make(map[string]*Queue)
	ex := NewDefaultExchange(queues)

	dest := &testDest{name: "q1"}
	err := ex.Bind(dest, "key", nil)

	if err == nil {
		t.Error("expected error from Bind on default exchange, got nil")
	}
}

func TestDefaultExchange_UnbindReturnsError(t *testing.T) {
	t.Parallel()

	queues := make(map[string]*Queue)
	ex := NewDefaultExchange(queues)

	dest := &testDest{name: "q1"}
	err := ex.Unbind(dest, "key", nil)

	if err == nil {
		t.Error("expected error from Unbind on default exchange, got nil")
	}
}

func TestDefaultExchange_IsDurable(t *testing.T) {
	t.Parallel()

	queues := make(map[string]*Queue)
	ex := NewDefaultExchange(queues)

	if !ex.IsDurable() {
		t.Error("default exchange should be durable")
	}
}

func TestDefaultExchange_IsNotAutoDelete(t *testing.T) {
	t.Parallel()

	queues := make(map[string]*Queue)
	ex := NewDefaultExchange(queues)

	if ex.IsAutoDelete() {
		t.Error("default exchange should not be auto-delete")
	}
}
