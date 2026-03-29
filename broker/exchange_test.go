package broker

import "testing"

// testDest is a minimal Destination for testing.
type testDest struct {
	name string
}

func (d *testDest) Name() string { return d.name }

func TestExchangeBaseFields(t *testing.T) {
	t.Parallel()

	base := exchangeBase{
		name:       "test-exchange",
		durable:    true,
		autoDelete: false,
	}

	if got := base.Name(); got != "test-exchange" {
		t.Errorf("Name() = %q, want %q", got, "test-exchange")
	}

	if !base.IsDurable() {
		t.Error("IsDurable() = false, want true")
	}

	if base.IsAutoDelete() {
		t.Error("IsAutoDelete() = true, want false")
	}
}

// --- Direct Exchange Tests ---

func TestDirectExchange_RouteExactKey(t *testing.T) {
	t.Parallel()

	ex := NewDirectExchange("direct-test", false, false)
	dest := &testDest{name: "queue-1"}

	if err := ex.Bind(dest, "order.created", nil); err != nil {
		t.Fatalf("Bind() error: %v", err)
	}

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "order.created"}, results)

	if _, ok := results[dest]; !ok {
		t.Error("expected queue-1 in results for key 'order.created'")
	}
}

func TestDirectExchange_MultipleQueuesSameKey(t *testing.T) {
	t.Parallel()

	ex := NewDirectExchange("direct-test", false, false)
	dest1 := &testDest{name: "queue-1"}
	dest2 := &testDest{name: "queue-2"}

	if err := ex.Bind(dest1, "order.created", nil); err != nil {
		t.Fatalf("Bind(dest1) error: %v", err)
	}

	if err := ex.Bind(dest2, "order.created", nil); err != nil {
		t.Fatalf("Bind(dest2) error: %v", err)
	}

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "order.created"}, results)

	if len(results) != 2 {
		t.Errorf("expected 2 destinations, got %d", len(results))
	}
}

func TestDirectExchange_NoMatch(t *testing.T) {
	t.Parallel()

	ex := NewDirectExchange("direct-test", false, false)
	dest := &testDest{name: "queue-1"}

	if err := ex.Bind(dest, "order.created", nil); err != nil {
		t.Fatalf("Bind() error: %v", err)
	}

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "order.cancelled"}, results)

	if len(results) != 0 {
		t.Errorf("expected 0 destinations, got %d", len(results))
	}
}

func TestDirectExchange_UnbindRemovesRouting(t *testing.T) {
	t.Parallel()

	ex := NewDirectExchange("direct-test", false, false)
	dest := &testDest{name: "queue-1"}

	if err := ex.Bind(dest, "order.created", nil); err != nil {
		t.Fatalf("Bind() error: %v", err)
	}

	if err := ex.Unbind(dest, "order.created", nil); err != nil {
		t.Fatalf("Unbind() error: %v", err)
	}

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "order.created"}, results)

	if len(results) != 0 {
		t.Errorf("expected 0 destinations after unbind, got %d", len(results))
	}
}

func TestDirectExchange_Type(t *testing.T) {
	t.Parallel()

	ex := NewDirectExchange("direct-test", false, false)

	if got := ex.Type(); got != "direct" {
		t.Errorf("Type() = %q, want %q", got, "direct")
	}
}

// --- Fanout Exchange Tests ---

func TestFanoutExchange_AllDestinations(t *testing.T) {
	t.Parallel()

	ex := NewFanoutExchange("fanout-test", false, false)
	dest1 := &testDest{name: "queue-1"}
	dest2 := &testDest{name: "queue-2"}
	dest3 := &testDest{name: "queue-3"}

	for _, dest := range []Destination{dest1, dest2, dest3} {
		if err := ex.Bind(dest, "", nil); err != nil {
			t.Fatalf("Bind(%s) error: %v", dest.Name(), err)
		}
	}

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "anything.ignored"}, results)

	if len(results) != 3 {
		t.Errorf("expected 3 destinations, got %d", len(results))
	}

	for _, dest := range []Destination{dest1, dest2, dest3} {
		if _, ok := results[dest]; !ok {
			t.Errorf("expected %s in results", dest.Name())
		}
	}
}

func TestFanoutExchange_EmptyBindings(t *testing.T) {
	t.Parallel()

	ex := NewFanoutExchange("fanout-test", false, false)

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "anything"}, results)

	if len(results) != 0 {
		t.Errorf("expected 0 destinations, got %d", len(results))
	}
}

func TestFanoutExchange_Type(t *testing.T) {
	t.Parallel()

	ex := NewFanoutExchange("fanout-test", false, false)

	if got := ex.Type(); got != "fanout" {
		t.Errorf("Type() = %q, want %q", got, "fanout")
	}
}
