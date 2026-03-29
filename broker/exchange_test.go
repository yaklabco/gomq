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

// --- Topic Exchange Tests ---

func TestTopicExchange_HashMatchesZeroOrMore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bindKey    string
		routingKey string
		wantMatch  bool
	}{
		{name: "hash suffix matches one word", bindKey: "stock.#", routingKey: "stock.usd", wantMatch: true},
		{name: "hash suffix matches two words", bindKey: "stock.#", routingKey: "stock.usd.nyse", wantMatch: true},
		{name: "hash suffix matches zero words", bindKey: "stock.#", routingKey: "stock", wantMatch: true},
		{name: "hash alone matches everything", bindKey: "#", routingKey: "stock.usd.nyse", wantMatch: true},
		{name: "hash alone matches single word", bindKey: "#", routingKey: "stock", wantMatch: true},
		{name: "hash alone matches empty string", bindKey: "#", routingKey: "", wantMatch: true},
		{name: "hash prefix matches one word", bindKey: "#.nyse", routingKey: "nyse", wantMatch: true},
		{name: "hash prefix matches two words", bindKey: "#.nyse", routingKey: "stock.nyse", wantMatch: true},
		{name: "hash prefix matches three words", bindKey: "#.nyse", routingKey: "stock.usd.nyse", wantMatch: true},
		{name: "hash prefix no match wrong suffix", bindKey: "#.nyse", routingKey: "stock.usd.london", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ex := NewTopicExchange("topic-test", false, false)
			dest := &testDest{name: "queue-1"}

			if err := ex.Bind(dest, tt.bindKey, nil); err != nil {
				t.Fatalf("Bind() error: %v", err)
			}

			results := make(map[Destination]struct{})
			ex.Route(&Message{RoutingKey: tt.routingKey}, results)

			_, found := results[dest]
			if found != tt.wantMatch {
				t.Errorf("Route(%q) with bind key %q: got match=%v, want match=%v",
					tt.routingKey, tt.bindKey, found, tt.wantMatch)
			}
		})
	}
}

func TestTopicExchange_StarMatchesExactlyOne(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bindKey    string
		routingKey string
		wantMatch  bool
	}{
		{name: "star matches one word", bindKey: "stock.*", routingKey: "stock.usd", wantMatch: true},
		{name: "star no match zero words", bindKey: "stock.*", routingKey: "stock", wantMatch: false},
		{name: "star no match two words", bindKey: "stock.*", routingKey: "stock.usd.nyse", wantMatch: false},
		{name: "star middle segment", bindKey: "stock.*.nyse", routingKey: "stock.usd.nyse", wantMatch: true},
		{name: "star middle no match wrong suffix", bindKey: "stock.*.nyse", routingKey: "stock.eur.london", wantMatch: false},
		{name: "star alone matches one word", bindKey: "*", routingKey: "stock", wantMatch: true},
		{name: "star alone no match two words", bindKey: "*", routingKey: "stock.usd", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ex := NewTopicExchange("topic-test", false, false)
			dest := &testDest{name: "queue-1"}

			if err := ex.Bind(dest, tt.bindKey, nil); err != nil {
				t.Fatalf("Bind() error: %v", err)
			}

			results := make(map[Destination]struct{})
			ex.Route(&Message{RoutingKey: tt.routingKey}, results)

			_, found := results[dest]
			if found != tt.wantMatch {
				t.Errorf("Route(%q) with bind key %q: got match=%v, want match=%v",
					tt.routingKey, tt.bindKey, found, tt.wantMatch)
			}
		})
	}
}

func TestTopicExchange_ExactMatch(t *testing.T) {
	t.Parallel()

	ex := NewTopicExchange("topic-test", false, false)
	dest := &testDest{name: "queue-1"}

	if err := ex.Bind(dest, "stock.usd", nil); err != nil {
		t.Fatalf("Bind() error: %v", err)
	}

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "stock.usd"}, results)

	if _, ok := results[dest]; !ok {
		t.Error("expected match for exact routing key 'stock.usd'")
	}

	results = make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "stock.eur"}, results)

	if len(results) != 0 {
		t.Error("expected no match for 'stock.eur' with binding 'stock.usd'")
	}
}

func TestTopicExchange_MultipleBindingsDedup(t *testing.T) {
	t.Parallel()

	ex := NewTopicExchange("topic-test", false, false)
	dest := &testDest{name: "queue-1"}

	// Both patterns match "stock.usd", destination should appear only once
	if err := ex.Bind(dest, "stock.*", nil); err != nil {
		t.Fatalf("Bind(stock.*) error: %v", err)
	}

	if err := ex.Bind(dest, "stock.#", nil); err != nil {
		t.Fatalf("Bind(stock.#) error: %v", err)
	}

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: "stock.usd"}, results)

	if len(results) != 1 {
		t.Errorf("expected destination to appear once, got %d", len(results))
	}
}

func TestTopicExchange_EmptyRoutingKey(t *testing.T) {
	t.Parallel()

	ex := NewTopicExchange("topic-test", false, false)
	dest := &testDest{name: "queue-1"}

	if err := ex.Bind(dest, "", nil); err != nil {
		t.Fatalf("Bind() error: %v", err)
	}

	results := make(map[Destination]struct{})
	ex.Route(&Message{RoutingKey: ""}, results)

	if _, ok := results[dest]; !ok {
		t.Error("expected match for empty routing key with empty binding key")
	}
}

func TestTopicExchange_Type(t *testing.T) {
	t.Parallel()

	ex := NewTopicExchange("topic-test", false, false)

	if got := ex.Type(); got != "topic" {
		t.Errorf("Type() = %q, want %q", got, "topic")
	}
}
