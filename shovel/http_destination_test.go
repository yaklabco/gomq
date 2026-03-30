package shovel

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHTTPDestinationPublish(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var receivedBodies []string
	var receivedHeaders []http.Header

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		mu.Lock()
		receivedBodies = append(receivedBodies, string(body))
		receivedHeaders = append(receivedHeaders, r.Header.Clone())
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dst := NewHTTPDestination(srv.URL, "POST", map[string]string{
		"X-Custom": "test-value",
	})

	if err := dst.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	msg := amqp.Publishing{
		Body:        []byte("hello world"),
		ContentType: "text/plain",
	}
	if err := dst.Publish(msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedBodies) != 1 {
		t.Fatalf("expected 1 request, got %d", len(receivedBodies))
	}
	if receivedBodies[0] != "hello world" {
		t.Errorf("body = %q, want %q", receivedBodies[0], "hello world")
	}
	if receivedHeaders[0].Get("Content-Type") != "text/plain" {
		t.Errorf("Content-Type = %q, want %q", receivedHeaders[0].Get("Content-Type"), "text/plain")
	}
	if receivedHeaders[0].Get("X-Custom") != "test-value" {
		t.Errorf("X-Custom = %q, want %q", receivedHeaders[0].Get("X-Custom"), "test-value")
	}

	if err := dst.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestHTTPDestinationErrorStatus(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	dst := NewHTTPDestination(srv.URL, "POST", nil)
	if err := dst.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	msg := amqp.Publishing{Body: []byte("test")}
	if err := dst.Publish(msg); err == nil {
		t.Error("expected error for 500 response, got nil")
	}
}

func TestHTTPDestinationDefaults(t *testing.T) {
	t.Parallel()

	dst := NewHTTPDestination("http://example.com", "", nil)

	if dst.Method != http.MethodPost {
		t.Errorf("Method = %q, want %q", dst.Method, http.MethodPost)
	}
	if dst.Headers == nil {
		t.Error("Headers should not be nil")
	}
}

func TestHTTPDestinationCustomMethod(t *testing.T) {
	t.Parallel()

	var receivedMethod string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dst := NewHTTPDestination(srv.URL, "PUT", nil)
	if err := dst.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	msg := amqp.Publishing{Body: []byte("update")}
	if err := dst.Publish(msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	if receivedMethod != "PUT" {
		t.Errorf("method = %q, want PUT", receivedMethod)
	}
}
