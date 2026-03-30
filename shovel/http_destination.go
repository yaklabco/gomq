package shovel

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// httpClientTimeout is the default timeout for HTTP destination requests.
const httpClientTimeout = 30 * time.Second

// HTTPDestination publishes messages as HTTP requests. The message
// body becomes the request body.
type HTTPDestination struct {
	URL     string
	Method  string
	Headers map[string]string
	client  *http.Client
}

// NewHTTPDestination creates an HTTP destination. Method defaults to
// POST if empty.
func NewHTTPDestination(url, method string, headers map[string]string) *HTTPDestination {
	if method == "" {
		method = http.MethodPost
	}
	if headers == nil {
		headers = make(map[string]string)
	}
	return &HTTPDestination{
		URL:     url,
		Method:  method,
		Headers: headers,
	}
}

// Connect initialises the HTTP client. This is a no-op for HTTP
// destinations since connections are made per-request.
func (d *HTTPDestination) Connect(_ context.Context) error {
	d.client = &http.Client{
		Timeout: httpClientTimeout,
	}
	return nil
}

// Publish sends the message body as an HTTP request.
func (d *HTTPDestination) Publish(msg amqp.Publishing) error {
	if d.client == nil {
		d.client = &http.Client{Timeout: httpClientTimeout}
	}

	req, err := http.NewRequestWithContext(context.Background(), d.Method, d.URL, bytes.NewReader(msg.Body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	if msg.ContentType != "" {
		req.Header.Set("Content-Type", msg.ContentType)
	}

	for k, v := range d.Headers {
		req.Header.Set(k, v)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("http %s %s: %w", d.Method, d.URL, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck // drain body
		_ = resp.Body.Close()                 //nolint:errcheck // best-effort cleanup
	}()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("http %s %s: status %d", d.Method, d.URL, resp.StatusCode)
	}

	return nil
}

// Close is a no-op for HTTP destinations.
func (d *HTTPDestination) Close() error {
	return nil
}
