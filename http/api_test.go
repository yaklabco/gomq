package mgmt_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yaklabco/gomq/broker"
	"github.com/yaklabco/gomq/config"
	mgmt "github.com/yaklabco/gomq/http"
	"github.com/yaklabco/gomq/shovel"
)

// newTestAPI creates a Server, UserStore, and API for testing.
func newTestAPI(t *testing.T) *mgmt.API {
	t.Helper()

	dir := t.TempDir()
	cfg := config.Default()
	cfg.DataDir = dir

	srv, err := broker.NewServer(cfg)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	t.Cleanup(func() {
		if closeErr := srv.Close(); closeErr != nil {
			t.Errorf("close server: %v", closeErr)
		}
	})

	shovelStore, err := shovel.NewStore(dir)
	if err != nil {
		t.Fatalf("new shovel store: %v", err)
	}

	return mgmt.NewAPI(srv, srv.Users(), shovelStore)
}

func doRequest(t *testing.T, ts *httptest.Server, method, path string, body string) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}

	req, err := http.NewRequest(method, ts.URL+path, bodyReader) //nolint:noctx // test helper
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.SetBasicAuth("guest", "guest")

	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	return resp
}

func doRequestNoAuth(t *testing.T, ts *httptest.Server, method, path string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, ts.URL+path, nil) //nolint:noctx // test helper
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	return resp
}

func readJSON(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck // test helper

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode json: %v", err)
	}

	return result
}

func readJSONArray(t *testing.T, resp *http.Response) []interface{} {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck // test helper

	var result []interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode json array: %v", err)
	}

	return result
}

func drainBody(t *testing.T, resp *http.Response) {
	t.Helper()
	_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck // test drain
	resp.Body.Close()                     //nolint:errcheck,revive // test drain
}

func TestAPI_AuthRequired(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequestNoAuth(t, ts, http.MethodGet, "/api/overview")
	defer resp.Body.Close() //nolint:errcheck // test

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestAPI_Overview(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/overview", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	result := readJSON(t, resp)

	if _, ok := result["gomq_version"]; !ok {
		t.Error("response missing gomq_version")
	}
}

func TestAPI_Whoami(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/whoami", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	result := readJSON(t, resp)

	if result["name"] != "guest" {
		t.Errorf("name = %v, want guest", result["name"])
	}
}

func TestAPI_Queues_Empty(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/queues", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	result := readJSONArray(t, resp)

	if len(result) != 0 {
		t.Errorf("queues = %d, want 0", len(result))
	}
}

func TestAPI_QueueDeclareAndGet(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Declare a queue via PUT.
	resp := doRequest(t, ts, http.MethodPut, "/api/queues/%2F/test-queue",
		`{"durable":true,"auto_delete":false}`)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			t.Fatalf("read body: %v", readErr)
		}
		t.Fatalf("PUT status = %d, want 201 or 204, body: %s", resp.StatusCode, body)
	}
	drainBody(t, resp)

	// Get the queue.
	resp = doRequest(t, ts, http.MethodGet, "/api/queues/%2F/test-queue", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	queue := readJSON(t, resp)

	if queue["name"] != "test-queue" {
		t.Errorf("name = %v, want test-queue", queue["name"])
	}
	if queue["vhost"] != "/" {
		t.Errorf("vhost = %v, want /", queue["vhost"])
	}
	if durable, ok := queue["durable"].(bool); !ok || !durable {
		t.Errorf("durable = %v, want true", queue["durable"])
	}
}

func TestAPI_QueueDeclareAndDelete(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Create.
	resp := doRequest(t, ts, http.MethodPut, "/api/queues/%2F/del-queue",
		`{"durable":false}`)
	drainBody(t, resp)

	// Delete.
	resp = doRequest(t, ts, http.MethodDelete, "/api/queues/%2F/del-queue", "")
	drainBody(t, resp)

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// Verify gone.
	resp = doRequest(t, ts, http.MethodGet, "/api/queues/%2F/del-queue", "")
	drainBody(t, resp)

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET after delete status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestAPI_ExchangeDeclareAndGet(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Declare.
	resp := doRequest(t, ts, http.MethodPut, "/api/exchanges/%2F/test.direct",
		`{"type":"direct","durable":true,"auto_delete":false}`)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			t.Fatalf("read body: %v", readErr)
		}
		t.Fatalf("PUT status = %d, want 201 or 204, body: %s", resp.StatusCode, body)
	}
	drainBody(t, resp)

	// Get.
	resp = doRequest(t, ts, http.MethodGet, "/api/exchanges/%2F/test.direct", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	ex := readJSON(t, resp)

	if ex["name"] != "test.direct" {
		t.Errorf("name = %v, want test.direct", ex["name"])
	}
	if ex["type"] != "direct" {
		t.Errorf("type = %v, want direct", ex["type"])
	}
}

func TestAPI_UsersList(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/users", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	users := readJSONArray(t, resp)

	found := false
	for _, entry := range users {
		um, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		if um["name"] == "guest" {
			found = true
			break
		}
	}

	if !found {
		t.Error("guest user not found in user list")
	}
}

func TestAPI_UserPutDelete(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Create user.
	resp := doRequest(t, ts, http.MethodPut, "/api/users/testuser",
		`{"password":"secret","tags":"monitoring"}`)
	drainBody(t, resp)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT user status = %d", resp.StatusCode)
	}

	// Get user.
	resp = doRequest(t, ts, http.MethodGet, "/api/users/testuser", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET user status = %d", resp.StatusCode)
	}

	user := readJSON(t, resp)
	if user["name"] != "testuser" {
		t.Errorf("name = %v, want testuser", user["name"])
	}

	// Delete user.
	resp = doRequest(t, ts, http.MethodDelete, "/api/users/testuser", "")
	drainBody(t, resp)

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE user status = %d", resp.StatusCode)
	}

	// Verify gone.
	resp = doRequest(t, ts, http.MethodGet, "/api/users/testuser", "")
	drainBody(t, resp)

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET after delete = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestAPI_Vhosts(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// List vhosts.
	resp := doRequest(t, ts, http.MethodGet, "/api/vhosts", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", resp.StatusCode)
	}

	vhosts := readJSONArray(t, resp)
	if len(vhosts) < 1 {
		t.Fatal("expected at least default vhost")
	}

	// Create a new vhost.
	resp = doRequest(t, ts, http.MethodPut, "/api/vhosts/test-vhost", "")
	drainBody(t, resp)

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT vhost status = %d", resp.StatusCode)
	}

	// Delete vhost.
	resp = doRequest(t, ts, http.MethodDelete, "/api/vhosts/test-vhost", "")
	drainBody(t, resp)

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("DELETE vhost status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestAPI_Definitions(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Export definitions.
	resp := doRequest(t, ts, http.MethodGet, "/api/definitions", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET definitions status = %d", resp.StatusCode)
	}

	defs := readJSON(t, resp)

	if _, ok := defs["vhosts"]; !ok {
		t.Error("definitions missing vhosts")
	}
	if _, ok := defs["users"]; !ok {
		t.Error("definitions missing users")
	}
	if _, ok := defs["exchanges"]; !ok {
		t.Error("definitions missing exchanges")
	}
	if _, ok := defs["queues"]; !ok {
		t.Error("definitions missing queues")
	}

	// Import definitions (create a queue).
	importBody := `{
		"queues": [{"name":"imported-q","vhost":"/","durable":true,"auto_delete":false,"arguments":{}}]
	}`
	resp = doRequest(t, ts, http.MethodPost, "/api/definitions", importBody)
	drainBody(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST definitions status = %d", resp.StatusCode)
	}

	// Verify queue was created.
	resp = doRequest(t, ts, http.MethodGet, "/api/queues/%2F/imported-q", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET imported queue status = %d", resp.StatusCode)
	}

	queue := readJSON(t, resp)
	if queue["name"] != "imported-q" {
		t.Errorf("imported queue name = %v, want imported-q", queue["name"])
	}
}

func TestAPI_AlivenessTest(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/aliveness-test/%2F", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	result := readJSON(t, resp)

	if result["status"] != "ok" {
		t.Errorf("status = %v, want ok", result["status"])
	}
}

func TestAPI_HealthCheck(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/healthchecks/node", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	result := readJSON(t, resp)

	if result["status"] != "ok" {
		t.Errorf("status = %v, want ok", result["status"])
	}
}

func TestAPI_Metrics(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/metrics", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	defer resp.Body.Close() //nolint:errcheck // test

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "gomq_connections") {
		t.Error("metrics missing gomq_connections")
	}
	if !strings.Contains(bodyStr, "gomq_queues_messages") {
		t.Error("metrics missing gomq_queues_messages")
	}
}

func TestAPI_Connections(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/connections", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	conns := readJSONArray(t, resp)
	if len(conns) != 0 {
		t.Errorf("connections = %d, want 0", len(conns))
	}
}

func TestAPI_Channels(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/channels", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	chs := readJSONArray(t, resp)
	if len(chs) != 0 {
		t.Errorf("channels = %d, want 0", len(chs))
	}
}

func TestAPI_Exchanges_ListAll(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/api/exchanges", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	exchanges := readJSONArray(t, resp)

	// Default vhost should have default exchanges.
	if len(exchanges) < 4 {
		t.Errorf("exchanges = %d, want >= 4 (default exchanges)", len(exchanges))
	}
}

func TestAPI_Bindings(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Create exchange and queue first.
	resp := doRequest(t, ts, http.MethodPut, "/api/exchanges/%2F/bind-test-ex",
		`{"type":"direct","durable":false}`)
	drainBody(t, resp)

	resp = doRequest(t, ts, http.MethodPut, "/api/queues/%2F/bind-test-q",
		`{"durable":false}`)
	drainBody(t, resp)

	// Create binding.
	resp = doRequest(t, ts, http.MethodPost, "/api/bindings/%2F/e/bind-test-ex/q/bind-test-q",
		`{"routing_key":"test-key"}`)
	drainBody(t, resp)

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST binding status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	// List bindings in vhost.
	resp = doRequest(t, ts, http.MethodGet, "/api/bindings/%2F", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET bindings status = %d", resp.StatusCode)
	}

	bindings := readJSONArray(t, resp)

	found := false
	for _, entry := range bindings {
		bm, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		if bm["source"] == "bind-test-ex" && bm["destination"] == "bind-test-q" {
			found = true
			break
		}
	}

	if !found {
		t.Error("binding not found in list")
	}
}

func TestAPI_Permissions(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Set permissions.
	resp := doRequest(t, ts, http.MethodPut, "/api/permissions/%2F/guest",
		`{"configure":".*","write":".*","read":".*"}`)
	drainBody(t, resp)

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT permissions status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	// Get permissions.
	resp = doRequest(t, ts, http.MethodGet, "/api/permissions/%2F/guest", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET permissions status = %d", resp.StatusCode)
	}

	perm := readJSON(t, resp)
	if perm["configure"] != ".*" {
		t.Errorf("configure = %v, want .*", perm["configure"])
	}

	// List all permissions.
	resp = doRequest(t, ts, http.MethodGet, "/api/permissions", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list permissions status = %d", resp.StatusCode)
	}

	perms := readJSONArray(t, resp)
	if len(perms) == 0 {
		t.Error("expected at least one permission entry")
	}
}

func TestAPI_QueuePurge(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Create queue.
	resp := doRequest(t, ts, http.MethodPut, "/api/queues/%2F/purge-q",
		`{"durable":false}`)
	drainBody(t, resp)

	// Purge.
	resp = doRequest(t, ts, http.MethodDelete, "/api/queues/%2F/purge-q/contents", "")
	drainBody(t, resp)

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("purge status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

// TestAPI_MetricsAlternateRoute tests /metrics as well as /api/metrics.
func TestAPI_MetricsAlternateRoute(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodGet, "/metrics", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	drainBody(t, resp)
}

// --- Shovels ---

func TestShovelAPI(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// List shovels — initially empty.
	resp := doRequest(t, ts, http.MethodGet, "/api/shovels", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var shovels []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&shovels); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close() //nolint:errcheck,revive // test drain

	if len(shovels) != 0 {
		t.Errorf("expected 0 shovels, got %d", len(shovels))
	}

	// Create a shovel.
	createBody := `{
		"src_uri": "amqp://source:5672",
		"src_queue": "src-q",
		"dest_uri": "amqp://dest:5672",
		"dest_type": "amqp",
		"exchange": "dest-ex",
		"ack_mode": "on-publish",
		"prefetch": 100
	}`
	resp = doRequest(t, ts, http.MethodPut, "/api/shovels/%2F/test-shovel", createBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	drainBody(t, resp)

	// List shovels — should have one.
	resp = doRequest(t, ts, http.MethodGet, "/api/shovels", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	if err := json.NewDecoder(resp.Body).Decode(&shovels); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close() //nolint:errcheck,revive // test drain

	if len(shovels) != 1 {
		t.Fatalf("expected 1 shovel, got %d", len(shovels))
	}
	if shovels[0]["name"] != "test-shovel" {
		t.Errorf("shovel name = %v, want %q", shovels[0]["name"], "test-shovel")
	}

	// Delete the shovel.
	resp = doRequest(t, ts, http.MethodDelete, "/api/shovels/%2F/test-shovel", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	drainBody(t, resp)

	// List again — empty.
	resp = doRequest(t, ts, http.MethodGet, "/api/shovels", "")
	if err := json.NewDecoder(resp.Body).Decode(&shovels); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close() //nolint:errcheck,revive // test drain

	if len(shovels) != 0 {
		t.Errorf("expected 0 shovels after delete, got %d", len(shovels))
	}
}

func TestShovelAPIValidation(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Missing required fields.
	resp := doRequest(t, ts, http.MethodPut, "/api/shovels/%2F/bad", `{}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	drainBody(t, resp)
}

func TestShovelAPIDeleteNotFound(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	resp := doRequest(t, ts, http.MethodDelete, "/api/shovels/%2F/nonexistent", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	drainBody(t, resp)
}

// --- Federation Links ---

func TestFederationLinkAPI(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// List — initially empty.
	resp := doRequest(t, ts, http.MethodGet, "/api/federation-links", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var links []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&links); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close() //nolint:errcheck,revive // test drain

	if len(links) != 0 {
		t.Errorf("expected 0 links, got %d", len(links))
	}

	// Create.
	createBody := `{
		"uri": "amqp://remote:5672",
		"exchange": "events",
		"max_hops": 2,
		"prefetch": 100
	}`
	resp = doRequest(t, ts, http.MethodPut, "/api/federation-links/%2F/upstream1", createBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	drainBody(t, resp)

	// List — should have one.
	resp = doRequest(t, ts, http.MethodGet, "/api/federation-links", "")
	if err := json.NewDecoder(resp.Body).Decode(&links); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close() //nolint:errcheck,revive // test drain

	if len(links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(links))
	}
	if links[0]["name"] != "upstream1" {
		t.Errorf("link name = %v, want %q", links[0]["name"], "upstream1")
	}

	// Delete.
	resp = doRequest(t, ts, http.MethodDelete, "/api/federation-links/%2F/upstream1", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	drainBody(t, resp)

	// List — empty again.
	resp = doRequest(t, ts, http.MethodGet, "/api/federation-links", "")
	if err := json.NewDecoder(resp.Body).Decode(&links); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close() //nolint:errcheck,revive // test drain

	if len(links) != 0 {
		t.Errorf("expected 0 links after delete, got %d", len(links))
	}
}

func TestFederationLinkAPIValidation(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t)
	ts := httptest.NewServer(api.Handler())
	defer ts.Close()

	// Missing URI.
	resp := doRequest(t, ts, http.MethodPut, "/api/federation-links/%2F/bad", `{"exchange": "ex"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	drainBody(t, resp)
}
