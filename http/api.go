// Package http provides a RabbitMQ-compatible HTTP management API for GoMQ.
package mgmt

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jamesainslie/gomq/auth"
	"github.com/jamesainslie/gomq/broker"
)

// Version is the broker version reported by the management API.
const Version = "0.1.0"

// API provides the HTTP management API endpoints.
type API struct {
	server *broker.Server
	users  *auth.UserStore
	router *router
}

// NewAPI creates a management API backed by the given server and user store.
func NewAPI(server *broker.Server, users *auth.UserStore) *API {
	a := &API{
		server: server,
		users:  users,
		router: newRouter(),
	}

	a.registerRoutes()

	return a
}

// Handler returns the HTTP handler for the management API.
func (a *API) Handler() http.Handler {
	return a.router
}

// registerRoutes wires all API endpoints to the router.
func (a *API) registerRoutes() {
	// Overview.
	a.route("GET", "/api/overview", a.handleOverview)
	a.route("GET", "/api/whoami", a.handleWhoami)

	// Connections.
	a.route("GET", "/api/connections", a.handleListConnections)
	a.route("DELETE", "/api/connections/{name}", a.handleDeleteConnection)

	// Channels.
	a.route("GET", "/api/channels", a.handleListChannels)

	// Exchanges.
	a.route("GET", "/api/exchanges", a.handleListAllExchanges)
	a.route("GET", "/api/exchanges/{vhost}", a.handleListExchanges)
	a.route("GET", "/api/exchanges/{vhost}/{name}", a.handleGetExchange)
	a.route("PUT", "/api/exchanges/{vhost}/{name}", a.handleDeclareExchange)
	a.route("DELETE", "/api/exchanges/{vhost}/{name}", a.handleDeleteExchange)

	// Queues — purge must be before delete to match first.
	a.route("DELETE", "/api/queues/{vhost}/{name}/contents", a.handlePurgeQueue)
	a.route("GET", "/api/queues", a.handleListAllQueues)
	a.route("GET", "/api/queues/{vhost}", a.handleListQueues)
	a.route("GET", "/api/queues/{vhost}/{name}", a.handleGetQueue)
	a.route("PUT", "/api/queues/{vhost}/{name}", a.handleDeclareQueue)
	a.route("DELETE", "/api/queues/{vhost}/{name}", a.handleDeleteQueue)

	// Bindings.
	a.route("GET", "/api/bindings", a.handleListAllBindings)
	a.route("GET", "/api/bindings/{vhost}", a.handleListBindings)
	a.route("POST", "/api/bindings/{vhost}/e/{exchange}/q/{queue}", a.handleCreateBinding)

	// Users.
	a.route("GET", "/api/users", a.handleListUsers)
	a.route("GET", "/api/users/{name}", a.handleGetUser)
	a.route("PUT", "/api/users/{name}", a.handlePutUser)
	a.route("DELETE", "/api/users/{name}", a.handleDeleteUser)

	// Vhosts.
	a.route("GET", "/api/vhosts", a.handleListVhosts)
	a.route("GET", "/api/vhosts/{name}", a.handleGetVhost)
	a.route("PUT", "/api/vhosts/{name}", a.handlePutVhost)
	a.route("DELETE", "/api/vhosts/{name}", a.handleDeleteVhost)

	// Permissions.
	a.route("GET", "/api/permissions", a.handleListPermissions)
	a.route("GET", "/api/permissions/{vhost}/{user}", a.handleGetPermissions)
	a.route("PUT", "/api/permissions/{vhost}/{user}", a.handlePutPermissions)

	// Policies.
	a.route("GET", "/api/policies", a.handleListAllPolicies)
	a.route("GET", "/api/policies/{vhost}", a.handleListPolicies)
	a.route("GET", "/api/policies/{vhost}/{name}", a.handleGetPolicy)
	a.route("PUT", "/api/policies/{vhost}/{name}", a.handlePutPolicy)
	a.route("DELETE", "/api/policies/{vhost}/{name}", a.handleDeletePolicy)

	// Vhost limits.
	a.route("GET", "/api/vhost-limits", a.handleListAllVhostLimits)
	a.route("GET", "/api/vhost-limits/{vhost}", a.handleGetVhostLimits)
	a.route("PUT", "/api/vhost-limits/{vhost}", a.handlePutVhostLimits)
	a.route("DELETE", "/api/vhost-limits/{vhost}", a.handleDeleteVhostLimits)

	// Definitions.
	a.route("GET", "/api/definitions", a.handleGetDefinitions)
	a.route("POST", "/api/definitions", a.handlePostDefinitions)

	// Health.
	a.route("GET", "/api/aliveness-test/{vhost}", a.handleAlivenessTest)
	a.route("GET", "/api/healthchecks/node", a.handleHealthCheck)

	// Metrics.
	a.route("GET", "/api/metrics", a.handleMetrics)
	a.route("GET", "/metrics", a.handleMetrics)
}

// route registers a handler with auth middleware.
func (a *API) route(method, pattern string, handler http.HandlerFunc) {
	a.router.handle(method, pattern, a.requireAuth(handler))
}

// requireAuth wraps a handler with HTTP Basic Auth validation.
func (a *API) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="GoMQ Management"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		if _, err := a.users.Authenticate(username, password); err != nil {
			w.Header().Set("WWW-Authenticate", `Basic realm="GoMQ Management"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

// --- Response helpers ---

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Best effort; headers already sent.
		_ = err //nolint:errcheck // headers already written
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// --- Overview ---

func (a *API) handleOverview(w http.ResponseWriter, _ *http.Request) {
	vhosts := a.server.VHosts()

	var totalQueues, totalExchanges int
	var totalMessages uint32
	for _, vh := range vhosts {
		queues := vh.Queues()
		totalQueues += len(queues)
		for _, q := range queues {
			totalMessages += q.Len()
		}
		totalExchanges += len(vh.Exchanges())
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"gomq_version":       Version,
		"node":               "gomq@localhost",
		"management_version": Version,
		"object_totals": map[string]int{
			"queues":      totalQueues,
			"exchanges":   totalExchanges,
			"connections": len(a.server.Connections()),
		},
		"queue_totals": map[string]interface{}{
			"messages": totalMessages,
		},
	})
}

func (a *API) handleWhoami(w http.ResponseWriter, r *http.Request) {
	username, _, _ := r.BasicAuth()

	user, ok := a.users.Get(username)
	if !ok {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name": user.Name,
		"tags": strings.Join(user.Tags, ","),
	})
}

// --- Connections ---

func (a *API) handleListConnections(w http.ResponseWriter, _ *http.Request) {
	conns := a.server.Connections()

	result := make([]map[string]interface{}, 0, len(conns))
	for _, c := range conns {
		result = append(result, map[string]interface{}{
			"name":     c.Name(),
			"vhost":    c.VHostName(),
			"user":     c.UserName(),
			"state":    "running",
			"channels": c.ChannelCount(),
		})
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleDeleteConnection(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	if !a.server.CloseConnection(name) {
		writeError(w, http.StatusNotFound, "connection not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Channels ---

func (a *API) handleListChannels(w http.ResponseWriter, _ *http.Request) {
	conns := a.server.Connections()

	var result []map[string]interface{}
	for _, c := range conns {
		for _, ch := range c.Channels() {
			result = append(result, map[string]interface{}{
				"name":       fmt.Sprintf("%s (%d)", c.Name(), ch.ID()),
				"connection": c.Name(),
				"number":     ch.ID(),
				"vhost":      c.VHostName(),
				"user":       c.UserName(),
			})
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, result)
}

// --- Exchanges ---

func (a *API) handleListAllExchanges(w http.ResponseWriter, _ *http.Request) {
	vhosts := a.server.VHosts()

	var result []map[string]interface{}
	for vhostName, vh := range vhosts {
		for _, ex := range vh.Exchanges() {
			result = append(result, exchangeJSON(vhostName, ex))
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleListExchanges(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	exchanges := vh.Exchanges()
	result := make([]map[string]interface{}, 0, len(exchanges))
	for _, ex := range exchanges {
		result = append(result, exchangeJSON(vhostName, ex))
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleGetExchange(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	ex, ok := vh.GetExchange(name)
	if !ok {
		writeError(w, http.StatusNotFound, "exchange not found")
		return
	}

	writeJSON(w, http.StatusOK, exchangeJSON(vhostName, ex))
}

func (a *API) handleDeclareExchange(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	var body struct {
		Type       string                 `json:"type"`
		Durable    bool                   `json:"durable"`
		AutoDelete bool                   `json:"auto_delete"`
		Arguments  map[string]interface{} `json:"arguments"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if body.Type == "" {
		body.Type = "direct"
	}

	if _, err := vh.DeclareExchange(name, body.Type, body.Durable, body.AutoDelete, body.Arguments); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (a *API) handleDeleteExchange(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	if err := vh.DeleteExchange(name, false); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func exchangeJSON(vhost string, ex broker.Exchange) map[string]interface{} {
	return map[string]interface{}{
		"name":        ex.Name(),
		"vhost":       vhost,
		"type":        ex.Type(),
		"durable":     ex.IsDurable(),
		"auto_delete": ex.IsAutoDelete(),
		"internal":    false,
		"arguments":   map[string]interface{}{},
	}
}

// --- Queues ---

func (a *API) handleListAllQueues(w http.ResponseWriter, _ *http.Request) {
	vhosts := a.server.VHosts()

	var result []map[string]interface{}
	for vhostName, vh := range vhosts {
		for _, q := range vh.Queues() {
			result = append(result, queueJSON(vhostName, q))
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleListQueues(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	queues := vh.Queues()
	result := make([]map[string]interface{}, 0, len(queues))
	for _, queue := range queues {
		result = append(result, queueJSON(vhostName, queue))
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleGetQueue(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	q, ok := vh.GetQueue(name)
	if !ok {
		writeError(w, http.StatusNotFound, "queue not found")
		return
	}

	writeJSON(w, http.StatusOK, queueJSON(vhostName, q))
}

func (a *API) handleDeclareQueue(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	var body struct {
		Durable    bool                   `json:"durable"`
		AutoDelete bool                   `json:"auto_delete"`
		Arguments  map[string]interface{} `json:"arguments"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if _, err := vh.DeclareQueue(name, body.Durable, false, body.AutoDelete, body.Arguments); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (a *API) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	if _, err := vh.DeleteQueue(name, false, false); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handlePurgeQueue(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	if _, err := vh.PurgeQueue(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func queueJSON(vhost string, q *broker.Queue) map[string]interface{} {
	args := q.Arguments()
	if args == nil {
		args = map[string]interface{}{}
	}

	return map[string]interface{}{
		"name":        q.Name(),
		"vhost":       vhost,
		"durable":     q.IsDurable(),
		"auto_delete": q.IsAutoDelete(),
		"exclusive":   q.IsExclusive(),
		"arguments":   args,
		"messages":    q.Len(),
		"consumers":   q.ConsumerCount(),
		"state":       "running",
	}
}

// --- Bindings ---

func (a *API) handleListAllBindings(w http.ResponseWriter, _ *http.Request) {
	vhosts := a.server.VHosts()

	result := make([]map[string]interface{}, 0) //nolint:prealloc // size unknown until iteration
	for vhostName, vh := range vhosts {
		result = append(result, collectBindings(vhostName, vh)...)
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleListBindings(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	result := collectBindings(vhostName, vh)
	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleCreateBinding(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	exchangeName := pathParam(r, "exchange")
	queueName := pathParam(r, "queue")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	var body struct {
		RoutingKey string                 `json:"routing_key"`
		Arguments  map[string]interface{} `json:"arguments"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if err := vh.BindQueue(queueName, exchangeName, body.RoutingKey, body.Arguments); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func collectBindings(vhostName string, vh *broker.VHost) []map[string]interface{} {
	var result []map[string]interface{}

	for _, ex := range vh.Exchanges() {
		for _, binding := range ex.Bindings() {
			args := binding.Arguments
			if args == nil {
				args = map[string]interface{}{}
			}
			result = append(result, map[string]interface{}{
				"source":           binding.Source,
				"vhost":            vhostName,
				"destination":      binding.Destination,
				"destination_type": "queue",
				"routing_key":      binding.RoutingKey,
				"arguments":        args,
			})
		}
	}

	return result
}

// --- Users ---

func (a *API) handleListUsers(w http.ResponseWriter, _ *http.Request) {
	users := a.users.List()

	result := make([]map[string]interface{}, 0, len(users))
	for _, u := range users {
		result = append(result, userJSON(u))
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleGetUser(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	user, ok := a.users.Get(name)
	if !ok {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	writeJSON(w, http.StatusOK, userJSON(user))
}

func (a *API) handlePutUser(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	var body struct {
		Password     string `json:"password"`
		PasswordHash string `json:"password_hash"`
		Tags         string `json:"tags"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	var pw auth.Password
	switch {
	case body.PasswordHash != "":
		var err error
		pw, err = auth.ParsePassword(body.PasswordHash)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid password hash")
			return
		}
	case body.Password != "":
		pw = auth.HashPassword(body.Password)
	default:
		pw = auth.HashPassword("")
	}

	var tags []string
	if body.Tags != "" {
		tags = strings.Split(body.Tags, ",")
	}

	user := &auth.User{
		Name:     name,
		Password: pw,
		Tags:     tags,
	}

	if err := a.users.Put(user); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (a *API) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	if err := a.users.Delete(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func userJSON(u *auth.User) map[string]interface{} {
	return map[string]interface{}{
		"name":          u.Name,
		"password_hash": u.Password.String(),
		"tags":          strings.Join(u.Tags, ","),
	}
}

// --- Vhosts ---

func (a *API) handleListVhosts(w http.ResponseWriter, _ *http.Request) {
	vhosts := a.server.VHosts()

	result := make([]map[string]interface{}, 0, len(vhosts))
	for name := range vhosts {
		result = append(result, map[string]interface{}{
			"name": name,
		})
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleGetVhost(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	if _, ok := a.server.GetVHost(name); !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name": name,
	})
}

func (a *API) handlePutVhost(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	if err := a.server.CreateVHost(name); err != nil {
		// If it already exists, return 204.
		if _, ok := a.server.GetVHost(name); ok {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (a *API) handleDeleteVhost(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	if err := a.server.DeleteVHost(name); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Permissions ---

func (a *API) handleListPermissions(w http.ResponseWriter, _ *http.Request) {
	users := a.users.List()

	var result []map[string]interface{}
	for _, u := range users {
		for vhost, perm := range u.Permissions {
			result = append(result, map[string]interface{}{
				"user":      u.Name,
				"vhost":     vhost,
				"configure": perm.Configure,
				"write":     perm.Write,
				"read":      perm.Read,
			})
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleGetPermissions(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	userName := pathParam(r, "user")

	user, ok := a.users.Get(userName)
	if !ok {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	perm, ok := user.Permissions[vhostName]
	if !ok {
		writeError(w, http.StatusNotFound, "permission not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"user":      user.Name,
		"vhost":     vhostName,
		"configure": perm.Configure,
		"write":     perm.Write,
		"read":      perm.Read,
	})
}

func (a *API) handlePutPermissions(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	userName := pathParam(r, "user")

	user, ok := a.users.Get(userName)
	if !ok {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	var body struct {
		Configure string `json:"configure"`
		Write     string `json:"write"`
		Read      string `json:"read"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	user.SetPermission(vhostName, auth.Permission{
		Configure: body.Configure,
		Write:     body.Write,
		Read:      body.Read,
	})

	// Persist user store.
	if err := a.users.Put(user); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Definitions ---

func (a *API) handleGetDefinitions(w http.ResponseWriter, _ *http.Request) {
	vhosts := a.server.VHosts()

	// Build vhosts list.
	vhostList := make([]map[string]interface{}, 0, len(vhosts))
	for name := range vhosts {
		vhostList = append(vhostList, map[string]interface{}{"name": name})
	}

	// Build exchanges, queues, and bindings.
	var exchanges []map[string]interface{}
	var queues []map[string]interface{}
	bindings := make([]map[string]interface{}, 0) //nolint:prealloc // size unknown until iteration

	for vhostName, vh := range vhosts {
		for _, ex := range vh.Exchanges() {
			exchanges = append(exchanges, exchangeJSON(vhostName, ex))
		}
		for _, q := range vh.Queues() {
			queues = append(queues, queueJSON(vhostName, q))
		}
		bindings = append(bindings, collectBindings(vhostName, vh)...)
	}

	if exchanges == nil {
		exchanges = []map[string]interface{}{}
	}
	if queues == nil {
		queues = []map[string]interface{}{}
	}
	if bindings == nil {
		bindings = []map[string]interface{}{}
	}

	// Build users.
	users := a.users.List()
	userList := make([]map[string]interface{}, 0, len(users))
	for _, u := range users {
		userList = append(userList, userJSON(u))
	}

	// Build permissions.
	var permissions []map[string]interface{}
	for _, u := range users {
		for vhost, perm := range u.Permissions {
			permissions = append(permissions, map[string]interface{}{
				"user":      u.Name,
				"vhost":     vhost,
				"configure": perm.Configure,
				"write":     perm.Write,
				"read":      perm.Read,
			})
		}
	}
	if permissions == nil {
		permissions = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"gomq_version": Version,
		"vhosts":       vhostList,
		"users":        userList,
		"exchanges":    exchanges,
		"queues":       queues,
		"bindings":     bindings,
		"permissions":  permissions,
	})
}

func (a *API) handlePostDefinitions(w http.ResponseWriter, r *http.Request) {
	var defs struct {
		Queues []struct {
			Name       string                 `json:"name"`
			VHost      string                 `json:"vhost"`
			Durable    bool                   `json:"durable"`
			AutoDelete bool                   `json:"auto_delete"`
			Arguments  map[string]interface{} `json:"arguments"`
		} `json:"queues"`
		Exchanges []struct {
			Name       string                 `json:"name"`
			VHost      string                 `json:"vhost"`
			Type       string                 `json:"type"`
			Durable    bool                   `json:"durable"`
			AutoDelete bool                   `json:"auto_delete"`
			Arguments  map[string]interface{} `json:"arguments"`
		} `json:"exchanges"`
		Bindings []struct {
			Source      string                 `json:"source"`
			VHost       string                 `json:"vhost"`
			Destination string                 `json:"destination"`
			RoutingKey  string                 `json:"routing_key"`
			Arguments   map[string]interface{} `json:"arguments"`
		} `json:"bindings"`
	}

	if err := json.NewDecoder(r.Body).Decode(&defs); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	// Import exchanges.
	for _, ex := range defs.Exchanges {
		vhostName := ex.VHost
		if vhostName == "" {
			vhostName = "/"
		}
		vh, ok := a.server.GetVHost(vhostName)
		if !ok {
			continue
		}
		// Best effort: skip errors on import.
		_, _ = vh.DeclareExchange(ex.Name, ex.Type, ex.Durable, ex.AutoDelete, ex.Arguments) //nolint:errcheck // best-effort import
	}

	// Import queues.
	for _, qdef := range defs.Queues {
		vhostName := qdef.VHost
		if vhostName == "" {
			vhostName = "/"
		}
		vh, ok := a.server.GetVHost(vhostName)
		if !ok {
			continue
		}
		_, _ = vh.DeclareQueue(qdef.Name, qdef.Durable, false, qdef.AutoDelete, qdef.Arguments) //nolint:errcheck // best-effort import
	}

	// Import bindings.
	for _, bdef := range defs.Bindings {
		vhostName := bdef.VHost
		if vhostName == "" {
			vhostName = "/"
		}
		vh, ok := a.server.GetVHost(vhostName)
		if !ok {
			continue
		}
		_ = vh.BindQueue(bdef.Destination, bdef.Source, bdef.RoutingKey, bdef.Arguments) //nolint:errcheck // best-effort import
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- Health ---

func (a *API) handleAlivenessTest(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")

	if _, ok := a.server.GetVHost(vhostName); !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleHealthCheck(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
