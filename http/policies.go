package mgmt

import (
	"encoding/json"
	"net/http"

	"github.com/yaklabco/gomq/broker"
)

// --- Policies ---

func (a *API) handleListAllPolicies(w http.ResponseWriter, _ *http.Request) {
	vhosts := a.server.VHosts()

	var result []map[string]interface{}
	for vhostName, vh := range vhosts {
		for _, p := range vh.Policies.ListPolicies() {
			result = append(result, policyJSON(vhostName, p))
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleListPolicies(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	policies := vh.Policies.ListPolicies()
	result := make([]map[string]interface{}, 0, len(policies))
	for _, p := range policies {
		result = append(result, policyJSON(vhostName, p))
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleGetPolicy(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	policy, ok := vh.Policies.GetPolicy(name)
	if !ok {
		writeError(w, http.StatusNotFound, "policy not found")
		return
	}

	writeJSON(w, http.StatusOK, policyJSON(vhostName, policy))
}

func (a *API) handlePutPolicy(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	var body struct {
		Pattern    string                 `json:"pattern"`
		ApplyTo    string                 `json:"apply-to"`
		Priority   int                    `json:"priority"`
		Definition map[string]interface{} `json:"definition"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	if body.Pattern == "" {
		writeError(w, http.StatusBadRequest, "pattern is required")
		return
	}

	if body.ApplyTo == "" {
		body.ApplyTo = broker.PolicyApplyAll
	}

	policy := &broker.Policy{
		Name:       name,
		Pattern:    body.Pattern,
		ApplyTo:    body.ApplyTo,
		Priority:   body.Priority,
		Definition: body.Definition,
	}

	if err := vh.Policies.SetPolicy(name, policy); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (a *API) handleDeletePolicy(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	if err := vh.Policies.DeletePolicy(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func policyJSON(vhost string, policy *broker.Policy) map[string]interface{} {
	def := policy.Definition
	if def == nil {
		def = map[string]interface{}{}
	}

	return map[string]interface{}{
		"name":       policy.Name,
		"vhost":      vhost,
		"pattern":    policy.Pattern,
		"apply-to":   policy.ApplyTo,
		"priority":   policy.Priority,
		"definition": def,
	}
}
