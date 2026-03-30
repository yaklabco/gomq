package mgmt

import (
	"encoding/json"
	"net/http"
)

// --- Vhost Limits ---

func (a *API) handleListAllVhostLimits(w http.ResponseWriter, _ *http.Request) {
	vhosts := a.server.VHosts()

	var result []map[string]interface{}
	for name, vh := range vhosts {
		maxConn, maxQ := vh.Limits()
		if maxConn > 0 || maxQ > 0 {
			result = append(result, vhostLimitsJSON(name, maxConn, maxQ))
		}
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handleGetVhostLimits(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	maxConn, maxQ := vh.Limits()
	writeJSON(w, http.StatusOK, vhostLimitsJSON(vhostName, maxConn, maxQ))
}

func (a *API) handlePutVhostLimits(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	var body struct {
		MaxConnections int `json:"max-connections"`
		MaxQueues      int `json:"max-queues"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	vh.SetLimits(body.MaxConnections, body.MaxQueues)
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handleDeleteVhostLimits(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")

	vh, ok := a.server.GetVHost(vhostName)
	if !ok {
		writeError(w, http.StatusNotFound, "vhost not found")
		return
	}

	vh.SetLimits(0, 0)
	w.WriteHeader(http.StatusNoContent)
}

func vhostLimitsJSON(vhost string, maxConn, maxQueues int) map[string]interface{} {
	return map[string]interface{}{
		"vhost": vhost,
		"value": map[string]int{
			"max-connections": maxConn,
			"max-queues":      maxQueues,
		},
	}
}
