package mgmt

import (
	"encoding/json"
	"net/http"

	"github.com/jamesainslie/gomq/shovel"
)

// --- Shovels ---

func (a *API) handleListShovels(w http.ResponseWriter, _ *http.Request) {
	configs := a.shovels.List()

	result := make([]map[string]interface{}, 0, len(configs))
	for _, cfg := range configs {
		result = append(result, map[string]interface{}{
			"name":        cfg.Name,
			"vhost":       cfg.VHost,
			"src_uri":     cfg.SrcURI,
			"src_queue":   cfg.SrcQueue,
			"dest_uri":    cfg.DestURI,
			"dest_type":   cfg.DestType,
			"exchange":    cfg.Exchange,
			"routing_key": cfg.RoutingKey,
			"ack_mode":    cfg.AckMode,
			"prefetch":    cfg.Prefetch,
			"status":      a.shovels.ShovelStatus(cfg.Name),
		})
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handlePutShovel(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	var body struct {
		SrcURI     string `json:"src_uri"`
		SrcQueue   string `json:"src_queue"`
		DestURI    string `json:"dest_uri"`
		DestType   string `json:"dest_type"`
		Exchange   string `json:"exchange"`
		RoutingKey string `json:"routing_key"`
		HTTPMethod string `json:"http_method"`
		AckMode    string `json:"ack_mode"`
		Prefetch   int    `json:"prefetch"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	cfg := &shovel.ShovelConfig{
		Name:       name,
		SrcURI:     body.SrcURI,
		SrcQueue:   body.SrcQueue,
		DestURI:    body.DestURI,
		DestType:   body.DestType,
		Exchange:   body.Exchange,
		RoutingKey: body.RoutingKey,
		HTTPMethod: body.HTTPMethod,
		AckMode:    body.AckMode,
		Prefetch:   body.Prefetch,
		VHost:      vhostName,
	}

	if err := a.shovels.Add(cfg); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (a *API) handleDeleteShovel(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	if err := a.shovels.Delete(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// --- Federation Links ---

func (a *API) handleListFederationLinks(w http.ResponseWriter, _ *http.Request) {
	statuses := a.shovels.FederationLinkStatuses()

	result := make([]map[string]interface{}, 0, len(statuses))
	for _, st := range statuses {
		result = append(result, map[string]interface{}{
			"name":     st.Name,
			"vhost":    st.VHost,
			"exchange": st.Exchange,
			"uri":      st.URI,
			"max_hops": st.MaxHops,
			"status":   st.Status,
		})
	}

	writeJSON(w, http.StatusOK, result)
}

func (a *API) handlePutFederationLink(w http.ResponseWriter, r *http.Request) {
	vhostName := pathParam(r, "vhost")
	name := pathParam(r, "name")

	var body struct {
		URI           string `json:"uri"`
		Exchange      string `json:"exchange"`
		LocalExchange string `json:"local_exchange"`
		MaxHops       int    `json:"max_hops"`
		Prefetch      int    `json:"prefetch"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}

	cfg := &shovel.FederationConfig{
		Name:          name,
		URI:           body.URI,
		Exchange:      body.Exchange,
		LocalExchange: body.LocalExchange,
		MaxHops:       body.MaxHops,
		Prefetch:      body.Prefetch,
		VHost:         vhostName,
	}

	if err := a.shovels.AddFederationLink(cfg); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (a *API) handleDeleteFederationLink(w http.ResponseWriter, r *http.Request) {
	name := pathParam(r, "name")

	if err := a.shovels.DeleteFederationLink(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
