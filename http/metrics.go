package mgmt

import (
	"fmt"
	"net/http"
	"strings"
)

// handleMetrics returns Prometheus-format metrics for the broker.
func (a *API) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	var sb strings.Builder

	conns := a.server.Connections()
	vhosts := a.server.VHosts()

	// Connection count.
	sb.WriteString("# HELP gomq_connections Number of active connections.\n")
	sb.WriteString("# TYPE gomq_connections gauge\n")
	fmt.Fprintf(&sb, "gomq_connections %d\n", len(conns))

	// Channel count.
	var totalChannels int
	for _, c := range conns {
		totalChannels += c.ChannelCount()
	}
	sb.WriteString("# HELP gomq_channels Number of active channels.\n")
	sb.WriteString("# TYPE gomq_channels gauge\n")
	fmt.Fprintf(&sb, "gomq_channels %d\n", totalChannels)

	// Per-vhost queue metrics.
	sb.WriteString("# HELP gomq_queues_messages Number of messages in queue.\n")
	sb.WriteString("# TYPE gomq_queues_messages gauge\n")
	sb.WriteString("# HELP gomq_queues_consumers Number of consumers on queue.\n")
	sb.WriteString("# TYPE gomq_queues_consumers gauge\n")
	sb.WriteString("# HELP gomq_queues_published_total Total messages published to queue.\n")
	sb.WriteString("# TYPE gomq_queues_published_total counter\n")
	sb.WriteString("# HELP gomq_queues_delivered_total Total messages delivered from queue.\n")
	sb.WriteString("# TYPE gomq_queues_delivered_total counter\n")
	sb.WriteString("# HELP gomq_queues_acknowledged_total Total messages acknowledged on queue.\n")
	sb.WriteString("# TYPE gomq_queues_acknowledged_total counter\n")

	for vhostName, vh := range vhosts {
		for _, q := range vh.Queues() {
			labels := fmt.Sprintf(`vhost=%q,queue=%q`, vhostName, q.Name())
			fmt.Fprintf(&sb, "gomq_queues_messages{%s} %d\n", labels, q.Len())
			fmt.Fprintf(&sb, "gomq_queues_consumers{%s} %d\n", labels, q.ConsumerCount())
			fmt.Fprintf(&sb, "gomq_queues_published_total{%s} %d\n", labels, q.PublishCount())
			fmt.Fprintf(&sb, "gomq_queues_delivered_total{%s} %d\n", labels, q.DeliverCount())
			fmt.Fprintf(&sb, "gomq_queues_acknowledged_total{%s} %d\n", labels, q.AckCount())
		}
	}

	// Vhost and exchange counts.
	var totalExchanges, totalQueues int
	for _, vh := range vhosts {
		totalExchanges += len(vh.Exchanges())
		totalQueues += len(vh.Queues())
	}

	sb.WriteString("# HELP gomq_exchanges Number of exchanges.\n")
	sb.WriteString("# TYPE gomq_exchanges gauge\n")
	fmt.Fprintf(&sb, "gomq_exchanges %d\n", totalExchanges)

	sb.WriteString("# HELP gomq_queues Number of queues.\n")
	sb.WriteString("# TYPE gomq_queues gauge\n")
	fmt.Fprintf(&sb, "gomq_queues %d\n", totalQueues)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, sb.String())
}
