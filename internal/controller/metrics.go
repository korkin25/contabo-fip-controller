package controller

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the controller's Prometheus metrics.
type Metrics struct {
	Leader          prometheus.Gauge
	Eligible        *prometheus.GaugeVec
	EligibleNodes   prometheus.Gauge
	Holder          *prometheus.GaugeVec
	Reassignments   *prometheus.CounterVec
	APIErrors       *prometheus.CounterVec
	ReconcileErrors prometheus.Counter
	LastReconcile   prometheus.Gauge
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "contabo_fip_leader",
			Help: "1 when this replica holds the leader lease.",
		}),
		Eligible: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "contabo_fip_node_eligible",
			Help: "1 when every watched Service is healthy on the node.",
		}, []string{"node"}),
		EligibleNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "contabo_fip_eligible_nodes",
			Help: "Number of nodes that may hold the floating IP.",
		}),
		Holder: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "contabo_fip_holder",
			Help: "1 for the instance the floating IP is assigned to; node is empty when the instance is not a known node.",
		}, []string{"node", "instance_id"}),
		Reassignments: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "contabo_fip_reassignments_total",
			Help: "Attempts to move the floating IP by result (success, failure, dry_run).",
		}, []string{"result"}),
		APIErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "contabo_fip_api_errors_total",
			Help: "Failed Contabo API calls by operation (get, assign, unassign).",
		}, []string{"op"}),
		ReconcileErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "contabo_fip_reconcile_errors_total",
			Help: "Cycles that could not evaluate the cluster state.",
		}),
		LastReconcile: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "contabo_fip_last_reconcile_timestamp_seconds",
			Help: "Unix time of the last completed cycle.",
		}),
	}
	reg.MustRegister(m.Leader, m.Eligible, m.EligibleNodes, m.Holder, m.Reassignments,
		m.APIErrors, m.ReconcileErrors, m.LastReconcile)
	return m
}
