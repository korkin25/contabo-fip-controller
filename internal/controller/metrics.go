package controller

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics are the controller's Prometheus metrics.
//
// Eligible, EligibleNodes, Holder and LastReconcile describe the cluster as
// the reconcile loop sees it. Only the replica that runs the loop (the
// leader) exports them; a follower would report zeros or a stale view.
type Metrics struct {
	Leader          prometheus.Gauge
	Eligible        *prometheus.GaugeVec
	EligibleNodes   prometheus.Gauge
	Holder          *prometheus.GaugeVec
	Reassignments   *prometheus.CounterVec
	APIErrors       *prometheus.CounterVec
	ReconcileErrors prometheus.Counter
	LastReconcile   prometheus.Gauge

	leaderOnly *gated
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
			Help: "1 when every watched Service is healthy on the node. Leader only.",
		}, []string{"node"}),
		EligibleNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "contabo_fip_eligible_nodes",
			Help: "Number of nodes that may hold the floating IP. Leader only.",
		}),
		Holder: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "contabo_fip_holder",
			Help: "1 for the instance the floating IP is assigned to; node is empty when the instance is not a known node. Leader only.",
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
			Help: "Unix time of the last successful cycle. Leader only.",
		}),
	}
	m.leaderOnly = &gated{cs: []prometheus.Collector{m.Eligible, m.EligibleNodes, m.Holder, m.LastReconcile}}
	reg.MustRegister(m.Leader, m.Reassignments, m.APIErrors, m.ReconcileErrors, m.leaderOnly)
	return m
}

// setActive starts or stops exporting the leader-only metrics. Stopping
// also clears them, so a later takeover does not show the old view.
func (m *Metrics) setActive(on bool) {
	m.leaderOnly.on.Store(on)
	if !on {
		m.Eligible.Reset()
		m.Holder.Reset()
		m.EligibleNodes.Set(0)
		m.LastReconcile.Set(0)
	}
}

// gated collects its collectors only while on.
type gated struct {
	on atomic.Bool
	cs []prometheus.Collector
}

func (g *gated) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range g.cs {
		c.Describe(ch)
	}
}

func (g *gated) Collect(ch chan<- prometheus.Metric) {
	if !g.on.Load() {
		return
	}
	for _, c := range g.cs {
		c.Collect(ch)
	}
}
