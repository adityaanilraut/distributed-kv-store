package raft

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	elections prometheus.Counter
	latency   prometheus.Histogram
	lag       *prometheus.GaugeVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		elections: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raft_leader_elections_total", Help: "Elections won by this node.",
		}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "raft_commit_seconds", Help: "Time from proposal to committed application.",
			Buckets: prometheus.ExponentialBuckets(.0005, 2, 13),
		}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_replication_lag_entries", Help: "Entries between the leader and each peer.",
		}, []string{"peer"}),
	}
	if reg != nil {
		reg.MustRegister(m.elections, m.latency, m.lag)
	}
	return m
}

func label(id uint64) string { return strconv.FormatUint(id, 10) }
