package kmon

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	E2EMessageLatencyQuantile = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kmon_e2e_message_latency_quantile",
			Help: "Quantile of e2e message delivery latency in milliseconds",
		},
		[]string{"partition", "quantile"},
	)
	P2BMessageLatencyQuantile = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kmon_p2b_message_latency_quantile",
			Help: "Quantile of producer-to-broker message ack latency in milliseconds",
		},
		[]string{"partition", "quantile"},
	)
	B2CMessageLatencyQuantile = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kmon_b2c_message_latency_quantile",
			Help: "Quantile of broker-to-consumer message delivery latency in milliseconds",
		},
		[]string{"partition", "quantile"},
	)
	ProduceMessageCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kmon_produce_message_count",
			Help: "Total number of produced messages",
		},
		[]string{"partition"},
	)
	ConsumeMessageCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kmon_consume_message_count",
			Help: "Total number of consumed messages",
		},
		[]string{"partition"},
	)
	ProduceMessageFailureCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kmon_produce_message_failure_count",
			Help: "Total number of produce message failures",
		},
		[]string{"partition"},
	)
)
