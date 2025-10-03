package kmon

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	E2EMessageLatencyHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kmon_e2e_message_latency_ms",
			Help:    "Latency of e2e message delivery in milliseconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		},
		[]string{"partition"},
	)
	P2BMessageLatencyHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kmon_p2b_message_latency_ms",
			Help:    "Latency of producer-to-broker message ack latency in milliseconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		},
		[]string{"partition"},
	)
	B2CMessageLatencyHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kmon_b2c_message_latency_ms",
			Help:    "Latency of broker-to-consumer message delivery in milliseconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 15),
		},
		[]string{"partition"},
	)
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
	ProduceFailureCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kmon_produce_failure_count",
			Help: "Total number of produce failures",
		},
		[]string{"partition"},
	)
	MonitoringTopicPartitionCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kmon_monitoring_topic_partition_count",
			Help: "Number of partitions in the monitoring topic",
		},
	)
)
