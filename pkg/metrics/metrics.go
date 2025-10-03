package metrics

import (
	prom "github.com/prometheus/client_golang/prometheus"
)

var (
	E2EMessageLatencyHistogram = prom.NewHistogramVec(
		prom.HistogramOpts{
			Name:    "kmon_e2e_message_latency_ms",
			Help:    "Latency of e2e message delivery in milliseconds",
			Buckets: prom.ExponentialBuckets(1, 2, 30000),
		},
		[]string{"partition"},
	)
	P2BMessageLatencyHistogram = prom.NewHistogramVec(
		prom.HistogramOpts{
			Name:    "kmon_p2b_message_latency_ms",
			Help:    "Latency of producer-to-broker message ack latency in milliseconds",
			Buckets: prom.ExponentialBuckets(1, 2, 30000),
		},
		[]string{"partition"},
	)
	B2CMessageLatencyHistogram = prom.NewHistogramVec(
		prom.HistogramOpts{
			Name:    "kmon_e2e_message_latency_ms",
			Help:    "Latency of e2e message delivery in milliseconds",
			Buckets: prom.ExponentialBuckets(1, 2, 30000),
		},
		[]string{"partition"},
	)
	E2EMessageLatencyQuantile = prom.NewGaugeVec(
		prom.GaugeOpts{
			Name: "kmon_e2e_message_latency_quantile",
			Help: "Quantile of e2e message delivery latency in milliseconds",
		},
		[]string{"partition", "quantile"},
	)
	P2BMessageLatencyQuantile = prom.NewGaugeVec(
		prom.GaugeOpts{
			Name: "kmon_p2b_message_latency_quantile",
			Help: "Quantile of producer-to-broker message ack latency in milliseconds",
		},
		[]string{"partition", "quantile"},
	)
	B2CMessageLatencyQuantile = prom.NewGaugeVec(
		prom.GaugeOpts{
			Name: "kmon_b2c_message_latency_quantile",
			Help: "Quantile of broker-to-consumer message delivery latency in milliseconds",
		},
		[]string{"partition", "quantile"},
	)
	ProduceFailureCount = prom.NewCounterVec(
		prom.CounterOpts{
			Name: "kmon_produce_failure_count",
			Help: "Total number of produce failures",
		},
		[]string{"partition"},
	)
	MonitoringTopicPartitionCount = prom.NewGauge(
		prom.GaugeOpts{
			Name: "kmon_monitoring_topic_partition_count",
			Help: "Number of partitions in the monitoring topic",
		},
	)
)

func Init() {
	prom.MustRegister(E2EMessageLatencyHistogram)
	prom.MustRegister(P2BMessageLatencyHistogram)
	prom.MustRegister(B2CMessageLatencyHistogram)
	prom.MustRegister(E2EMessageLatencyQuantile)
	prom.MustRegister(P2BMessageLatencyQuantile)
	prom.MustRegister(B2CMessageLatencyQuantile)
	prom.MustRegister(ProduceFailureCount)
	prom.MustRegister(MonitoringTopicPartitionCount)
}

