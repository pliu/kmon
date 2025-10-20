package kmon

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/phuslu/log"
	"github.com/pliu/datastructs/pkg/stats"
	"github.com/pliu/kmon/pkg/clients"
	"github.com/pliu/kmon/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Monitor struct {
	producerClient   clients.KgoClient
	producerTopic    string
	consumerClient   clients.KgoClient
	instanceUUID     string
	partitions       int
	p2bStats         map[int]*stats.Stats
	b2cStats         map[int]*stats.Stats
	e2eStats         map[int]*stats.Stats
	producerAckStats map[int]*stats.Stats
	sampleFrequency  time.Duration
	isMirror         bool
}

func NewMonitorWithClients(producerClient clients.KgoClient, producerTopic string, consumerClient clients.KgoClient, instanceUUID string, partitions int, sampleFrequency time.Duration, statsWindow time.Duration, isMirror bool) *Monitor {
	m := &Monitor{
		producerClient:  producerClient,
		producerTopic:   producerTopic,
		consumerClient:  consumerClient,
		instanceUUID:    instanceUUID,
		partitions:      partitions,
		sampleFrequency: sampleFrequency,
		isMirror:        isMirror,
	}
	m.p2bStats = make(map[int]*stats.Stats)
	m.b2cStats = make(map[int]*stats.Stats)
	m.e2eStats = make(map[int]*stats.Stats)
	m.producerAckStats = make(map[int]*stats.Stats)
	if m.isMirror {
		m.p2bStats[0] = stats.NewStats(statsWindow)
		m.b2cStats[0] = stats.NewStats(statsWindow)
		m.e2eStats[0] = stats.NewStats(statsWindow)
		m.producerAckStats[0] = stats.NewStats(statsWindow)
	} else {
		for p := range m.partitions {
			m.p2bStats[p] = stats.NewStats(statsWindow)
			m.b2cStats[p] = stats.NewStats(statsWindow)
			m.e2eStats[p] = stats.NewStats(statsWindow)
			m.producerAckStats[p] = stats.NewStats(statsWindow)
		}
	}
	return m
}

// TODO: Cross-cluster measurements should ignore partitions on e2e and not measure b2c
func NewMonitorFromConfig(cfg *config.KMonConfig, partitions int) (*Monitor, error) {
	var producerClient *kgo.Client
	var consumerClient *kgo.Client
	var err error
	isMirror := false

	if cfg.ConsumerKafkaConfig == nil {
		producerClient, err = clients.GetFranzGoClient(cfg.ProducerKafkaConfig, cfg.ProducerMonitoringTopic)
		if err != nil {
			return nil, err
		}
		consumerClient = producerClient
	} else {
		producerClient, err = clients.GetFranzGoClient(cfg.ProducerKafkaConfig)
		if err != nil {
			return nil, err
		}
		consumerClient, err = clients.GetFranzGoClient(cfg.ConsumerKafkaConfig, cfg.ConsumerMonitoringTopic)
		if err != nil {
			producerClient.Close()
			return nil, err
		}
		isMirror = true
	}

	instanceUUID := uuid.NewString()
	sampleFrequency := time.Duration(cfg.GetSampleFrequencyMs()) * time.Millisecond
	statsWindow := time.Duration(cfg.GetStatsWindowSeconds()) * time.Second

	return NewMonitorWithClients(producerClient, cfg.ProducerMonitoringTopic, consumerClient, instanceUUID, partitions, sampleFrequency, statsWindow, isMirror), nil
}

func (m *Monitor) Start(ctx context.Context) {
	defer m.producerClient.Close()
	if m.consumerClient != m.producerClient {
		defer m.consumerClient.Close()
	}
	log.Info().Msgf("Starting monitor instance %s", m.instanceUUID)

	m.warmup(ctx)

	go m.consumeLoop(ctx)
	go m.updateQuantilesLoop(ctx)

	ticker := time.NewTicker(m.sampleFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msgf("Stopping monitor instance %s", m.instanceUUID)
			return
		case <-ticker.C:
			m.publishProbeBatch(ctx)
		}
	}
}

func (m *Monitor) warmup(ctx context.Context) {
	m.publishProbeBatch(ctx)
	time.Sleep(3 * time.Second)
}

func (m *Monitor) publishProbeBatch(ctx context.Context) {
	for partition := range m.partitions {
		m.publishProbe(ctx, partition)
	}
}

func (m *Monitor) publishProbe(ctx context.Context, partition int) {
	sentAt := time.Now()
	record := &kgo.Record{
		Topic:     m.producerTopic,
		Partition: int32(partition),
		Key:       []byte(m.instanceUUID),
		Value:     fmt.Appendf(nil, "%d", sentAt.UnixNano()),
	}

	m.producerClient.Produce(ctx, record, func(r *kgo.Record, err error) {
		p := 0
		if !m.isMirror {
			p = int(r.Partition)
		}
		partitionLabel := m.partitionLabel(p)

		if err != nil {
			ProduceMessageFailureCount.WithLabelValues(partitionLabel).Inc()
			return
		}

		m.producerAckStats[p].Add(time.Since(sentAt).Milliseconds())
		ProduceMessageCount.WithLabelValues(partitionLabel).Inc()
	})
}

func (m *Monitor) consumeLoop(ctx context.Context) {
	for {
		fetches := m.consumerClient.PollFetches(ctx)

		select {
		case <-ctx.Done():
			return
		default:
			if fetches.IsClientClosed() {
				return
			}

			fetches.EachError(func(topic string, partition int32, err error) {
				ConsumeMessageCount.WithLabelValues(m.partitionLabel(int(partition))).Inc()
			})

			now := time.Now()
			fetches.EachRecord(func(record *kgo.Record) {
				m.handleConsumedRecord(record, now)
			})
		}
	}
}

func (m *Monitor) handleConsumedRecord(record *kgo.Record, consumeTime time.Time) {
	// Only process messages that were generated by this instance
	if string(record.Key) != m.instanceUUID {
		return
	}

	timestamp, err := strconv.ParseInt(string(record.Value), 10, 64)
	if err != nil {
		// TODO: Log, metric?
		return
	}
	sentAt := time.Unix(0, timestamp)

	partition := 0
	if !m.isMirror {
		partition = int(record.Partition)
	}
	partitionLabel := m.partitionLabel(partition)

	m.b2cStats[partition].Add(consumeTime.Sub(record.Timestamp).Milliseconds())
	m.e2eStats[partition].Add(consumeTime.Sub(sentAt).Milliseconds())
	m.p2bStats[partition].Add(record.Timestamp.Sub(sentAt).Milliseconds())

	ConsumeMessageCount.WithLabelValues(partitionLabel).Inc()
}

func (m *Monitor) partitionLabel(partition int) string {
	return fmt.Sprintf("%d", partition)
}

func (m *Monitor) updateQuantilesLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			loopOver := 1
			if !m.isMirror {
				loopOver = m.partitions
			}
			for partition := range loopOver {
				partitionLabel := m.partitionLabel(partition)
				m.updateQuantiles(m.e2eStats[partition], E2EMessageLatencyQuantile, partitionLabel)
				m.updateQuantiles(m.p2bStats[partition], P2BMessageLatencyQuantile, partitionLabel)
				m.updateQuantiles(m.b2cStats[partition], B2CMessageLatencyQuantile, partitionLabel)
				m.updateQuantiles(m.producerAckStats[partition], ProducerAckLatencyQuantile, partitionLabel)
			}
		}
	}
}

func (m *Monitor) updateQuantiles(stats *stats.Stats, gauge *prometheus.GaugeVec, partitionLabel string) {
	percentiles := []float64{50, 99}
	res, ok := stats.Percentile(percentiles)
	if !ok {
		return
	}
	for i, val := range percentiles {
		gauge.WithLabelValues(partitionLabel, fmt.Sprintf("p%d", int(val))).Set(float64(res[i]))
	}
}
