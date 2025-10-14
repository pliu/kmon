//go:build integration

package kmon

import (
	"context"
	"testing"
	"time"

	"github.com/pliu/kmon/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMonitorIntegration(t *testing.T) {
	seedBrokers := []string{"localhost:10000"}
	topic := "kmon-monitor"

	client, err := kgo.NewClient(kgo.SeedBrokers(seedBrokers...))
	require.NoError(t, err)
	adminClient := kadm.NewClient(client)
	defer adminClient.Close()
	partitions := 3
	_, err = adminClient.CreateTopics(context.Background(), int32(partitions), 3, nil, topic)
	require.NoError(t, err)
	defer adminClient.DeleteTopics(context.Background(), topic)

	// Give the topic time to be created
	time.Sleep(1 * time.Second)

	cfg := &config.KMonConfig{
		ProducerMonitoringTopic: topic,
		ProducerKafkaConfig: &config.KafkaConfig{
			SeedBrokers: seedBrokers,
		},
		SampleFrequencyMs:  50,
		StatsWindowSeconds: 10,
	}
	m, err := NewMonitorFromConfig(cfg, partitions)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go m.Start(ctx)

	// Wait for some probes to be sent and consumed
	time.Sleep(14 * time.Second)

	for partition := range partitions {
		require.Greater(t, m.e2eStats[partition].Len(), 0)
		require.Greater(t, m.b2cStats[partition].Len(), 0)
		require.Greater(t, m.p2bStats[partition].Len(), 0)
		require.Greater(t, m.producerAckStats[partition].Len(), 0)

		avg, ok := m.e2eStats[partition].Average()
		require.True(t, ok)
		percentiles, ok := m.e2eStats[partition].Percentile([]float64{50, 99})
		require.True(t, ok)
		t.Logf("Data points [%d]: %d", partition, m.e2eStats[partition].Len())
		t.Logf("Average latency [%d]: %.2fms", partition, avg)
		t.Logf("Median latency [%d]: %dms", partition, percentiles[0])
		t.Logf("p99 latency [%d]: %dms", partition, percentiles[1])
	}
}

func TestMonitorIntegrationMirrored(t *testing.T) {
	seedBrokers := []string{"localhost:10000"}
	topic := "kmon-monitor"

	client, err := kgo.NewClient(kgo.SeedBrokers(seedBrokers...))
	require.NoError(t, err)
	adminClient := kadm.NewClient(client)
	defer adminClient.Close()
	partitions := 3
	_, err = adminClient.CreateTopics(context.Background(), int32(partitions), 3, nil, topic)
	require.NoError(t, err)
	defer adminClient.DeleteTopics(context.Background(), topic)

	// Give the topic time to be created
	time.Sleep(1 * time.Second)

	cfg := &config.KMonConfig{
		ProducerMonitoringTopic: topic,
		ConsumerMonitoringTopic: topic,
		ProducerKafkaConfig: &config.KafkaConfig{
			SeedBrokers: seedBrokers,
		},
		ConsumerKafkaConfig: &config.KafkaConfig{
			SeedBrokers: seedBrokers,
		},
		SampleFrequencyMs:  50,
		StatsWindowSeconds: 10,
	}
	m, err := NewMonitorFromConfig(cfg, partitions)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go m.Start(ctx)

	// Wait for some probes to be sent and consumed
	time.Sleep(14 * time.Second)

	require.Equal(t, 1, len(m.e2eStats))
	require.Equal(t, 1, len(m.b2cStats))
	require.Equal(t, 1, len(m.p2bStats))
	require.Equal(t, 1, len(m.producerAckStats))
	require.Greater(t, m.e2eStats[0].Len(), 0)
	require.Greater(t, m.b2cStats[0].Len(), 0)
	require.Greater(t, m.p2bStats[0].Len(), 0)
	require.Greater(t, m.producerAckStats[0].Len(), 0)

	avg, ok := m.e2eStats[0].Average()
	require.True(t, ok)
	percentiles, ok := m.e2eStats[0].Percentile([]float64{50, 99})
	require.True(t, ok)
	t.Logf("Data points: %d", m.e2eStats[0].Len())
	t.Logf("Average latency: %.2fms", avg)
	t.Logf("Median latenc: %dms", percentiles[0])
	t.Logf("p99 latency: %dms", percentiles[1])
}
