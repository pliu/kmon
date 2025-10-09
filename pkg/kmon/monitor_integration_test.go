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
	partitions := []int32{0, 1, 2}
	_, err = adminClient.CreateTopics(context.Background(), int32(len(partitions)), 3, nil, topic)
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

	require.NotEmpty(t, m.partitionStats)
	for p, ps := range m.partitionStats {
		require.Greater(t, ps.e2e.Len(), 0)
		require.Greater(t, ps.p2b.Len(), 0)
		require.Greater(t, ps.b2c.Len(), 0)

		avg, ok := ps.e2e.Average()
		require.True(t, ok)
		percentiles, ok := ps.e2e.Percentile([]float64{50, 99})
		require.True(t, ok)
		t.Logf("Data points [%d]: %d", p, ps.e2e.Len())
		t.Logf("Average latency [%d]: %.2fms", p, avg)
		t.Logf("Median latency [%d]: %dms", p, percentiles[0])
		t.Logf("p99 latency [%d]: %dms", p, percentiles[1])
	}
}
