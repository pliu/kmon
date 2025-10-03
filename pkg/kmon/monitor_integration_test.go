//go:build kafka

package kmon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pliu/kmon/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMonitorIntegration(t *testing.T) {
	// This test requires a running Kafka instance
	seedBrokers := []string{"localhost:9092"}
	topic := fmt.Sprintf("test-topic-%d", time.Now().UnixNano())

	// Create a topic for the test
	client, err := kgo.NewClient(kgo.SeedBrokers(seedBrokers...))
	assert.NoError(t, err)
	defer client.Close()
	adminClient := kadm.NewClient(client)
	_, err = adminClient.CreateTopics(context.Background(), 1, 1, nil, topic)
	assert.NoError(t, err)
	defer adminClient.DeleteTopics(context.Background(), topic)

	// Give the topic time to be created
	time.Sleep(1 * time.Second)

	// Create a Monitor instance
	cfg := &config.KMonConfig{
		ProducerMonitoringTopic: topic,
		ProducerKafkaConfig: &config.KafkaConfig{
			SeedBrokers: seedBrokers,
		},
		SampleFrequencyMs: 50,
	}
	m, err := NewMonitorFromConfig(cfg)
	assert.NoError(t, err)

	// Start Monitor in a goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go m.Start(ctx)

	// Wait for some probes to be sent and consumed
	time.Sleep(8 * time.Second)

	// Check that the partition stats have been updated
	assert.NotEmpty(t, m.partitionStats)
	for p, ps := range m.partitionStats {
		assert.Greater(t, ps.e2e.Len(), 0)
		assert.Greater(t, ps.p2b.Len(), 0)

		avg, ok := ps.e2e.Average()
		assert.True(t, ok)
		percentiles, ok := ps.e2e.Percentile([]float64{50, 99})
		assert.True(t, ok)
		t.Logf("Data points [%d]: %d", p, ps.e2e.Len())
		t.Logf("Average latency [%d]: %.2fms", p, avg)
		t.Logf("Median latency [%d]: %dms", p, percentiles[0])
		t.Logf("p99 latency [%d]: %dms", p, percentiles[1])
	}
}
