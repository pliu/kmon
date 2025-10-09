//go:build integration

package kmon

import (
	"context"
	"testing"
	"time"

	"github.com/pliu/kmon/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestKMonIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	topic := "kmon-integration"
	data := []byte(`{
		"producerKafkaConfig": {
			"seedBrokers": [
				"localhost:10000"
			]
		},
		"producerMonitoringTopic": "` + topic + `",
		"sampleFrequencyMs": 200,
		"topicReconciliationFrequencyMin": 1
	}`)
	cfg, err := config.GetKMonConfigFromBytes(&data)
	require.NoError(t, err)

	kmon, err := NewKMonFromConfig(cfg, ctx)
	require.NoError(t, err)
	kmon.topicManager.reconciliationInterval = 20 * time.Second

	_, _ = kmon.topicManager.admClient.DeleteTopics(ctx, topic)
	kmon.topicManager.waitUntilTopicNoLongerExists(ctx)

	go kmon.Start()

	kmon.topicManager.waitUntilTopicExists(ctx)
	time.Sleep(1 * time.Second)

	first_uuid := kmon.monitor.instanceUUID
	partitions, err := kmon.topicManager.getTopicPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(partitions))

	time.Sleep(15 * time.Second)

	require.NotEmpty(t, kmon.monitor.partitionStats)
	for _, ps := range kmon.monitor.partitionStats {
		require.Greater(t, ps.e2e.Len(), 0)
		require.Greater(t, ps.p2b.Len(), 0)
		require.Greater(t, ps.b2c.Len(), 0)
	}

	_, _ = kmon.topicManager.admClient.DeleteTopics(ctx, topic)
	kmon.topicManager.waitUntilTopicNoLongerExists(ctx)
	kmon.topicManager.waitUntilTopicExists(ctx)
	time.Sleep(1 * time.Second)
	second_uuid := kmon.monitor.instanceUUID
	require.NotEqual(t, first_uuid, second_uuid)
	partitions, err = kmon.topicManager.getTopicPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(partitions))

	minInsyncReplicas := "1"
	topicConfigs := map[string]*string{
		"min.insync.replicas": &minInsyncReplicas,
	}

	_, _ = kmon.topicManager.admClient.DeleteTopics(ctx, topic)
	kmon.topicManager.waitUntilTopicNoLongerExists(ctx)
	_, _ = kmon.topicManager.admClient.CreateTopics(ctx, 4, 1, topicConfigs, topic)
	defer kmon.topicManager.admClient.DeleteTopics(ctx, topic)
	kmon.topicManager.waitUntilTopicExists(ctx)
	partitions, err = kmon.topicManager.getTopicPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, len(partitions))

	time.Sleep(25 * time.Second)

	third_uuid := kmon.monitor.instanceUUID
	require.NotEqual(t, second_uuid, third_uuid)
	partitions, err = kmon.topicManager.getTopicPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(partitions))
	require.NotEmpty(t, kmon.monitor.partitionStats)
	for _, ps := range kmon.monitor.partitionStats {
		require.Greater(t, ps.e2e.Len(), 0)
		require.Greater(t, ps.p2b.Len(), 0)
		require.Greater(t, ps.b2c.Len(), 0)
	}
}
