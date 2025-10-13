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
	numPartitions, err := kmon.topicManager.getTopicNumPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, numPartitions)

	time.Sleep(15 * time.Second)

	for partition := range kmon.monitor.partitions {
		require.Greater(t, kmon.monitor.e2eStats[partition].Len(), 0)
		require.Greater(t, kmon.monitor.b2cStats[partition].Len(), 0)
		require.Greater(t, kmon.monitor.p2bStats[partition].Len(), 0)
		require.Greater(t, kmon.monitor.producerAckStats[partition].Len(), 0)
	}

	_, _ = kmon.topicManager.admClient.DeleteTopics(ctx, topic)
	kmon.topicManager.waitUntilTopicNoLongerExists(ctx)
	kmon.topicManager.waitUntilTopicExists(ctx)
	time.Sleep(1 * time.Second)
	second_uuid := kmon.monitor.instanceUUID
	require.NotEqual(t, first_uuid, second_uuid)
	numPartitions, err = kmon.topicManager.getTopicNumPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, numPartitions)

	minInsyncReplicas := "1"
	topicConfigs := map[string]*string{
		"min.insync.replicas": &minInsyncReplicas,
	}

	_, _ = kmon.topicManager.admClient.DeleteTopics(ctx, topic)
	kmon.topicManager.waitUntilTopicNoLongerExists(ctx)
	_, _ = kmon.topicManager.admClient.CreateTopics(ctx, 4, 1, topicConfigs, topic)
	defer kmon.topicManager.admClient.DeleteTopics(ctx, topic)
	kmon.topicManager.waitUntilTopicExists(ctx)
	numPartitions, err = kmon.topicManager.getTopicNumPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 4, numPartitions)

	time.Sleep(25 * time.Second)

	third_uuid := kmon.monitor.instanceUUID
	require.NotEqual(t, second_uuid, third_uuid)
	numPartitions, err = kmon.topicManager.getTopicNumPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, numPartitions)
	for partition := range kmon.monitor.partitions {
		require.Greater(t, kmon.monitor.e2eStats[partition].Len(), 0)
		require.Greater(t, kmon.monitor.b2cStats[partition].Len(), 0)
		require.Greater(t, kmon.monitor.p2bStats[partition].Len(), 0)
		require.Greater(t, kmon.monitor.producerAckStats[partition].Len(), 0)
	}
}
