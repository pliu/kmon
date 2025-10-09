//go:build integration

package kmon

import (
	"context"
	"testing"

	"github.com/pliu/kmon/pkg/config"
	"github.com/stretchr/testify/require"
)

func setupTopicManager(t *testing.T, topic string) (*TopicManager, context.Context) {
	ctx := context.Background()
	cfg := &config.KMonConfig{
		ProducerMonitoringTopic: topic,
		ProducerKafkaConfig: &config.KafkaConfig{
			SeedBrokers: []string{"localhost:10000"},
		},
	}

	tm, err := NewTopicManagerFromConfig(cfg)
	require.NoError(t, err)
	tm.changeDetectedCallback = func() {}
	tm.doneReconcilingCallback = func(i int) {}

	t.Cleanup(func() {
		_, _ = tm.admClient.DeleteTopics(ctx, topic)
		tm.admClient.Close()
	})

	return tm, ctx
}

func TestTopicManagerMaybeReconcileTopicNoTopic(t *testing.T) {
	topic := "kmon-create"
	tm, ctx := setupTopicManager(t, topic)

	_, _ = tm.admClient.DeleteTopics(ctx, topic)
	tm.waitUntilTopicNoLongerExists(ctx)

	require.NoError(t, tm.maybeReconcileTopic(ctx))

	partitions, err := tm.getTopicPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(partitions))
}

func TestTopicManagerMaybeReconcileTopicIncorrectTopic(t *testing.T) {
	topic := "kmon-incorrect"
	tm, ctx := setupTopicManager(t, topic)

	_, _ = tm.admClient.DeleteTopics(ctx, topic)
	tm.waitUntilTopicNoLongerExists(ctx)

	_, err := tm.admClient.CreateTopics(context.Background(), 1, 1, nil, topic)
	require.NoError(t, err)
	tm.waitUntilTopicExists(ctx)

	partitions, err := tm.getTopicPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(partitions))

	require.NoError(t, tm.maybeReconcileTopic(ctx))
	partitions, err = tm.getTopicPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(partitions))
}
