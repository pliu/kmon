//go:build integration

package kmon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

const integrationBroker = "localhost:10000"

func setupTopicManager(t *testing.T, topic string) (*TopicManager, context.Context) {
	ctx := context.Background()
	client, err := kgo.NewClient(kgo.SeedBrokers(integrationBroker))

	require.NoError(t, err)

	tm := NewTopicManagerWithClients(client, topic, time.Second)
	tm.changeDetectedCallback = func() {}
	tm.doneReconcilingCallback = func() {}

	t.Cleanup(func() {
		_, _ = tm.admClient.DeleteTopics(ctx, topic)
		tm.admClient.Close()
	})

	return tm, ctx
}

func TestTopicManagerMaybeReconcileTopicNoTopic(t *testing.T) {
	topic := fmt.Sprintf("kmon-create-topic-%d", time.Now().UnixNano())
	tm, ctx := setupTopicManager(t, topic)

	_, _ = tm.admClient.DeleteTopics(ctx, topic)
	tm.waitUntilTopicNoLongerExists(ctx)

	require.NoError(t, tm.maybeReconcileTopic(ctx))

	partitions, err := tm.getTopicPartitions(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, len(partitions))
}

func TestTopicManagerMaybeReconcileTopicIncorrectTopic(t *testing.T) {
	topic := fmt.Sprintf("kmon-incorrect-topic-%d", time.Now().UnixNano())
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
