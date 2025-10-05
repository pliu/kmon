//go:build integration

package clients

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pliu/kmon/pkg/config"
	"github.com/pliu/kmon/pkg/utils"
)

const (
	kafkaBroker       = "localhost:10000"
	numWarmupMessages = 10
)

func TestKafka_WriteAndConsume_MultipleTopics(t *testing.T) {
	runWriteAndConsumeTest(t, 5, 200)
}

func TestKafka_WriteAndConsume_SingleTopic_Serial(t *testing.T) {
	runWriteAndConsumeTest(t, 1, 1000)
}

func runWriteAndConsumeTest(t *testing.T, numTopics int, numMessages int) {
	// Create a mock config for testing
	mockConfig := &config.KafkaConfig{
		SeedBrokers: []string{kafkaBroker},
	}

	// 1. Create a new admin client
	cl, err := GetFranzGoClient(mockConfig)
	if err != nil {
		t.Fatalf("failed to create kafka client: %v", err)
	}
	adm := kadm.NewClient(cl)
	defer adm.Close()

	// 2. Create topics
	topicNames := make([]string, numTopics)
	for i := 0; i < numTopics; i++ {
		topicNames[i] = fmt.Sprintf("test-topic-%d-%d-%d", time.Now().UnixNano(), numTopics, i)
	}

	_, err = adm.CreateTopics(context.Background(), 20, 1, nil, topicNames...)
	if err != nil {
		t.Fatalf("failed to create topics: %v", err)
	}
	defer func() {
		_, err := adm.DeleteTopics(context.Background(), topicNames...)
		if err != nil {
			t.Logf("failed to delete topics: %v", err)
		}
	}()

	// 3. Warm-up
	t.Logf("Warming up client and %d topic(s)...", numTopics)
	var wgWarmup sync.WaitGroup
	for i := 0; i < numTopics; i++ {
		wgWarmup.Add(1)
		go func(topic string) {
			defer wgWarmup.Done()
			for j := 0; j < numWarmupMessages; j++ {
				cl.Produce(context.Background(), &kgo.Record{
					Topic: topic,
					Value: []byte(fmt.Sprintf("warmup-%d", j)),
				}, nil) // No callback needed for warmup
			}
		}(topicNames[i])
	}
	wgWarmup.Wait()
	if err := cl.Flush(context.Background()); err != nil {
		t.Fatalf("failed to flush warmup messages: %v", err)
	}
	t.Log("Warm-up complete.")

	// 4. Write messages
	latencyMap := make(map[string]*utils.Stats)
	mockClock := clock.NewMock()
	for _, topic := range topicNames {
		latencyMap[topic] = utils.NewStatsWithClock(1*time.Second, mockClock)
	}

	var wg sync.WaitGroup
	wg.Add(numTopics * numMessages)

	for i := range numTopics {
		go func(topic string) {
			for j := range numMessages {
				start := time.Now()
				cl.Produce(context.Background(), &kgo.Record{
					Topic: topic,
					Value: []byte(fmt.Sprintf("message-%d", j)),
				}, func(r *kgo.Record, err error) {
					defer wg.Done()
					if err != nil {
						t.Errorf("failed to produce message: %v", err)
					}

					topicStats := latencyMap[r.Topic]
					latencyMicros := time.Since(start).Microseconds()
					topicStats.Add(latencyMicros)
				})
			}
		}(topicNames[i])
	}
	wg.Wait()

	// 5. Calculate latency stats
	overallStats := utils.NewStatsWithClock(1*time.Second, mockClock)
	for _, topicStats := range latencyMap {
		overallStats.Merge(topicStats)
	}

	require.Equal(t, numTopics*numMessages, overallStats.Len())
	avgLatency, ok := overallStats.Average()
	if !ok {
		t.Fatalf("failed to calculate average latency")
	}
	percentiles, ok := overallStats.Percentile([]float64{50, 99})
	if !ok || len(percentiles) < 2 {
		t.Fatalf("failed to calculate percentile latencies")
	}

	t.Logf("Average latency: %.2fµs", avgLatency)
	t.Logf("Median latency: %dµs", percentiles[0])
	t.Logf("p99 latency: %dµs", percentiles[1])

	// 5. Consume messages
	cl.AddConsumeTopics(topicNames...)
	var consumedMessages int
	for consumedMessages < numTopics*(numMessages+numWarmupMessages) {
		fetches := cl.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(topic string, p int32, err error) {
			t_err := fmt.Errorf("kafka fetch error: topic %s, partition %d: %v", topic, p, err)
			require.NoError(t, t_err)
		})
		consumedMessages += fetches.NumRecords()
	}
	require.Equal(t, numTopics*(numMessages+numWarmupMessages), consumedMessages)
}
