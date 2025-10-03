package clients

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pliu/kmon/pkg/config"
)

const (
	kafkaBroker       = "localhost:9092"
	numWarmupMessages = 10
)

type latencyData struct {
	mu        sync.Mutex
	latencies []float64
}

func TestKafka_WriteAndConsume_MultipleTopics(t *testing.T) {
	runWriteAndConsumeTest(t, 5, 200)
}

func TestKafka_WriteAndConsume_SingleTopic_Serial(t *testing.T) {
	runWriteAndConsumeTest(t, 1, 1000)
}

func runWriteAndConsumeTest(t *testing.T, numTopics int, numMessages int) {
	// Create a mock config for testing
	mockConfig := &config.KMonConfig{
		KafkaConfig: config.KafkaConfig{
			SeedBrokers: []string{kafkaBroker},
		},
	}

	// 1. Create a new admin client
	cl, err := GetFranzGoClient(mockConfig)
	if err != nil {
		t.Fatalf("failed to create kafka client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

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
	latencyMap := make(map[string]*latencyData)
	for _, topic := range topicNames {
		latencyMap[topic] = &latencyData{
			latencies: make([]float64, 0, numMessages),
		}
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

					topicLatencies := latencyMap[r.Topic]
					latency := float64(time.Since(start).Microseconds())

					topicLatencies.mu.Lock()
					topicLatencies.latencies = append(topicLatencies.latencies, latency)
					topicLatencies.mu.Unlock()
				})
			}
		}(topicNames[i])
	}
	wg.Wait()

	// 5. Calculate latency stats
	var allLatencies []float64
	for _, data := range latencyMap {
		allLatencies = append(allLatencies, data.latencies...)
	}
	var sum float64
	for _, l := range allLatencies {
		sum += l
	}
	sort.Float64s(allLatencies)
	avgLatency := sum / float64(len(allLatencies))
	p99Latency, _ := percentile(allLatencies, 99)
	p50Latency, _ := percentile(allLatencies, 50)

	t.Logf("Average latency: %.2fµs", avgLatency)
	t.Logf("Median latency: %.2fµs", p50Latency)
	t.Logf("p99 latency: %.2fµs", p99Latency)

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
			assert.NoError(t, t_err)
		})
		consumedMessages += fetches.NumRecords()
	}
	assert.Equal(t, numTopics*(numMessages+numWarmupMessages), consumedMessages)
}

func percentile(data []float64, p float64) (float64, bool) {
	if len(data) == 0 || p < 0 || p > 100 {
		return 0, false
	}
	sort.Float64s(data)
	index := int(float64(len(data)-1) * (p / 100.0))
	return data[index], true
}
