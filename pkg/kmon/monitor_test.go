package kmon

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pliu/kmon/pkg/clients"
	"github.com/pliu/kmon/pkg/config"
	"github.com/pliu/kmon/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MockKgoClient is a mock implementation of the KgoClient interface
type MockKgoClient struct {
	clients.KgoClient
	ProduceFunc     func(context.Context, *kgo.Record, func(*kgo.Record, error))
	PollFetchesFunc func(context.Context) clients.KgoFetches
}

func (m *MockKgoClient) Produce(ctx context.Context, r *kgo.Record, f func(*kgo.Record, error)) {
	if m.ProduceFunc != nil {
		m.ProduceFunc(ctx, r, f)
	}
}

func (m *MockKgoClient) PollFetches(ctx context.Context) clients.KgoFetches {
	if m.PollFetchesFunc != nil {
		return m.PollFetchesFunc(ctx)
	}
	return &MockKgoFetches{}
}

func (m *MockKgoClient) Close() {}

// MockKgoFetches is a mock implementation of the KgoFetches interface
type MockKgoFetches struct {
	clients.KgoFetches
	EachRecordFunc func(func(*kgo.Record))
}

func (m *MockKgoFetches) EachRecord(f func(*kgo.Record)) {
	if m.EachRecordFunc != nil {
		m.EachRecordFunc(f)
	}
}

// MockKadmClient is a mock implementation of the KadmClient interface
type MockKadmClient struct {
	clients.KadmClient
	ListTopicsFunc func(context.Context, ...string) (kadm.TopicDetails, error)
}

func (m *MockKadmClient) ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
	if m.ListTopicsFunc != nil {
		return m.ListTopicsFunc(ctx, topics...)
	}
	return nil, nil
}

func (m *MockKadmClient) Close() {}

func TestInitialisePartitions(t *testing.T) {
	mockKadmClient := &MockKadmClient{
		ListTopicsFunc: func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			return kadm.TopicDetails{
				"test-topic": {
					Partitions: map[int32]kadm.PartitionDetail{
						1: {},
						0: {},
						2: {},
					},
				},
			}, nil
		},
	}

	m := NewMonitorWithClients(&config.KMonConfig{
		ProducerMonitoringTopic: "test-topic",
	}, nil, nil, mockKadmClient, "test-uuid")

	err := m.initialisePartitions(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []int32{0, 1, 2}, m.partitions)
}

func TestHandleConsumedRecord(t *testing.T) {
	// Create a Monitor instance with mock clients
	m := NewMonitorWithClients(&config.KMonConfig{}, &MockKgoClient{}, &MockKgoClient{}, &MockKadmClient{}, "test-uuid")
	m.partitions = []int32{0, 1, 2}
	for _, p := range m.partitions {
		m.partitionStats[p] = newPartitionMetrics(5 * time.Minute)
	}

	// Create a stats object to record the latency of the handleConsumedRecord function
	handleConsumedRecordStats := utils.NewStatsWithClock(1*time.Second, clock.NewMock())

	// Handle multiple records for multiple partitions
	for _, p := range m.partitions {
		for range 400 {
			latency := time.Duration(rand.Intn(1000)) * time.Millisecond
			sentAt := time.Now().Add(-latency)
			record := &kgo.Record{
				Key:       []byte("test-uuid"),
				Value:     []byte(fmt.Sprintf("%d", sentAt.UnixNano())),
				Partition: p,
			}
			start := time.Now()
			m.handleConsumedRecord(record)
			handleConsumedRecordStats.Add(time.Since(start).Nanoseconds())
		}
	}

	// Check if the E2E latency metric has been updated for each partition
	for _, p := range m.partitions {
		assert.Equal(t, m.partitionStats[p].e2e.Len(), 400)
	}

	// Print the stats of the handleConsumedRecord function
	avg, ok := handleConsumedRecordStats.Average()
	assert.True(t, ok)
	percentiles, ok := handleConsumedRecordStats.Percentile([]float64{50, 99})
	assert.True(t, ok)
	t.Logf("Average latency: %.2fµs", avg/1000)
	t.Logf("Median latency: %dµs", percentiles[0]/1000)
	t.Logf("p99 latency: %dµs", percentiles[1]/1000)

	for _, p := range m.partitions {
		for range 400 {
			latency := time.Duration(rand.Intn(1000)) * time.Millisecond
			sentAt := time.Now().Add(-latency)
			record := &kgo.Record{
				Key:       []byte("test-uuid2"),
				Value:     []byte(fmt.Sprintf("%d", sentAt.UnixNano())),
				Partition: p,
			}
			m.handleConsumedRecord(record)
		}
	}

	for _, p := range m.partitions {
		assert.Equal(t, m.partitionStats[p].e2e.Len(), 400)
	}
}

func TestPublishProbe(t *testing.T) {
	var producedRecord *kgo.Record
	mockProducerClient := &MockKgoClient{
		ProduceFunc: func(ctx context.Context, r *kgo.Record, f func(*kgo.Record, error)) {
			producedRecord = r
			f(r, nil) // call the callback
		},
	}

	m := NewMonitorWithClients(&config.KMonConfig{
		ProducerMonitoringTopic: "test-topic",
	}, mockProducerClient, nil, nil, "test-uuid")
	m.partitionStats[1] = newPartitionMetrics(5 * time.Minute)

	m.publishProbe(context.Background(), 1)

	assert.NotNil(t, producedRecord)
	assert.Equal(t, "test-topic", producedRecord.Topic)
	assert.Equal(t, int32(1), producedRecord.Partition)
	assert.Equal(t, "test-uuid", string(producedRecord.Key))

	// Check the timestamp in the value
	timestamp, err := strconv.ParseInt(string(producedRecord.Value), 10, 64)
	assert.NoError(t, err)
	assert.InDelta(t, time.Now().UnixNano(), timestamp, float64(time.Second))
}
