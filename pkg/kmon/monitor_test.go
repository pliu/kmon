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
	"github.com/pliu/kmon/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MockKgoClient is a mock implementation of the KgoClient interface
type MockKgoClient struct {
	clients.KgoClient
	ProduceFunc func(context.Context, *kgo.Record, func(*kgo.Record, error))
}

func (m *MockKgoClient) Produce(ctx context.Context, r *kgo.Record, f func(*kgo.Record, error)) {
	if m.ProduceFunc != nil {
		m.ProduceFunc(ctx, r, f)
	}
}

func (m *MockKgoClient) PollFetches(ctx context.Context) kgo.Fetches {
	return kgo.Fetches{}
}

func (m *MockKgoClient) Close() {}

func TestHandleConsumedRecord(t *testing.T) {
	// Create a Monitor instance with mock clients
	m := NewMonitorWithClients(&MockKgoClient{}, "", &MockKgoClient{}, "test-uuid", 3, time.Duration(1), time.Duration(5)*time.Minute)

	// Create a stats object to record the latency of the handleConsumedRecord function
	handleConsumedRecordStats := utils.NewStatsWithClock(1*time.Second, clock.NewMock())

	// Handle multiple records for multiple partitions
	for p := range m.partitions {
		for range 400 {
			latency := time.Duration(rand.Intn(1000)) * time.Millisecond
			sentAt := time.Now().Add(-latency)
			record := &kgo.Record{
				Key:       []byte("test-uuid"),
				Value:     []byte(fmt.Sprintf("%d", sentAt.UnixNano())),
				Partition: int32(p),
			}
			start := time.Now()
			m.handleConsumedRecord(record, start)
			handleConsumedRecordStats.Add(time.Since(start).Nanoseconds())
		}
	}

	// Check if the E2E latency metric has been updated for each partition
	for partition := range m.partitions {
		require.Equal(t, m.e2eStats[partition].Len(), 400)
		require.Equal(t, m.b2cStats[partition].Len(), 400)
		require.Equal(t, m.p2bStats[partition].Len(), 400)
	}

	// Print the stats of the handleConsumedRecord function
	avg, ok := handleConsumedRecordStats.Average()
	require.True(t, ok)
	percentiles, ok := handleConsumedRecordStats.Percentile([]float64{50, 99})
	require.True(t, ok)
	t.Logf("Average latency: %.2fµs", avg/1000)
	t.Logf("Median latency: %dµs", percentiles[0]/1000)
	t.Logf("p99 latency: %dµs", percentiles[1]/1000)

	unusedTime := time.Now()
	for p := range m.partitions {
		for range 400 {
			latency := time.Duration(rand.Intn(1000)) * time.Millisecond
			sentAt := time.Now().Add(-latency)
			record := &kgo.Record{
				Key:       []byte("test-uuid2"),
				Value:     []byte(fmt.Sprintf("%d", sentAt.UnixNano())),
				Partition: int32(p),
			}
			m.handleConsumedRecord(record, unusedTime)
		}
	}

	for partition := range m.partitions {
		require.Equal(t, m.e2eStats[partition].Len(), 400)
		require.Equal(t, m.b2cStats[partition].Len(), 400)
		require.Equal(t, m.p2bStats[partition].Len(), 400)
	}
}

func TestPublishProbeBatch(t *testing.T) {
	// Track all produced records
	var producedRecords []*kgo.Record

	// Create mock client that captures all produced records
	mockProducerClient := &MockKgoClient{
		ProduceFunc: func(ctx context.Context, r *kgo.Record, f func(*kgo.Record, error)) {
			producedRecords = append(producedRecords, r)
			f(r, nil) // call the callback
		},
	}

	// Create monitor with multiple partitions
	partitions := 3
	m := NewMonitorWithClients(mockProducerClient, "test-topic", nil, "test-uuid", partitions, time.Duration(1), time.Duration(5)*time.Minute)

	// Call publishProbeBatch which should call publishProbe for each partition
	ctx := context.Background()
	m.publishProbeBatch(ctx)

	// Verify that records were produced for each partition
	require.Equal(t, partitions, len(producedRecords))

	// Check that each partition has a corresponding record
	expectedPartitions := make(map[int]bool)
	for _, record := range producedRecords {
		require.Equal(t, "test-topic", record.Topic)
		require.Less(t, int(record.Partition), partitions)
		require.Equal(t, "test-uuid", string(record.Key))
		expectedPartitions[int(record.Partition)] = true

		// Check the timestamp in the value
		timestamp, err := strconv.ParseInt(string(record.Value), 10, 64)
		require.NoError(t, err)
		require.InDelta(t, time.Now().UnixNano(), timestamp, float64(time.Second))
	}
	require.Equal(t, partitions, len(m.p2bStats))

	// Ensure all partitions were covered
	for p := range partitions {
		require.True(t, expectedPartitions[p], "Partition %d should have been probed", p)
		require.Equal(t, 1, m.producerAckStats[p].Len())
	}
}
