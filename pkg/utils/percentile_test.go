package utils

import (
	"math/rand"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
)

func TestLatencyTracker_BasicPercentiles(t *testing.T) {
	mockClock := clock.NewMock()
	lt := NewLatencyTrackerWithClock(10*time.Second, mockClock)

	// Add 100 values from 1 to 100
	for i := 1; i <= 100; i++ {
		lt.Add(int64(i))
	}

	// Check percentiles
	p50, ok := lt.Percentile(50)
	require.True(t, ok)
	require.Equal(t, int64(50), p50, "p50 should be 50")

	p90, ok := lt.Percentile(90)
	require.True(t, ok)
	require.Equal(t, int64(90), p90, "p90 should be 90")

	p99, ok := lt.Percentile(99)
	require.True(t, ok)
	require.Equal(t, int64(99), p99, "p99 should be 99")

	p100, ok := lt.Percentile(100)
	require.True(t, ok)
	require.Equal(t, int64(100), p100, "p100 should be 100")
}

func TestLatencyTracker_SlidingWindow(t *testing.T) {
	windowSize := 1 * time.Second
	mockClock := clock.NewMock()
	lt := NewLatencyTrackerWithClock(windowSize, mockClock)

	// Add initial set of values (1-100)
	for i := 1; i <= 100; i++ {
		lt.Add(int64(i))
	}
	require.Equal(t, 100, lt.latencies.Len())
	p99, ok := lt.Percentile(99)
	require.True(t, ok)
	require.Equal(t, int64(99), p99)

	// Advance clock by half the window size
	mockClock.Add(windowSize / 2)

	// Add a second set of values (101-200)
	for i := 101; i <= 200; i++ {
		lt.Add(int64(i))
	}
	require.Equal(t, 200, lt.latencies.Len())

	// Check percentiles with mixed data {1..100, 101..200}
	p95, ok := lt.Percentile(95)
	require.True(t, ok)
	require.Equal(t, int64(190), p95)

	p99, ok = lt.Percentile(99)
	require.True(t, ok)
	require.Equal(t, int64(198), p99)

	// Advance clock so the first set expires
	mockClock.Add(windowSize/2 + 1*time.Millisecond)

	// Add a third set of values (201-300). This will trigger cleanup.
	for i := 201; i <= 300; i++ {
		lt.Add(int64(i))
	}

	// Now the list should contain {101..200, 201..300}
	require.Equal(t, 200, lt.latencies.Len())

	p95, ok = lt.Percentile(95)
	require.True(t, ok)
	require.Equal(t, int64(290), p95)

	p99, ok = lt.Percentile(99)
	require.True(t, ok)
	require.Equal(t, int64(298), p99)
}

func TestLatencyTracker_Performance(t *testing.T) {
	windowSize := 10 * time.Minute
	lt := NewLatencyTracker(windowSize)

	// Pre-populate the LatencyTracker with 5000 data points
	for range 50000 {
		lt.Add(rand.Int63n(1000))
	}

	// Measure the latency of 1000 Add calls
	addDurations := make([]time.Duration, 1000)
	for i := range 1000 {
		start := time.Now()
		lt.Add(rand.Int63n(1000))
		addDurations[i] = time.Since(start)
	}

	// Measure the latency of 1000 Percentile calls
	percentileDurations := make([]time.Duration, 1000)
	for i := range 1000 {
		start := time.Now()
		lt.Percentile(99)
		percentileDurations[i] = time.Since(start)
	}

	t.Logf("Data points: %d", lt.Len())

	// Calculate average latencies
	var totalAdd time.Duration
	for _, d := range addDurations {
		totalAdd += d
	}
	avgAdd := totalAdd / time.Duration(len(addDurations))

	var totalPercentile time.Duration
	for _, d := range percentileDurations {
		totalPercentile += d
	}
	avgPercentile := totalPercentile / time.Duration(len(percentileDurations))

	t.Logf("Average Add latency: %v", avgAdd)
	t.Logf("Average Percentile latency: %v", avgPercentile)
}
