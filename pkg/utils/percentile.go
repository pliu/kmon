package utils

import (
	"container/list"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

// LatencyTracker tracks latency measurements in a sliding window and calculates percentiles.
type LatencyTracker struct {
	mu         sync.Mutex
	latencies  *SortedList
	window     *list.List
	windowSize time.Duration
	clock      clock.Clock
}

// NewLatencyTracker creates a new LatencyTracker with a real clock.
func NewLatencyTracker(windowSize time.Duration) *LatencyTracker {
	return NewLatencyTrackerWithClock(windowSize, clock.New())
}

// NewLatencyTrackerWithClock creates a new LatencyTracker with a custom clock.
func NewLatencyTrackerWithClock(windowSize time.Duration, clk clock.Clock) *LatencyTracker {
	return &LatencyTracker{
		latencies:  NewSortedList(),
		window:     list.New(),
		windowSize: windowSize,
		clock:      clk,
	}
}

type measurement struct {
	timestamp time.Time
	latency   int64
}

// Add adds a new latency measurement.
func (lt *LatencyTracker) Add(latency int64) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	now := lt.clock.Now()
	lt.latencies.Insert(latency, true)
	lt.window.PushBack(&measurement{timestamp: now, latency: latency})
	lt.cleanup(now)
}

// Percentile calculates the latency at a given percentile.
func (lt *LatencyTracker) Percentile(p float64) (int64, bool) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if p < 0 || p > 100 {
		return 0, false
	}

	count := lt.latencies.Len()
	if count == 0 {
		return 0, false
	}

	index := int(float64(count-1) * (p / 100.0))
	item, ok := lt.latencies.GetByIndex(index)
	if !ok {
		return 0, false
	}
	return item.Key, true
}

// Len returns the number of items in the tracker.
func (lt *LatencyTracker) Len() int {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.latencies.Len()
}

// cleanup removes measurements that are older than the window size.
func (lt *LatencyTracker) cleanup(now time.Time) {
	for e := lt.window.Front(); e != nil; e = lt.window.Front() {
		m := e.Value.(*measurement)
		if now.Sub(m.timestamp) > lt.windowSize {
			lt.latencies.Delete(m.latency)
			lt.window.Remove(e)
		} else {
			// The list is sorted by time, so we can stop here.
			break
		}
	}
}
