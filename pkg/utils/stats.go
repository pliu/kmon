package utils

import (
	"container/list"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

// Stats tracks numeric values in a sliding window and can produce summaries.
type Stats struct {
	mu         sync.Mutex
	values     *SortedList
	window     *list.List
	windowSize time.Duration
	clock      clock.Clock
	sum        int64
}

// NewStats creates a new Stats tracker with a real clock.
func NewStats(windowSize time.Duration) *Stats {
	return NewStatsWithClock(windowSize, clock.New())
}

// NewStatsWithClock creates a new Stats tracker with a custom clock.
func NewStatsWithClock(windowSize time.Duration, clk clock.Clock) *Stats {
	return &Stats{
		values:     NewSortedList(),
		window:     list.New(),
		windowSize: windowSize,
		clock:      clk,
	}
}

type measurement struct {
	timestamp time.Time
	value     int64
}

// Add adds a new value measurement.
func (s *Stats) Add(value int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	s.values.Insert(value)
	s.window.PushBack(&measurement{timestamp: now, value: value})
	s.sum += value
	s.cleanup(now)
}

// Average returns the average value of the current window.
func (s *Stats) Average() (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := s.values.Len()
	if count == 0 {
		return 0, false
	}
	return float64(s.sum) / float64(count), true
}

// Percentile calculates the latency values at the requested percentiles.
func (s *Stats) Percentile(percentiles []float64) ([]int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(percentiles) == 0 {
		return nil, false
	}
	for _, p := range percentiles {
		if p < 0 || p > 100 {
			return nil, false
		}
	}

	count := s.values.Len()
	if count == 0 {
		return nil, false
	}

	results := make([]int64, 0, len(percentiles))
	for _, p := range percentiles {
		index := int(float64(count-1) * (p / 100.0))
		key, ok := s.values.GetByIndex(index)
		if !ok {
			return nil, false
		}
		results = append(results, key)
	}
	return results, true
}

// Len returns the number of items in the tracker.
func (s *Stats) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values.Len()
}

// Values returns the current window's values in ascending order, including duplicates.
func (s *Stats) Values() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.values == nil || s.values.Len() == 0 {
		return []int64{}
	}
	return s.values.Keys()
}

// Merge merges all observations from other into this tracker.
func (s *Stats) Merge(other *Stats) {
	if other == nil {
		return
	}
	if s == other {
		return
	}

	// Grab other's state by copying its sorted list.
	other.mu.Lock()
	otherLen := other.values.Len()
	if otherLen == 0 {
		other.mu.Unlock()
		return
	}
	tmpValues := NewSortedList()
	tmpValues.Merge(other.values)
	otherSum := other.sum
	measurements := make([]measurement, 0, otherLen)
	for e := other.window.Front(); e != nil; e = e.Next() {
		m := e.Value.(*measurement)
		measurements = append(measurements, *m)
	}
	other.mu.Unlock()

	s.mu.Lock()
	s.values.Merge(tmpValues)
	s.sum += otherSum
	s.mergeMeasurements(measurements)
	s.cleanup(s.clock.Now())
	s.mu.Unlock()
}

func (s *Stats) mergeMeasurements(ms []measurement) {
	if len(ms) == 0 {
		return
	}
	if s.window.Len() == 0 {
		for i := range ms {
			m := ms[i]
			s.window.PushBack(&measurement{timestamp: m.timestamp, value: m.value})
		}
		return
	}

	newList := list.New()
	existing := s.window.Front()
	idx := 0

	for existing != nil && idx < len(ms) {
		em := existing.Value.(*measurement)
		if ms[idx].timestamp.Before(em.timestamp) {
			m := ms[idx]
			newList.PushBack(&measurement{timestamp: m.timestamp, value: m.value})
			idx++
		} else {
			newList.PushBack(existing.Value)
			existing = existing.Next()
		}
	}

	for ; idx < len(ms); idx++ {
		m := ms[idx]
		newList.PushBack(&measurement{timestamp: m.timestamp, value: m.value})
	}

	for ; existing != nil; existing = existing.Next() {
		newList.PushBack(existing.Value)
	}

	s.window = newList
}

// cleanup removes measurements that are older than the window size.
func (s *Stats) cleanup(now time.Time) {
	for e := s.window.Front(); e != nil; e = s.window.Front() {
		m := e.Value.(*measurement)
		if now.Sub(m.timestamp) > s.windowSize {
			s.values.Delete(m.value)
			s.sum -= m.value
			s.window.Remove(e)
		} else {
			// The list is sorted by time, so we can stop here.
			break
		}
	}
}
