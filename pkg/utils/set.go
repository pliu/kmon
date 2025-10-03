package utils

import "sync"

// Set is a generic thread-safe set data structure.
type Set[T comparable] struct {
	m    map[T]struct{}
	lock sync.RWMutex
}

// NewSet creates and returns a new Set.
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		m: make(map[T]struct{}),
	}
}

// Add adds an element to the set.
func (s *Set[T]) Add(item T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.m[item] = struct{}{}
}

// Remove removes an element from the set.
func (s *Set[T]) Remove(item T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.m, item)
}

// Contains checks if an element is in the set.
func (s *Set[T]) Contains(item T) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok := s.m[item]
	return ok
}

// Values returns a slice of the elements in the set.
func (s *Set[T]) Values() []T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	values := make([]T, 0, len(s.m))
	for item := range s.m {
		values = append(values, item)
	}
	return values
}
