package utils

type Set[T comparable] struct {
	m map[T]struct{}
}

// NewSet creates and returns a new Set.
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		m: make(map[T]struct{}),
	}
}

// Add adds an element to the set.
func (s *Set[T]) Add(item T) {
	s.m[item] = struct{}{}
}

// Remove removes an element from the set.
func (s *Set[T]) Remove(item T) {
	delete(s.m, item)
}

// Contains checks if an element is in the set.
func (s *Set[T]) Contains(item T) bool {
	_, ok := s.m[item]
	return ok
}

// Len returns the number of elements in the set.
func (s *Set[T]) Len() int {
	return len(s.m)
}

// Values returns a snapshot slice of all elements.
func (s *Set[T]) Items() []T {
	items := make([]T, 0, len(s.m))
	for item := range s.m {
		items = append(items, item)
	}
	return items
}

// Equals checks if the set contains exactly the same elements as another set.
func (s *Set[T]) Equals(other *Set[T]) bool {
	if other == nil {
		return false
	}

	// If lengths differ, sets can't be equal
	if len(s.m) != len(other.m) {
		return false
	}

	// Check if every element in s exists in other
	for item := range s.m {
		if _, exists := other.m[item]; !exists {
			return false
		}
	}

	return true
}
