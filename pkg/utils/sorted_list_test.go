package utils

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSortedList_Delete(t *testing.T) {
	sl := NewSortedList()
	sl.Insert(10, "a")
	sl.Insert(20, "b")
	sl.Insert(5, "c")
	sl.Insert(10, "d") // duplicate

	require.Equal(t, 4, sl.Len())
	vals := sl.GetAll(10)
	require.Len(t, vals, 2)

	// Delete one of the duplicates
	sl.Delete(10)
	require.Equal(t, 3, sl.Len())
	vals = sl.GetAll(10)
	require.Len(t, vals, 1)

	// Delete the other duplicate
	sl.Delete(10)
	require.Equal(t, 2, sl.Len())
	vals = sl.GetAll(10)
	require.Len(t, vals, 0)

	// Check remaining elements
	item, ok := sl.GetByIndex(0)
	require.True(t, ok)
	require.Equal(t, int64(5), item.Key)

	item, ok = sl.GetByIndex(1)
	require.True(t, ok)
	require.Equal(t, int64(20), item.Key)
}

func TestSortedList_Duplicates(t *testing.T) {
	sl := NewSortedList()
	sl.Insert(10, "a")
	sl.Insert(20, "b")
	sl.Insert(10, "c")

	require.Equal(t, 3, sl.Len())

	vals := sl.GetAll(10)
	require.Len(t, vals, 2)
	// Order of duplicates is not guaranteed, so we check for presence.
	require.Contains(t, vals, "a")
	require.Contains(t, vals, "c")

	sl.Delete(10)
	require.Equal(t, 2, sl.Len())
	vals = sl.GetAll(10)
	require.Len(t, vals, 1)
}

func TestSortedList_GetByIndex(t *testing.T) {
	sl := NewSortedList()
	sl.Insert(10, "a")
	sl.Insert(20, "b")
	sl.Insert(5, "c")
	sl.Insert(15, "d")
	sl.Insert(10, "e") // duplicate

	// Expected order: 5, 10, 10, 15, 20
	expectedKeys := []int64{5, 10, 10, 15, 20}
	for i, key := range expectedKeys {
		item, ok := sl.GetByIndex(i)
		require.True(t, ok)
		require.Equal(t, key, item.Key)
	}

	_, ok := sl.GetByIndex(len(expectedKeys))
	require.False(t, ok)
}

func TestSortedList_IndexOf(t *testing.T) {
	sl := NewSortedList()
	sl.Insert(10, "a")
	sl.Insert(20, "b")
	sl.Insert(5, "c")
	sl.Insert(10, "d") // Duplicate

	idx, ok := sl.IndexOf(5)
	require.True(t, ok)
	require.Equal(t, 0, idx)

	idx, ok = sl.IndexOf(10)
	require.True(t, ok)
	require.Equal(t, 1, idx) // Should be the index of the first occurrence

	idx, ok = sl.IndexOf(20)
	require.True(t, ok)
	require.Equal(t, 3, idx)

	_, ok = sl.IndexOf(100)
	require.False(t, ok)
}

func TestSortedList_Randomized(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	sl := NewSortedList()
	goSlice := make([]int, 0)

	// Insert a large number of random elements
	for range 1000 {
		val := r.Intn(100)
		sl.Insert(int64(val), val)
		goSlice = append(goSlice, val)
	}

	// Delete some elements
	for range 200 {
		if len(goSlice) > 0 {
			idx := r.Intn(len(goSlice))
			val := goSlice[idx]
			sl.Delete(int64(val))
			goSlice = append(goSlice[:idx], goSlice[idx+1:]...)
		}
	}

	require.Equal(t, len(goSlice), sl.Len())

	// Sort the Go slice to compare against the SortedList
	sort.Ints(goSlice)

	// Check that the SortedList is still sorted correctly
	i := 0
	for item := range sl.Iter() {
		require.Equal(t, int64(goSlice[i]), item.Key, fmt.Sprintf("Mismatch at index %d", i))
		i++
	}
	require.Equal(t, len(goSlice), i) // Ensure we iterated over all elements
}
