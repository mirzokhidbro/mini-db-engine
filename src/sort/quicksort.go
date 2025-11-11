package sort

import "fmt"

func QuickSort() {
	fmt.Println("working")
}

func partition(arr []int, low, high int) ([]int, int) {

	pivot := arr[high]
	i := low

	for j := i; j < high; j++ {

		if arr[j] < pivot {

			arr[i], arr[j] = arr[j], arr[i]

			i++

		}

	}

	arr[high], arr[i] = arr[i], arr[high]

	return arr, i
}

func quickSort(arr []int, low, high int) []int {
	if low < high {
		arr, p := partition(arr, low, high)
		quickSort(arr, low, p-1)
		quickSort(arr, p+1, high)
	}

	return arr
}

func QuickSortStart(arr []int) []int {
	return quickSort(arr, 0, len(arr)-1)
}

// IndexEntry represents a single index entry for sorting
type IndexEntry struct {
	Value      interface{}
	PageNumber uint64
	RecordSlot uint16
}

// partitionEntries partitions the entries array for quicksort
func partitionEntries(entries []IndexEntry, low, high int, isInt bool) ([]IndexEntry, int) {
	pivot := entries[high].Value
	i := low

	for j := i; j < high; j++ {
		if compareValues(entries[j].Value, pivot, isInt) < 0 {
			entries[i], entries[j] = entries[j], entries[i]
			i++
		}
	}

	entries[high], entries[i] = entries[i], entries[high]
	return entries, i
}

// quickSortEntries performs quicksort on entries
func quickSortEntries(entries []IndexEntry, low, high int, isInt bool) []IndexEntry {
	if low < high {
		entries, p := partitionEntries(entries, low, high, isInt)
		quickSortEntries(entries, low, p-1, isInt)
		quickSortEntries(entries, p+1, high, isInt)
	}
	return entries
}

// QuickSortIndexEntries sorts index entries
// isInt: true for integer sorting, false for float sorting
func QuickSortIndexEntries(entries []IndexEntry, isInt bool) []IndexEntry {
	if len(entries) == 0 {
		return entries
	}
	return quickSortEntries(entries, 0, len(entries)-1, isInt)
}

// compareValues compares two values
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func compareValues(a, b interface{}, isInt bool) int {
	if isInt {
		// Integer comparison
		va, oka := a.(int64)
		vb, okb := b.(int64)

		if !oka {
			if ia, ok := a.(int); ok {
				va = int64(ia)
				oka = true
			}
		}
		if !okb {
			if ib, ok := b.(int); ok {
				vb = int64(ib)
				okb = true
			}
		}

		if !oka || !okb {
			return 0
		}

		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	} else {
		// Float comparison
		va, oka := a.(float64)
		vb, okb := b.(float64)

		if !oka || !okb {
			return 0
		}

		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	}
}
