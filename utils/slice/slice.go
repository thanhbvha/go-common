// Package slice provides generic utility functions for Go slices.
//
// It complements the standard library slices package with filtering, mapping,
// and stable deduplication.
package slice

// Unique returns a new slice containing the first occurrence of every value.
// It preserves input order and returns nil for a nil input.
func Unique[T comparable](values []T) []T {
	if values == nil {
		return nil
	}
	seen := make(map[T]struct{}, len(values))
	result := make([]T, 0, len(values))
	for _, entry := range values {
		if _, exists := seen[entry]; !exists {
			seen[entry] = struct{}{}
			result = append(result, entry)
		}
	}
	return result
}

// Filter returns the values for which keep returns true. It returns nil for a
// nil input and panics when keep is nil.
func Filter[T any](values []T, keep func(T) bool) []T {
	if values == nil {
		return nil
	}
	result := make([]T, 0, len(values))
	for _, v := range values {
		if keep(v) {
			result = append(result, v)
		}
	}
	return result
}

// Map transforms every value while preserving order. It returns nil for a nil
// input and panics when transform is nil.
func Map[T any, U any](values []T, transform func(T) U) []U {
	if values == nil {
		return nil
	}
	result := make([]U, len(values))
	for i, v := range values {
		result[i] = transform(v)
	}
	return result
}
