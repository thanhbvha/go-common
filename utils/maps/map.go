// Package maps provides generic utility functions for Go maps.
//
// It leverages Go 1.18+ generics to offer common map operations like getting keys,
// values, or merging maps in a type-safe manner.
package maps

// Keys returns all keys from a map as a slice
func Keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Values returns all values from a map as a slice
func Values[K comparable, V any](m map[K]V) []V {
	values := make([]V, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}
