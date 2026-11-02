package slice

import (
	"reflect"
	"strconv"
	"testing"
)

func TestUnique(t *testing.T) {
	if got := Unique([]int{3, 1, 3, 2, 1}); !reflect.DeepEqual(got, []int{3, 1, 2}) {
		t.Fatalf("Unique() = %v", got)
	}
	if got := Unique[int](nil); got != nil {
		t.Fatalf("Unique(nil) = %#v", got)
	}
}

func TestFilterAndMap(t *testing.T) {
	filtered := Filter([]int{1, 2, 3, 4}, func(value int) bool { return value%2 == 0 })
	if !reflect.DeepEqual(filtered, []int{2, 4}) {
		t.Fatalf("Filter() = %v", filtered)
	}
	mapped := Map(filtered, strconv.Itoa)
	if !reflect.DeepEqual(mapped, []string{"2", "4"}) {
		t.Fatalf("Map() = %v", mapped)
	}
	if Filter[int](nil, func(int) bool { return true }) != nil {
		t.Fatal("Filter(nil) must return nil")
	}
	if Map[int, string](nil, strconv.Itoa) != nil {
		t.Fatal("Map(nil) must return nil")
	}
}
