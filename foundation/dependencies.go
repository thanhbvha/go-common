package foundation

import (
	"container/heap"
	"fmt"
)

// orderComponents uses registration order to break ties between ready nodes.
// Validation completes before any lifecycle callback is invoked.
func orderComponents(components []Component) ([]Component, error) {
	indices := make(map[string]int, len(components))
	for i, component := range components {
		indices[component.Name] = i
	}
	remaining := make([]int, len(components))
	dependents := make([][]int, len(components))
	ready := &componentQueue{}
	for i, component := range components {
		remaining[i] = len(component.DependsOn)
		for _, name := range component.DependsOn {
			dependency, ok := indices[name]
			if !ok {
				return nil, fmt.Errorf("%w: component %q requires missing component %q", ErrInvalidDependency, component.Name, name)
			}
			dependents[dependency] = append(dependents[dependency], i)
		}
		if remaining[i] == 0 {
			heap.Push(ready, i)
		}
	}
	ordered := make([]Component, 0, len(components))
	for ready.Len() > 0 {
		i := heap.Pop(ready).(int)
		ordered = append(ordered, components[i])
		for _, dependent := range dependents[i] {
			remaining[dependent]--
			if remaining[dependent] == 0 {
				heap.Push(ready, dependent)
			}
		}
	}
	if len(ordered) != len(components) {
		var blocked []string
		for i, count := range remaining {
			if count > 0 {
				blocked = append(blocked, components[i].Name)
			}
		}
		return nil, fmt.Errorf("%w: dependency cycle blocks %q", ErrInvalidDependency, blocked)
	}
	return ordered, nil
}

type componentQueue []int

func (q componentQueue) Len() int           { return len(q) }
func (q componentQueue) Less(i, j int) bool { return q[i] < q[j] }
func (q componentQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *componentQueue) Push(value any)    { *q = append(*q, value.(int)) }
func (q *componentQueue) Pop() any {
	last := len(*q) - 1
	value := (*q)[last]
	*q = (*q)[:last]
	return value
}
