package foundation

import (
	"context"
	"io"
)

// Resource creates a component with initialization and cleanup callbacks.
func Resource(name string, start, stop func(context.Context) error) Component {
	return Component{Name: name, Start: start, Stop: stop}
}

// Worker creates a supervised long-running component.
func Worker(name string, run func(context.Context) error) Component {
	return Component{Name: name, Run: run}
}

// Closer creates a shutdown-only component for an io.Closer.
func Closer(name string, closer io.Closer) Component {
	return Component{
		Name: name,
		Stop: func(context.Context) error {
			if closer == nil {
				return nil
			}
			return closer.Close()
		},
	}
}
