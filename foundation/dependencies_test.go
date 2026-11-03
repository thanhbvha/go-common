package foundation

import (
	"context"
	"errors"
	"testing"
)

func TestDependencyLifecycle(t *testing.T) {
	app := newTestApp(t, Config{Name: "dependencies"})
	var order []string
	// A diamond, with the root registered last and a ready independent node.
	for _, spec := range []struct {
		name         string
		dependencies []string
	}{
		{"api", []string{"cache", "worker"}},
		{"independent", nil},
		{"cache", []string{"db"}},
		{"worker", []string{"db"}},
		{"db", nil},
	} {
		if err := app.Register(Component{Name: spec.name, DependsOn: spec.dependencies,
			Start: func(context.Context) error { order = append(order, "start:"+spec.name); return nil },
			Stop:  func(context.Context) error { order = append(order, "stop:"+spec.name); return nil },
		}); err != nil {
			t.Fatal(err)
		}
	}
	done := runAsync(app, t.Context())
	waitForState(t, app, StateRunning)
	if err := app.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	want := []string{"start:independent", "start:db", "start:cache", "start:worker", "start:api", "stop:api", "stop:worker", "stop:cache", "stop:db", "stop:independent"}
	if !equalStrings(order, want) {
		t.Fatalf("order=%v want=%v", order, want)
	}
}

func TestDependencyValidationBeforeCallbacks(t *testing.T) {
	for _, cycle := range []bool{false, true} {
		app := newTestApp(t, Config{Name: "invalid"})
		called := false
		callback := func(context.Context) error { called = true; return nil }
		for _, component := range []Component{
			{Name: "independent", Start: callback},
			{Name: "a", DependsOn: []string{"b"}, Start: callback},
		} {
			if err := app.Register(component); err != nil {
				t.Fatal(err)
			}
		}
		if cycle {
			if err := app.Register(Component{Name: "b", DependsOn: []string{"a"}, Start: callback}); err != nil {
				t.Fatal(err)
			}
		}
		err := app.Run(t.Context())
		if !errors.Is(err, ErrInvalidDependency) || called || app.State() != StateFailed {
			t.Fatalf("err=%v called=%v state=%v", err, called, app.State())
		}
		if app.Shutdown(t.Context()) != err {
			t.Fatal("terminal graph error not retained")
		}
	}
}

func TestDependencyRegistrationOwnership(t *testing.T) {
	app := newTestApp(t, Config{Name: "ownership"})
	for _, dependencies := range [][]string{{""}, {"a"}, {"b", " b "}, {"bad\nname"}} {
		if err := app.Register(Component{Name: "a", DependsOn: dependencies, Run: waitForCancellation}); !errors.Is(err, ErrInvalidComponent) {
			t.Fatalf("dependencies=%q: %v", dependencies, err)
		}
	}
	dependencies := []string{" b "}
	if err := app.Register(Component{Name: "a", DependsOn: dependencies, Run: waitForCancellation}); err != nil {
		t.Fatal(err)
	}
	dependencies[0] = "missing"
	if err := app.Register(Worker("b", waitForCancellation)); err != nil {
		t.Fatal(err)
	}
	done := runAsync(app, t.Context())
	waitForState(t, app, StateRunning)
	if err := app.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDependencyRollback(t *testing.T) {
	app := newTestApp(t, Config{Name: "rollback"})
	failure := errors.New("start failure")
	var stopped []string
	for _, c := range []Component{
		{Name: "api", DependsOn: []string{"worker"}, Start: func(context.Context) error { return failure }},
		{Name: "worker", DependsOn: []string{"db"}, Stop: func(context.Context) error { stopped = append(stopped, "worker"); return nil }},
		{Name: "db", Stop: func(context.Context) error { stopped = append(stopped, "db"); return nil }},
	} {
		if err := app.Register(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Run(t.Context()); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !equalStrings(stopped, []string{"worker", "db"}) {
		t.Fatal(stopped)
	}
}
