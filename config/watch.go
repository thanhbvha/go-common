package config

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Watcher owns file notifications and serial reload callbacks. Callbacks must
// return promptly; Close waits for a callback already in progress to finish.
type Watcher struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (w *Watcher) Close() error          { w.once.Do(w.cancel); <-w.done; return nil }
func (w *Watcher) Done() <-chan struct{} { return w.done }

// Watch publishes an initial validated configuration, then fresh snapshots on
// file changes. Invalid reloads retain the last published value. Directory
// watching handles atomic file replacement and ConfigMap symlink swaps.
// Treat published snapshots as immutable. onError receives reload/callback
// failures and must return promptly. Call Close outside these callbacks.
func Watch(ctx context.Context, opts Options, factory func() any, onChange func(any), onError func(error)) (*Watcher, error) {
	if ctx == nil || factory == nil || onChange == nil {
		return nil, errors.New("config: context, factory and callback are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.Path == "" {
		return nil, errors.New("config: watch requires a file path")
	}
	path, err := filepath.Abs(opts.Path)
	if err != nil {
		return nil, err
	}
	opts.Path = path
	fs, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err = fs.Add(filepath.Dir(path)); err != nil {
		fs.Close()
		return nil, err
	}
	var last any
	publish := func() (err error) {
		defer func() {
			if recover() != nil {
				err = errors.New("config: reload callback panicked")
			}
		}()
		target := factory()
		if err = Load(opts, target); err != nil {
			return err
		}
		if reflect.DeepEqual(last, target) {
			return nil
		}
		onChange(target)
		last = target
		return nil
	}
	if err = publish(); err != nil {
		fs.Close()
		return nil, err
	}
	watchCtx, cancel := context.WithCancel(ctx)
	w := &Watcher{cancel: cancel, done: make(chan struct{})}
	report := func(err error) {
		if onError != nil {
			func() { defer func() { _ = recover() }(); onError(err) }()
		}
	}
	go func() {
		defer close(w.done)
		defer fs.Close()
		defer cancel()
		var timer *time.Timer
		var tick <-chan time.Time
		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()
		for {
			select {
			case <-watchCtx.Done():
				return
			case _, ok := <-fs.Events:
				if !ok {
					return
				}
				if timer == nil {
					timer = time.NewTimer(40 * time.Millisecond)
				} else {
					timer.Reset(40 * time.Millisecond)
				}
				tick = timer.C
			case err, ok := <-fs.Errors:
				if !ok {
					return
				}
				report(fmt.Errorf("config: watch: %w", err))
			case <-tick:
				tick = nil
				if watchCtx.Err() != nil {
					return
				}
				if err := publish(); err != nil {
					report(err)
				}
			}
		}
	}()
	return w, nil
}
