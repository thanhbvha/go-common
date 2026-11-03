package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/v2/db/orm"
)

func demoManager(ctx context.Context, opt options) (result error) {
	manager := orm.NewManager()
	var fixtures []*fixture
	var providers []*localTelemetry
	defer func() {
		cleanup, cancel := cleanupContext()
		defer cancel()
		for _, f := range fixtures {
			result = errors.Join(result, f.drop(cleanup))
		}
		result = errors.Join(result, manager.Close(cleanup))
		for _, p := range providers {
			result = errors.Join(result, p.close())
		}
	}()
	configurations := map[string]orm.Config{}
	prefixes := map[string]string{}
	for _, name := range []string{"primary", "reporting", "extra"} {
		cfg, prefix, telemetry, err := newConfig(opt)
		if err != nil {
			return err
		}
		configurations[name] = cfg
		prefixes[name] = prefix
		if telemetry != nil {
			providers = append(providers, telemetry)
		}
	}
	initial := map[string]orm.Config{"primary": configurations["primary"], "reporting": configurations["reporting"]}
	if err := manager.Init(ctx, initial, "primary"); err != nil {
		return err
	}
	if err := expect(manager.Init(ctx, initial, "primary"), orm.ErrAlreadyInitialized); err != nil {
		return err
	}
	primary, err := manager.Lookup("")
	if err != nil {
		return err
	}
	named, err := manager.Lookup("primary")
	if err != nil {
		return err
	}
	if err := require(primary == named, "default alias differs from primary"); err != nil {
		return err
	}
	if err := manager.Add(ctx, "extra", configurations["extra"]); err != nil {
		return err
	}
	if err := expect(manager.Add(ctx, "extra", configurations["extra"]), orm.ErrDuplicateConnection); err != nil {
		return err
	}
	if _, err := manager.Lookup("missing"); !errors.Is(err, orm.ErrNotFound) {
		return fmt.Errorf("missing alias was accepted")
	}
	for _, name := range []string{"primary", "reporting", "extra"} {
		c, err := manager.Lookup(name)
		if err != nil {
			return err
		}
		f := &fixture{client: c, config: configurations[name], prefix: prefixes[name]}
		fixtures = append(fixtures, f)
		if err := nativeFailure("manager scratch schema", c.DB().WithContext(ctx).AutoMigrate(&Patient{}, &Visit{})); err != nil {
			return err
		}
	}
	root, err := fixtures[0].repository()
	if err != nil {
		return err
	}
	if err := fixtures[1].client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
		_, err := root.InTransaction(tx)
		return expect(err, orm.ErrTransactionClientMismatch)
	}); err != nil {
		return err
	}
	for _, f := range fixtures {
		if err := f.client.Ping(ctx); err != nil {
			return err
		}
		if err := f.drop(ctx); err != nil {
			return err
		}
	}
	fixtures = nil // DDL cleanup precedes pool shutdown, including explicit Close.
	if err := manager.Close(ctx); err != nil {
		return err
	}
	if _, err := manager.Lookup(""); !errors.Is(err, orm.ErrClosed) {
		return fmt.Errorf("closed manager accepted lookup")
	}
	fmt.Println("  Init(2 pools), default/named Lookup, Add, duplicate/missing and Close verified")
	fmt.Println("  another client/pool cannot participate in the primary transaction")
	return nil
}
func awaitCondition(ctx context.Context, condition func() bool) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for !condition() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("demo synchronization timed out")
		case <-ticker.C:
		}
	}
	return nil
}
func demoAdmission(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, func(cfg *orm.Config) { cfg.MaxInFlight = 1; cfg.MaxQueued = 1 }, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		row := patient("admission", "one")
		if err := r.Create(ctx, &row); err != nil {
			return err
		}
		started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		go func() {
			finished <- f.client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
				close(started)
				select {
				case <-release:
					return nil
				case <-tx.Context().Done():
					return tx.Context().Err()
				}
			})
		}()
		select {
		case <-started:
		case err := <-finished:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
		queued, cancel := context.WithCancel(ctx)
		defer cancel()
		queryResult := make(chan error, 1)
		go func() { _, err := r.FindByID(queued, row.ID); queryResult <- err }()
		if err := awaitCondition(ctx, func() bool { return f.client.AdmissionStats().Queued == 1 }); err != nil {
			return err
		}
		if _, err := r.FindByID(ctx, row.ID); !errors.Is(err, orm.ErrBusy) {
			return fmt.Errorf("full queue did not reject work")
		}
		cancel()
		if err := expect(<-queryResult, context.Canceled); err != nil {
			return err
		}
		timeout, stop := context.WithTimeout(ctx, 30*time.Millisecond)
		defer stop()
		if _, err := r.FindByID(timeout, row.ID); !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("queued deadline lost")
		}
		unblock()
		if err := <-finished; err != nil {
			return err
		}
		if _, err := r.FindByID(ctx, row.ID); err != nil {
			return err
		}
		s := f.client.AdmissionStats()
		fmt.Printf("  admitted=%d rejected=%d waits=%d canceled=%d in_flight=%d queued=%d\n", s.Admitted, s.Rejected, s.Waits, s.Canceled, s.InFlight, s.Queued)
		return nil
	})
}
func demoLifecycle(ctx context.Context, opt options) (result error) {
	cfg, _, telemetry, err := newConfig(opt)
	if err != nil {
		return err
	}
	if telemetry != nil {
		defer func() { result = errors.Join(result, telemetry.close()) }()
	}
	c, err := orm.Open(ctx, cfg)
	if err != nil {
		return err
	}
	// No tables are created: shutdown can be exercised without leaving scratch DDL.
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer func() {
		unblock()
		cleanup, cancel := cleanupContext()
		defer cancel()
		result = errors.Join(result, c.Close(cleanup))
	}()
	go func() {
		finished <- c.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-tx.Context().Done():
				return tx.Context().Err()
			}
		})
	}()
	select {
	case <-started:
	case err := <-finished:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := expect(c.Close(short), context.DeadlineExceeded); err != nil {
		return err
	}
	if err := expect(c.Ping(ctx), orm.ErrClosed); err != nil {
		return err
	}
	unblock()
	if err := <-finished; err != nil {
		return err
	}
	if err := c.Close(ctx); err != nil {
		return err
	}
	if err := c.Close(ctx); err != nil {
		return err
	}
	s := c.TransactionStats()
	fmt.Printf("  Close timed out waiting, retry completed; committed=%d active=%d\n", s.Committed, s.Active)
	return nil
}
