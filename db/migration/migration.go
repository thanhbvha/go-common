// Package migration provides instance-scoped migrations for a dedicated deployment
// job. It borrows its SQL pool; the caller retains responsibility for closing it.
package migration

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"

	"github.com/pressly/goose/v3"
)

// Options selects a migration filesystem and dialect. FS is rooted at Dir;
// without FS, Dir names a local directory. Empty Dir means the filesystem root.
// A Manager serializes its own operations. Run one deployment migration job
// per database; it does not install a cross-process migration locker.
type Options struct {
	DB      *sql.DB
	Dialect string
	Dir     string
	FS      fs.FS
}

// Manager keeps dialect, filesystem, and Go migration registry isolated. It does
// not close the borrowed DB and never modifies goose's process-global settings.
type Manager struct{ provider *goose.Provider }

func New(opts Options) (*Manager, error) {
	if opts.DB == nil || opts.Dialect == "" {
		return nil, fmt.Errorf("migration: database and dialect required")
	}
	if opts.Dir == "" {
		opts.Dir = "."
	}
	var files fs.FS
	if opts.FS != nil {
		var err error
		files, err = fs.Sub(opts.FS, opts.Dir)
		if err != nil {
			return nil, fmt.Errorf("migration filesystem: %w", err)
		}
	} else {
		files = os.DirFS(opts.Dir)
	}
	provider, err := goose.NewProvider(goose.Dialect(opts.Dialect), opts.DB, files, goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return nil, fmt.Errorf("migration: initialize: %w", err)
	}
	return &Manager{provider: provider}, nil
}
func (m *Manager) Up(ctx context.Context) ([]*goose.MigrationResult, error) {
	return m.provider.Up(ctx)
}
func (m *Manager) Down(ctx context.Context) (*goose.MigrationResult, error) {
	return m.provider.Down(ctx)
}
func (m *Manager) UpTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	return m.provider.UpTo(ctx, version)
}
func (m *Manager) DownTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	return m.provider.DownTo(ctx, version)
}
func (m *Manager) Status(ctx context.Context) ([]*goose.MigrationStatus, error) {
	return m.provider.Status(ctx)
}
