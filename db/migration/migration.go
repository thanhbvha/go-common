package migration

import (
	"database/sql"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
)

// Options defines configuration for migrations.
type Options struct {
	// DB is the *sql.DB instance.
	DB *sql.DB

	// Dialect is the database dialect (e.g. "postgres", "mysql", "sqlite3").
	Dialect string

	// Dir is the path to the directory containing migration files.
	// Defaults to "." if empty.
	// If FS is provided, this represents the directory inside the embedded FS.
	Dir string

	// FS is an optional embedded filesystem containing migration files.
	// Highly recommended for production to embed SQL files into the binary.
	FS fs.FS
}

// Manager handles database migrations wrapping goose.
type Manager struct {
	opts Options
}

// New creates a new migration manager.
func New(opts Options) (*Manager, error) {
	if opts.DB == nil {
		return nil, fmt.Errorf("migration: db instance is required")
	}
	if opts.Dialect == "" {
		return nil, fmt.Errorf("migration: dialect is required")
	}

	if err := goose.SetDialect(opts.Dialect); err != nil {
		return nil, fmt.Errorf("migration: failed to set dialect %q: %w", opts.Dialect, err)
	}

	if opts.FS != nil {
		goose.SetBaseFS(opts.FS)
	}

	if opts.Dir == "" {
		opts.Dir = "."
	}

	return &Manager{opts: opts}, nil
}

// Up applies all available migrations.
func (m *Manager) Up() error {
	if err := goose.Up(m.opts.DB, m.opts.Dir); err != nil {
		return fmt.Errorf("migration up failed: %w", err)
	}
	return nil
}

// Down rolls back a single migration from the current version.
func (m *Manager) Down() error {
	if err := goose.Down(m.opts.DB, m.opts.Dir); err != nil {
		return fmt.Errorf("migration down failed: %w", err)
	}
	return nil
}

// Status prints the status of all migrations.
func (m *Manager) Status() error {
	if err := goose.Status(m.opts.DB, m.opts.Dir); err != nil {
		return fmt.Errorf("migration status failed: %w", err)
	}
	return nil
}

// UpTo migrates up to a specific version.
func (m *Manager) UpTo(version int64) error {
	if err := goose.UpTo(m.opts.DB, m.opts.Dir, version); err != nil {
		return fmt.Errorf("migration up to %d failed: %w", version, err)
	}
	return nil
}

// DownTo migrates down to a specific version.
func (m *Manager) DownTo(version int64) error {
	if err := goose.DownTo(m.opts.DB, m.opts.Dir, version); err != nil {
		return fmt.Errorf("migration down to %d failed: %w", version, err)
	}
	return nil
}
