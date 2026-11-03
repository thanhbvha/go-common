package logger

import (
	"io"
	"log/slog"
)

// OverflowPolicy applies when the async queue is full.
type OverflowPolicy uint8

const (
	// OverflowSync preserves records by writing on the caller.
	OverflowSync OverflowPolicy = iota
	// OverflowDrop reports ErrDropped through OnError.
	OverflowDrop
)

// Options configures the behavior of a Logger instance.
type Options struct {
	// Writer is a borrowed output. The runtime never closes it.
	Writer io.Writer
	// Async enables a bounded queue. The default is synchronous.
	Async bool
	// Overflow defaults to OverflowSync.
	Overflow OverflowPolicy
	// OnError observes writer errors and rejected records; it must not block.
	OnError func(error)

	// Level sets the minimum log level. Records below this level are discarded.
	// Default: slog.LevelInfo.
	Level slog.Level

	// StdOut enables writing log records to standard output.
	// Default: true. If File is nil and StdOut is false, StdOut is forced on.
	StdOut bool

	// TextFormat enables plain text logging instead of JSON logging.
	// This is useful for local development to make logs easier to read.
	// Default: false (JSON format).
	TextFormat bool

	// File configures optional file-based log rotation via lumberjack.
	// Set to nil to disable file logging entirely.
	File *FileOptions

	// BufferSize is the capacity of the async dispatch channel.
	// A larger buffer reduces back-pressure under burst load.
	// Default: 1024 when Async is enabled.
	BufferSize int

	// NumWorkers is the number of goroutines draining the async channel.
	// Default: 1 when Async is enabled.
	NumWorkers int
}

// FileOptions configures log file rotation through lumberjack.
// All fields have built-in defaults applied when the value is zero.
type FileOptions struct {
	// Path is the destination log file path (required when File is non-nil).
	Path string

	// MaxSizeMB is the maximum size in megabytes before the file is rotated.
	// Default: 10.
	MaxSizeMB int

	// MaxBackups is the maximum number of rotated files to retain.
	// Default: 5.
	MaxBackups int

	// MaxAgeDays is the maximum number of days to retain rotated files.
	// Default: 30.
	MaxAgeDays int

	// Compress determines whether rotated files are gzip-compressed.
	// Default: false.
	Compress bool
}

// DefaultOptions returns an Options value with sensible defaults.
// StdOut is enabled, file logging is disabled, INFO level is set.
func DefaultOptions() Options {
	return Options{
		Level:      slog.LevelInfo,
		StdOut:     true,
		File:       nil,
		BufferSize: 1024,
		NumWorkers: 0, // resolved at runtime
	}
}
