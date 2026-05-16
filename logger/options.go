package logger

import "log/slog"

// Options configures the behavior of a Logger instance.
type Options struct {
	// Level sets the minimum log level. Records below this level are discarded.
	// Default: slog.LevelInfo.
	Level slog.Level

	// StdOut enables writing log records to standard output.
	// Default: true. If File is nil and StdOut is false, StdOut is forced on.
	StdOut bool

	// File configures optional file-based log rotation via lumberjack.
	// Set to nil to disable file logging entirely.
	File *FileOptions

	// BufferSize is the capacity of the async dispatch channel.
	// A larger buffer reduces back-pressure under burst load.
	// Default: 100_000.
	BufferSize int

	// NumWorkers is the number of goroutines draining the async channel.
	// Default: runtime.NumCPU() * 2 (minimum 1).
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
	// Default: true.
	Compress bool
}

// DefaultOptions returns an Options value with sensible defaults.
// StdOut is enabled, file logging is disabled, INFO level is set.
func DefaultOptions() Options {
	return Options{
		Level:      slog.LevelInfo,
		StdOut:     true,
		File:       nil,
		BufferSize: 100_000,
		NumWorkers: 0, // resolved at runtime
	}
}
