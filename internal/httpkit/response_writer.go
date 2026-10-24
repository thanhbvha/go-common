// Package httpkit contains internal, framework-independent HTTP primitives
// shared by the repository's net/http adapters.
package httpkit

import (
	"bufio"
	"io"
	"net"
	"net/http"
)

// ResponseWriter records the response status and byte count while forwarding
// capabilities commonly required by streaming and WebSocket handlers.
type ResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	committed    bool
}

// NewResponseWriter wraps w with response metadata recording.
func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{ResponseWriter: w}
}

// Unwrap returns the underlying response writer for http.ResponseController.
func (w *ResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// WriteHeader records and forwards the first response status code.
func (w *ResponseWriter) WriteHeader(statusCode int) {
	if w.committed {
		return
	}
	w.statusCode = statusCode
	w.committed = true
	w.ResponseWriter.WriteHeader(statusCode)
}

// Write records bytes written and implicitly commits HTTP 200 when needed.
func (w *ResponseWriter) Write(data []byte) (int, error) {
	if !w.committed {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytesWritten += int64(n)
	return n, err
}

// ReadFrom preserves io.ReaderFrom optimizations while recording the response.
func (w *ResponseWriter) ReadFrom(src io.Reader) (int64, error) {
	if !w.committed {
		w.WriteHeader(http.StatusOK)
	}
	var (
		n   int64
		err error
	)
	if readerFrom, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err = readerFrom.ReadFrom(src)
	} else {
		n, err = io.Copy(w.ResponseWriter, src)
	}
	w.bytesWritten += n
	return n, err
}

// StatusCode returns the effective response status. An untouched response is
// treated as HTTP 200, matching net/http behavior.
func (w *ResponseWriter) StatusCode() int {
	if !w.committed {
		return http.StatusOK
	}
	return w.statusCode
}

// BytesWritten returns the number of response body bytes forwarded.
func (w *ResponseWriter) BytesWritten() int64 {
	return w.bytesWritten
}

// Committed reports whether headers or body have been written.
func (w *ResponseWriter) Committed() bool {
	return w.committed
}

// Flush forwards streaming flushes when supported by the underlying writer.
func (w *ResponseWriter) Flush() {
	_ = w.FlushError()
}

// FlushError is understood by http.ResponseController and reports unsupported
// streaming rather than silently succeeding.
func (w *ResponseWriter) FlushError() error {
	if !w.committed {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(interface{ FlushError() error }); ok {
		return flusher.FlushError()
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
		return nil
	}
	return http.ErrNotSupported
}

// Hijack forwards connection takeover for WebSocket and similar protocols.
func (w *ResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	conn, readWriter, err := hijacker.Hijack()
	if err == nil && !w.committed {
		w.statusCode = http.StatusSwitchingProtocols
		w.committed = true
	}
	return conn, readWriter, err
}

// Push forwards HTTP/2 server pushes when supported.
func (w *ResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

var (
	_ http.ResponseWriter = (*ResponseWriter)(nil)
	_ http.Flusher        = (*ResponseWriter)(nil)
	_ http.Hijacker       = (*ResponseWriter)(nil)
	_ http.Pusher         = (*ResponseWriter)(nil)
	_ io.ReaderFrom       = (*ResponseWriter)(nil)
)
