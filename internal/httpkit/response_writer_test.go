package httpkit

import (
	"bufio"
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResponseWriterRecordsResponse(t *testing.T) {
	recorder := httptest.NewRecorder()
	w := NewResponseWriter(recorder)

	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	w.WriteHeader(http.StatusCreated)

	if got := w.StatusCode(); got != http.StatusOK {
		t.Fatalf("StatusCode() = %d, want %d", got, http.StatusOK)
	}
	if got := w.BytesWritten(); got != 5 {
		t.Fatalf("BytesWritten() = %d, want 5", got)
	}
	if !w.Committed() || recorder.Body.String() != "hello" {
		t.Fatalf("unexpected response state: committed=%v body=%q", w.Committed(), recorder.Body.String())
	}
}

func TestResponseWriterReadFrom(t *testing.T) {
	recorder := httptest.NewRecorder()
	w := NewResponseWriter(recorder)
	n, err := w.ReadFrom(strings.NewReader("streamed"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 8 || w.BytesWritten() != 8 || recorder.Body.String() != "streamed" {
		t.Fatalf("unexpected streamed response: n=%d bytes=%d body=%q", n, w.BytesWritten(), recorder.Body.String())
	}
}

func TestResponseWriterForwardsOptionalInterfaces(t *testing.T) {
	base := &capableWriter{header: make(http.Header)}
	w := NewResponseWriter(base)

	w.Flush()
	if !base.flushed {
		t.Fatal("Flush was not forwarded")
	}
	if err := w.Push("/asset", nil); err != nil || base.pushed != "/asset" {
		t.Fatalf("Push() error = %v, target = %q", err, base.pushed)
	}
	conn, _, err := w.Hijack()
	if err != nil || conn == nil || !base.hijacked {
		t.Fatalf("Hijack() = %v, %v", conn, err)
	}
	if !w.Committed() || w.StatusCode() != http.StatusOK {
		t.Fatalf("hijacked response: committed=%v status=%d", w.Committed(), w.StatusCode())
	}
	_ = conn.Close()
	_ = base.peer.Close()
	if w.Unwrap() != base {
		t.Fatal("Unwrap did not return the underlying writer")
	}
}

func TestResponseWriterHijackCommitsUntouchedResponse(t *testing.T) {
	base := &capableWriter{header: make(http.Header)}
	w := NewResponseWriter(base)

	conn, _, err := w.Hijack()
	if err != nil {
		t.Fatal(err)
	}
	if !w.Committed() || w.StatusCode() != http.StatusSwitchingProtocols {
		t.Fatalf("hijacked response: committed=%v status=%d", w.Committed(), w.StatusCode())
	}
	_ = conn.Close()
	_ = base.peer.Close()
}

type capableWriter struct {
	header   http.Header
	body     bytes.Buffer
	flushed  bool
	hijacked bool
	pushed   string
	peer     net.Conn
}

func (w *capableWriter) Header() http.Header         { return w.header }
func (w *capableWriter) WriteHeader(int)             {}
func (w *capableWriter) Write(p []byte) (int, error) { return w.body.Write(p) }
func (w *capableWriter) Flush()                      { w.flushed = true }
func (w *capableWriter) Push(target string, _ *http.PushOptions) error {
	w.pushed = target
	return nil
}
func (w *capableWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	server, peer := net.Pipe()
	w.peer = peer
	w.hijacked = true
	return server, bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server)), nil
}
