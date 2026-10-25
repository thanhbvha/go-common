package eventbus

import (
	"math"
	"testing"
	"time"

	"github.com/goccy/go-json"
)

type TestPayload struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func TestCodecEncodesAndValidatesDedupeTTL(t *testing.T) {
	codec := NewCodec(nil)
	data, env, err := codec.Encode("test", 1, 1, WithDedupeKey("order-1"), WithDedupeTTL(750*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if env.DedupeTTL != int64(750*time.Millisecond) {
		t.Fatalf("dedupe TTL = %dns, want 750ms", env.DedupeTTL)
	}
	decoded, err := codec.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.DedupeTTL != int64(750*time.Millisecond) {
		t.Fatalf("decoded dedupe TTL = %dns, want 750ms", decoded.DedupeTTL)
	}
	if _, _, err := codec.Encode("test", 1, 1, WithDedupeTTL(time.Second)); err == nil {
		t.Fatal("dedupe TTL without key was accepted")
	}
	if _, _, err := codec.Encode("test", 1, 1, WithDedupeKey("key"), WithDedupeTTL(-time.Second)); err == nil {
		t.Fatal("negative dedupe TTL was accepted")
	}
}

func TestBug_C2_CodecInt64Precision(t *testing.T) {
	codec := NewCodec(nil)

	// An int64 larger than 2^53, which would lose precision if unmarshaled into float64.
	largeID := int64(math.MaxInt64 - 5)

	payload := TestPayload{
		ID:   largeID,
		Name: "test-precision",
	}

	data, env, err := codec.Encode("test", payload, 3)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if env.V != 3 {
		t.Fatalf("Expected version 3, got %d", env.V)
	}
	if env.Attempt != 1 || env.MaxAttempts != 3 {
		t.Fatalf("unexpected attempt bounds: attempt=%d max=%d", env.Attempt, env.MaxAttempts)
	}

	decodedEnv, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	job := Job{env: decodedEnv}

	var decodedPayload TestPayload
	err = job.Decode(&decodedPayload)
	if err != nil {
		t.Fatalf("job.Decode: %v", err)
	}

	if decodedPayload.ID != largeID {
		t.Fatalf("Precision lost! Expected %d, got %d", largeID, decodedPayload.ID)
	}

	t.Logf("Bug C2 fixed: large int64 %d preserved exactly", decodedPayload.ID)
}

func TestCodecDecodeV2NormalizesAttemptSemantics(t *testing.T) {
	tests := []struct {
		name        string
		attempt     int
		maxRetry    int
		wantAttempt int
		wantMax     int
	}{
		{name: "initial delivery", attempt: 0, maxRetry: 3, wantAttempt: 1, wantMax: 3},
		{name: "later delivery", attempt: 2, maxRetry: 5, wantAttempt: 3, wantMax: 5},
		{name: "invalid legacy maximum is bounded", attempt: 0, maxRetry: 0, wantAttempt: 1, wantMax: 1},
	}

	codec := NewCodec(nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(Envelope{
				V:        2,
				ID:       "legacy-id",
				Type:     "legacy-task",
				Payload:  json.RawMessage(`{"value":1}`),
				Attempt:  tt.attempt,
				MaxRetry: tt.maxRetry,
			})
			if err != nil {
				t.Fatalf("marshal v2 envelope: %v", err)
			}

			env, err := codec.Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if env.V != 3 || env.Kind != TaskTopology {
				t.Fatalf("legacy envelope not normalized: version=%d kind=%s", env.V, env.Kind)
			}
			if env.Attempt != tt.wantAttempt || env.MaxAttempts != tt.wantMax {
				t.Fatalf("attempt bounds = (%d, %d), want (%d, %d)", env.Attempt, env.MaxAttempts, tt.wantAttempt, tt.wantMax)
			}
		})
	}
}

func TestCodecEncodeTopologyV3(t *testing.T) {
	codec := NewCodec(nil)
	data, env, err := codec.EncodeTopology(EventTopology, "created", map[string]int{"id": 1}, 4)
	if err != nil {
		t.Fatalf("EncodeTopology: %v", err)
	}
	if env.V != 3 || env.Kind != EventTopology || env.Attempt != 1 || env.MaxAttempts != 4 {
		t.Fatalf("unexpected v3 envelope: %+v", env)
	}

	var wire map[string]json.RawMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatalf("decode wire JSON: %v", err)
	}
	if _, ok := wire["max_attempts"]; !ok {
		t.Fatal("v3 envelope does not contain max_attempts")
	}
	if _, ok := wire["max_retry"]; ok {
		t.Fatal("v3 envelope unexpectedly contains legacy max_retry")
	}
}
