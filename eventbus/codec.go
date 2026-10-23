package eventbus

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

// Codec handles serialization and deserialization of Envelopes.
type Codec struct {
	clock Clock
}

func NewCodec(clock Clock) *Codec {
	if clock == nil {
		clock = realClock{}
	}
	return &Codec{clock: clock}
}

// Encode builds a task Envelope v3 and serializes it to JSON.
//
// Deprecated: use EncodeTopology when the topology kind is known.
func (c *Codec) Encode(jobType string, payload any, maxAttemptsDefault int, opts ...JobOption) ([]byte, *Envelope, error) {
	return c.EncodeTopology(TaskTopology, jobType, payload, maxAttemptsDefault, opts...)
}

// EncodeTopology builds an Envelope v3 and serializes it to JSON.
func (c *Codec) EncodeTopology(kind TopologyKind, jobType string, payload any, maxAttemptsDefault int, opts ...JobOption) ([]byte, *Envelope, error) {
	env, err := c.buildEnvelope(kind, jobType, payload, maxAttemptsDefault, opts...)
	if err != nil {
		return nil, nil, err
	}
	data, err := json.Marshal(env)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode envelope: %w", err)
	}
	return data, env, nil
}

func (c *Codec) buildEnvelope(kind TopologyKind, jobType string, payload any, maxAttemptsDefault int, opts ...JobOption) (*Envelope, error) {
	if err := validateName("topology name", jobType); err != nil {
		return nil, err
	}
	if kind != TaskTopology && kind != EventTopology {
		return nil, fmt.Errorf("%w: unknown topology kind %d", ErrInvalidOption, kind)
	}
	if maxAttemptsDefault < 0 {
		return nil, fmt.Errorf("%w: max attempts must not be negative", ErrInvalidOption)
	}
	options := &JobOptions{}
	if err := applyJobOptions(options, opts); err != nil {
		return nil, err
	}

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	now := c.clock.Now()

	runAt := int64(0)
	if !options.RunAt.IsZero() {
		runAt = options.RunAt.UnixMilli()
	} else if options.Delay > 0 {
		runAt = now.Add(options.Delay).UnixMilli()
	}

	maxAttempts := max(maxAttemptsDefault, 1)

	env := &Envelope{
		V:           3,
		ID:          uuid.NewString(),
		Kind:        kind,
		Type:        jobType,
		Payload:     json.RawMessage(rawPayload),
		Attempt:     1,
		MaxAttempts: maxAttempts,
		CreatedAt:   now.UnixMilli(),
		RunAt:       runAt,
		OrderingKey: options.OrderingKey,
		DedupeKey:   options.DedupeKey,
		DedupeTTL:   int64(options.DedupeTTL),
		Headers:     options.Headers,
	}

	return env, nil
}

// Decode parses v3 envelopes and normalizes legacy v2 envelopes to v3
// attempt semantics.
func (c *Codec) Decode(data []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("failed to decode envelope: %w", err)
	}

	switch env.V {
	case 2:
		env.legacy = true
		env.V = 3
		env.Kind = TaskTopology
		env.Attempt++
		env.MaxAttempts = max(env.MaxRetry, 1)
		env.MaxRetry = 0
	case 3:
		if env.Attempt < 1 {
			return nil, fmt.Errorf("invalid envelope attempt: %d", env.Attempt)
		}
		if env.MaxAttempts < 1 {
			return nil, fmt.Errorf("invalid envelope max_attempts: %d", env.MaxAttempts)
		}
	default:
		return nil, fmt.Errorf("unsupported envelope version: %d", env.V)
	}

	return &env, nil
}
