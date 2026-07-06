package nats

import (
	"context"
	"github.com/goccy/go-json"
	"fmt"
)

// ============================================================
// JSON helpers – JetStream
// ============================================================

// JSPublishJSON marshals v to JSON then calls JSPublish.
// Equivalent to XAdd with a JSON-encoded payload.
func (c *Client) JSPublishJSON(subject string, v any) (*PubAck, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("nats: JSPublishJSON marshal: %w", err)
	}
	return c.JSPublish(context.Background(), subject, data)
}

// DecodeJSON decodes the message payload into v.
// Call after Fetch to decode a JetStream message:
//
//	msgs, _ := client.Fetch(ps, 10)
//	var evt MyEvent
//	msgs[0].DecodeJSON(&evt)
func (m *Msg) DecodeJSON(v any) error {
	return json.Unmarshal(m.Data, v)
}

// ============================================================
// JSON helpers – Core Pub/Sub
// ============================================================

// PublishJSON marshals v to JSON and calls Publish.
func (c *Client) PublishJSON(subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("nats: PublishJSON marshal: %w", err)
	}
	return c.Publish(subject, data)
}

// ============================================================
// JSON helpers – KV Store
// ============================================================

// KVPutJSON marshals v to JSON and calls KVPut.
// Returns the new revision number.
func (c *Client) KVPutJSON(bucket, key string, v any) (uint64, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, fmt.Errorf("nats: KVPutJSON marshal: %w", err)
	}
	return c.KVPut(context.Background(), bucket, key, data)
}

// KVGetJSON calls KVGet and unmarshals the value into v.
func (c *Client) KVGetJSON(bucket, key string, v any) error {
	entry, err := c.KVGet(context.Background(), bucket, key)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(entry.Value, v); err != nil {
		return fmt.Errorf("nats: KVGetJSON unmarshal: %w", err)
	}
	return nil
}
