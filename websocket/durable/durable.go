// Package durable defines delivery contracts for persistent WebSocket events.
// Best-effort fan-out remains in websocket/pubsub and deliberately does not
// implement this interface.
package durable

import (
	"context"
	"errors"
	"time"
)

var ErrConsumerClosed = errors.New("websocket durable consumer closed")

type Message struct {
	ID        string
	Subject   string
	Data      []byte
	Timestamp time.Time
}

type PublishOptions struct {
	DeduplicationID string
}

type ConsumerOptions struct {
	Name       string
	BatchSize  int
	AckTimeout time.Duration
}

type Consumer interface {
	Receive(context.Context) (Message, error)
	Ack(context.Context, Message) error
	Nack(context.Context, Message) error
	Close() error
}

type Bus interface {
	Publish(context.Context, string, []byte, PublishOptions) (Message, error)
	Consume(context.Context, string, ConsumerOptions) (Consumer, error)
	WaitReady(context.Context) error
	Close() error
}
