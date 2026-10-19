package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	natsDLQRecordVersion = 1
	maxDLQAdminBatch     = 1000
)

type dlqRecord struct {
	Version        int               `json:"v"`
	Message        transport.Message `json:"message"`
	Reason         string            `json:"reason"`
	LastError      string            `json:"error"`
	Attempts       int               `json:"attempts"`
	FailedAt       int64             `json:"failed_at"`
	Source         string            `json:"source"`
	SourceGroup    string            `json:"source_group"`
	SourcePosition string            `json:"source_position"`
}

func (d *natsDelivery) deadLetterRecord(msg transport.Message, decision transport.Decision) ([]byte, error) {
	if len(msg.Payload) == 0 {
		return nil, fmt.Errorf("nats: DLQ payload required")
	}
	failedAt := decision.FailedAt
	if failedAt.IsZero() {
		failedAt = d.transport.now()
	}
	record := dlqRecord{
		Version: natsDLQRecordVersion, Message: msg,
		Attempts: decision.Attempts, FailedAt: failedAt.UnixMilli(),
	}
	record.Reason, record.LastError = transport.NormalizeDecisionMetadata(decision)
	if metadata, err := d.nmsg.Metadata(); err == nil {
		record.Source = metadata.Stream
		record.SourceGroup = metadata.Consumer
		record.SourcePosition = strconv.FormatUint(metadata.Sequence.Stream, 10)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("nats: encode DLQ record: %w", err)
	}
	if len(encoded) > transport.MaxEncodedDLQRecordBytes {
		return nil, fmt.Errorf("nats: encoded DLQ record exceeds %d bytes", transport.MaxEncodedDLQRecordBytes)
	}
	return encoded, nil
}

// ListPage returns DLQ records oldest first. The cursor is the opaque decimal
// JetStream sequence of the last returned record and remains stable when older
// records are deleted.
func (t *natsTransport) ListPage(ctx context.Context, topology transport.Topology, cursor string, limit int) (transport.DLQPage, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return transport.DLQPage{}, err
	}
	if limit < 1 || limit > maxDLQAdminBatch {
		return transport.DLQPage{}, fmt.Errorf("nats: DLQ page limit must be between 1 and %d", maxDLQAdminBatch)
	}
	stream := t.dlqStreamName(topology)
	info, err := t.js.StreamInfo(stream, gonats.Context(ctx))
	if err != nil {
		return transport.DLQPage{}, fmt.Errorf("nats: inspect DLQ stream: %w", err)
	}
	sequence := info.State.FirstSeq
	if cursor != "" {
		parsed, err := strconv.ParseUint(cursor, 10, 64)
		if err != nil || parsed == ^uint64(0) {
			return transport.DLQPage{}, fmt.Errorf("nats: invalid DLQ cursor")
		}
		sequence = parsed + 1
	}

	records := make([]transport.DeadLetter, 0, limit+1)
	for sequence <= info.State.LastSeq && len(records) <= limit {
		if err := ctx.Err(); err != nil {
			return transport.DLQPage{}, err
		}
		raw, err := t.js.GetMsg(stream, sequence, gonats.Context(ctx))
		sequence++
		if errors.Is(err, gonats.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return transport.DLQPage{}, fmt.Errorf("nats: read DLQ record: %w", err)
		}
		record, err := decodeDLQRecord(raw)
		if err != nil {
			return transport.DLQPage{}, err
		}
		records = append(records, record)
	}
	page := transport.DLQPage{Records: records}
	if len(page.Records) > limit {
		page.Records = page.Records[:limit]
		page.NextCursor = page.Records[len(page.Records)-1].ID
	}
	return page, nil
}

func (t *natsTransport) List(ctx context.Context, topology transport.Topology, offset, limit int) ([]transport.DeadLetter, error) {
	if offset < 0 {
		return nil, fmt.Errorf("nats: DLQ offset must not be negative")
	}
	if limit < 1 || limit > maxDLQAdminBatch {
		return nil, fmt.Errorf("nats: DLQ page limit must be between 1 and %d", maxDLQAdminBatch)
	}
	cursor := ""
	remaining := offset
	for remaining > 0 {
		step := min(remaining, maxDLQAdminBatch)
		page, err := t.ListPage(ctx, topology, cursor, step)
		if err != nil {
			return nil, err
		}
		if len(page.Records) < step {
			return nil, nil
		}
		remaining -= len(page.Records)
		cursor = page.Records[len(page.Records)-1].ID
	}
	page, err := t.ListPage(ctx, topology, cursor, limit)
	if err != nil {
		return nil, err
	}
	return page.Records, nil
}

func (t *natsTransport) Requeue(ctx context.Context, topology transport.Topology, messageIDs []string) error {
	if err := t.ValidateTopology(topology); err != nil {
		return err
	}
	if len(messageIDs) > maxDLQAdminBatch {
		return fmt.Errorf("nats: DLQ requeue batch exceeds %d records", maxDLQAdminBatch)
	}
	stream := t.dlqStreamName(topology)
	seen := make(map[uint64]struct{}, len(messageIDs))
	for _, id := range messageIDs {
		sequence, err := strconv.ParseUint(id, 10, 64)
		if err != nil || sequence == 0 {
			return fmt.Errorf("nats: invalid DLQ record ID")
		}
		if _, exists := seen[sequence]; exists {
			continue
		}
		seen[sequence] = struct{}{}
		raw, err := t.js.GetMsg(stream, sequence, gonats.Context(ctx))
		if errors.Is(err, gonats.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("nats: read DLQ record %d: %w", sequence, err)
		}
		record, err := decodeDLQRecord(raw)
		if err != nil {
			return err
		}
		brokerID := stableTransferID("dlq-requeue", stream+":"+id)
		if _, err := t.publish(ctx, t.streamName(topology), record.Message.ID, brokerID, record.Message.Payload); err != nil {
			return fmt.Errorf("nats: requeue DLQ record %d: %w", sequence, err)
		}
		if err := t.js.DeleteMsg(stream, sequence, gonats.Context(ctx)); err != nil && !errors.Is(err, gonats.ErrMsgNotFound) {
			return fmt.Errorf("nats: delete requeued DLQ record %d: %w", sequence, err)
		}
	}
	return nil
}

func (t *natsTransport) Purge(ctx context.Context, topology transport.Topology) error {
	if err := t.ValidateTopology(topology); err != nil {
		return err
	}
	if err := t.js.PurgeStream(t.dlqStreamName(topology), gonats.Context(ctx)); err != nil {
		return fmt.Errorf("nats: purge DLQ: %w", err)
	}
	return nil
}

func (t *natsTransport) Count(ctx context.Context, topology transport.Topology) (int64, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return 0, err
	}
	info, err := t.js.StreamInfo(t.dlqStreamName(topology), gonats.Context(ctx))
	if err != nil {
		return 0, fmt.Errorf("nats: count DLQ: %w", err)
	}
	return checkedNATSCount("DLQ", info.State.Msgs)
}

func decodeDLQRecord(raw *gonats.RawStreamMsg) (transport.DeadLetter, error) {
	var record dlqRecord
	if err := json.Unmarshal(raw.Data, &record); err != nil || record.Version != natsDLQRecordVersion {
		return transport.DeadLetter{}, fmt.Errorf("nats: DLQ record %d is malformed or unsupported", raw.Sequence)
	}
	if len(record.Message.Payload) == 0 || record.Attempts < 0 || record.FailedAt < 0 {
		return transport.DeadLetter{}, fmt.Errorf("nats: DLQ record %d has invalid fields", raw.Sequence)
	}
	return transport.DeadLetter{
		ID: strconv.FormatUint(raw.Sequence, 10), Message: record.Message,
		Reason: record.Reason, LastError: record.LastError, Attempts: record.Attempts,
		FailedAt: time.UnixMilli(record.FailedAt), Source: record.Source,
		SourceGroup: record.SourceGroup, SourcePosition: record.SourcePosition,
	}, nil
}
