package nats

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	delayRecordVersion       = 3
	legacyDelayRecordVersion = 2
	delayCursorKey           = "meta.cursor"
	delaySlotWidth           = time.Second
	defaultDelayBatchSize    = 100
	defaultDelayLease        = 30 * time.Second
	defaultDelayCatchupSlots = 60
	delayQuarantinePrefix    = "quarantine."
)

type delayRecord struct {
	Version    int    `json:"v"`
	ScheduleID string `json:"schedule_id"`
	MessageID  string `json:"message_id,omitempty"`
	RunAt      int64  `json:"run_at"`
	Payload    []byte `json:"payload"`
	Owner      string `json:"owner,omitempty"`
	LeaseUntil int64  `json:"lease_until,omitempty"`
	TransferID string `json:"transfer_id,omitempty"`
	Target     string `json:"target,omitempty"`
}

type delayTransfer struct {
	Version int         `json:"v"`
	Key     string      `json:"key"`
	Record  delayRecord `json:"record"`
}

func newSchedulerID() string { return uuid.NewString() }

func stableTransferID(kind, source string) string {
	hash := sha256.Sum256([]byte(kind + ":" + source))
	return fmt.Sprintf("%s-%x", kind, hash[:])
}

func delaySlot(at time.Time) int64 { return at.Unix() / int64(delaySlotWidth/time.Second) }

func delayKey(minute, runAtMillis int64, scheduleID string) string {
	return fmt.Sprintf("v2.%020d.%020d.%s", minute, runAtMillis, scheduleID)
}

func delaySlotPattern(minute int64) string { return fmt.Sprintf("v2.%020d.>", minute) }

func delayBatchSize(topo transport.Topology) int {
	if value, ok := topo.Options["nats.delay_batch_size"].(int); ok {
		return value
	}
	return defaultDelayBatchSize
}

func delayLease(topo transport.Topology) time.Duration {
	if value, ok := topo.Options["nats.delay_lease"].(time.Duration); ok {
		return value
	}
	return defaultDelayLease
}

func (t *natsTransport) schedule(ctx context.Context, kv gonats.KeyValue, msg transport.Message, runAt time.Time) error {
	for range 4 {
		if err := ctx.Err(); err != nil {
			return err
		}
		scheduleID := newSchedulerID()
		record := delayRecord{
			Version: delayRecordVersion, ScheduleID: scheduleID, MessageID: msg.ID,
			RunAt: runAt.UnixMilli(), Payload: msg.Payload,
		}
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("nats delay encode: %w", err)
		}
		key := delayKey(delaySlot(runAt), record.RunAt, scheduleID)
		if _, err := kv.Create(key, data); err == nil {
			return nil
		} else if !errors.Is(err, gonats.ErrKeyExists) {
			return fmt.Errorf("nats delay create: %w", err)
		}
	}
	return fmt.Errorf("nats delay: failed to allocate a collision-free schedule ID")
}

func (t *natsTransport) scheduleRetry(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time, transferID, target string) error {
	if err := t.validateEventLayout(ctx, topo); err != nil {
		return err
	}
	kv, err := t.getKV(t.kvBucketName(topo))
	if err != nil {
		return fmt.Errorf("nats retry open delay bucket: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target != "" {
		return t.withSubscriberLane(ctx, topo, target, "schedule", func(lease *subscriberLaneLeaseHandle, _ subscriberLaneState) error {
			return t.scheduleRetryRecord(ctx, kv, msg, runAt, transferID, target, lease)
		})
	}
	return t.scheduleRetryRecord(ctx, kv, msg, runAt, transferID, target, nil)
}

func (t *natsTransport) scheduleRetryRecord(ctx context.Context, kv gonats.KeyValue, msg transport.Message, runAt time.Time, transferID, target string, lease *subscriberLaneLeaseHandle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scheduledAt := runAt
	if now := t.now(); !scheduledAt.After(now) {
		scheduledAt = now
	}
	slot := delaySlot(scheduledAt)
	record := delayRecord{
		Version: delayRecordVersion, ScheduleID: transferID, MessageID: msg.ID,
		RunAt: scheduledAt.UnixMilli(), Payload: msg.Payload, TransferID: transferID, Target: target,
	}
	transfer := delayTransfer{
		Version: delayRecordVersion,
		Key:     delayKey(slot, record.RunAt, transferID),
		Record:  record,
	}
	markerKey := "transfer." + transferID
	marker, err := json.Marshal(transfer)
	if err != nil {
		return fmt.Errorf("nats retry encode transfer: %w", err)
	}
	if lease != nil {
		if err := lease.checkOwnership(ctx); err != nil {
			return err
		}
	}
	if _, err := kv.Create(markerKey, marker); err != nil {
		if !errors.Is(err, gonats.ErrKeyExists) {
			return fmt.Errorf("nats retry create transfer: %w", err)
		}
		entry, getErr := kv.Get(markerKey)
		if getErr != nil {
			return fmt.Errorf("nats retry read transfer: %w", getErr)
		}
		if err := json.Unmarshal(entry.Value(), &transfer); err != nil {
			return fmt.Errorf("nats retry decode transfer: %w", err)
		}
		if transfer.Version != delayRecordVersion || transfer.Record.TransferID != transferID || transfer.Record.Target != target || transfer.Key == "" {
			return fmt.Errorf("nats retry invalid transfer %q", markerKey)
		}
	}
	if lease != nil {
		if err := lease.checkOwnership(ctx); err != nil {
			return err
		}
	}
	return createScheduledRecord(kv, transfer.Key, transfer.Record)
}

func createScheduledRecord(kv gonats.KeyValue, key string, record delayRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("nats delay encode record: %w", err)
	}
	if _, err := kv.Create(key, data); err == nil {
		return nil
	} else if !errors.Is(err, gonats.ErrKeyExists) {
		return fmt.Errorf("nats delay create record: %w", err)
	}
	entry, err := kv.Get(key)
	if err != nil {
		return fmt.Errorf("nats delay inspect existing record: %w", err)
	}
	var existing delayRecord
	if err := json.Unmarshal(entry.Value(), &existing); err != nil {
		return fmt.Errorf("nats delay decode existing record: %w", err)
	}
	if existing.Version != record.Version || existing.ScheduleID != record.ScheduleID ||
		existing.TransferID != record.TransferID || existing.MessageID != record.MessageID ||
		existing.Target != record.Target || existing.RunAt != record.RunAt || !bytes.Equal(existing.Payload, record.Payload) {
		return fmt.Errorf("nats delay schedule identity collision for %q", key)
	}
	return nil
}

func (t *natsTransport) ensureDelayCursor(ctx context.Context, kv gonats.KeyValue) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entry, err := kv.Get(delayCursorKey)
	if err == nil {
		if _, err := strconv.ParseInt(string(entry.Value()), 10, 64); err != nil {
			return fmt.Errorf("invalid cursor: %w", err)
		}
		return nil
	}
	if !errors.Is(err, gonats.ErrKeyNotFound) && !errors.Is(err, gonats.ErrKeyDeleted) {
		return err
	}
	_, err = kv.Create(delayCursorKey, []byte(strconv.FormatInt(delaySlot(t.now()), 10)))
	if err != nil && !errors.Is(err, gonats.ErrKeyExists) {
		return err
	}
	return nil
}

func (t *natsTransport) delaySweeper(ctx context.Context, topo transport.Topology, kv gonats.KeyValue) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		_, _ = t.dispatchDue(ctx, topo, kv, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// dispatchDue performs bounded publication work without retaining the delayed
// backlog in process memory. KV watchers stream a fixed-size window internally.
func (t *natsTransport) dispatchDue(ctx context.Context, topo transport.Topology, kv gonats.KeyValue, now time.Time) (int, error) {
	t.recordSchedulerPoll(topo, now)
	if err := t.validateEventLayoutKV(topo, kv); err != nil {
		t.recordSchedulerError(topo, "poll", err, now)
		return 0, err
	}
	batch := delayBatchSize(topo)
	if _, err := t.migrateLegacyDelayed(ctx, topo, kv, now, batch); err != nil {
		t.recordSchedulerError(topo, "poll", err, now)
		return 0, err
	}
	entry, err := kv.Get(delayCursorKey)
	if err != nil {
		t.recordSchedulerError(topo, "poll", err, now)
		return 0, fmt.Errorf("nats delay read cursor: %w", err)
	}
	cursor, err := strconv.ParseInt(string(entry.Value()), 10, 64)
	if err != nil {
		t.recordSchedulerError(topo, "poll", err, now)
		return 0, fmt.Errorf("nats delay parse cursor: %w", err)
	}
	current := delaySlot(now)
	if current-cursor > defaultDelayCatchupSlots {
		entry, cursor, err = recoverDelayCursor(ctx, kv, entry, cursor, current)
		if err != nil {
			t.recordSchedulerError(topo, "dispatch", err, now)
			return 0, err
		}
	}
	total := 0
	for range defaultDelayCatchupSlots {
		if cursor > current || total >= batch {
			break
		}
		published, complete, err := t.dispatchDelaySlot(ctx, topo, kv, cursor, now, batch-total)
		total += published
		if err != nil {
			t.recordSchedulerError(topo, natsSchedulerOperation(err), err, now)
			return total, err
		}
		if !complete || cursor == current {
			break
		}
		advanced, nextEntry, err := advanceDelayCursor(kv, entry, cursor+1)
		if err != nil {
			t.recordSchedulerError(topo, "commit", err, now)
			return total, err
		}
		entry = nextEntry
		if advanced {
			cursor++
			continue
		}
		cursor, err = strconv.ParseInt(string(entry.Value()), 10, 64)
		if err != nil {
			t.recordSchedulerError(topo, "commit", err, now)
			return total, fmt.Errorf("nats delay parse concurrent cursor: %w", err)
		}
	}
	t.recordSchedulerSuccess(topo, total, now)
	return total, nil
}

// recoverDelayCursor skips arbitrarily long empty downtime without probing one
// second at a time. The watcher streams only metadata and has bounded buffering.
// New schedules cannot be inserted behind current because PublishAt accepts only
// future run times, so advancing to the oldest live slot (or current) is safe.
func recoverDelayCursor(ctx context.Context, kv gonats.KeyValue, entry gonats.KeyValueEntry, cursor, current int64) (gonats.KeyValueEntry, int64, error) {
	watcher, err := kv.Watch("v2.>", gonats.IgnoreDeletes(), gonats.MetaOnly(), gonats.Context(ctx))
	if err != nil {
		return nil, cursor, fmt.Errorf("nats delay recover cursor: %w", err)
	}
	defer watcher.Stop()
	target := current
	errorsCh := watcher.Error()
	for {
		select {
		case <-ctx.Done():
			return nil, cursor, ctx.Err()
		case watchErr, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if watchErr != nil {
				return nil, cursor, fmt.Errorf("nats delay recover cursor: %w", watchErr)
			}
		case scheduled, ok := <-watcher.Updates():
			if !ok {
				return nil, cursor, fmt.Errorf("nats delay recovery watcher closed before initial snapshot completed")
			}
			if scheduled == nil {
				if target <= cursor {
					return entry, cursor, nil
				}
				advanced, latest, err := advanceDelayCursor(kv, entry, target)
				if err != nil {
					return nil, cursor, err
				}
				if !advanced {
					target, err = strconv.ParseInt(string(latest.Value()), 10, 64)
					if err != nil {
						return nil, cursor, fmt.Errorf("nats delay parse recovered cursor: %w", err)
					}
				}
				return latest, target, nil
			}
			parts := strings.Split(scheduled.Key(), ".")
			if len(parts) != 4 {
				return nil, cursor, fmt.Errorf("nats delay invalid v2 key %q", scheduled.Key())
			}
			slot, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, cursor, fmt.Errorf("nats delay invalid v2 key %q: %w", scheduled.Key(), err)
			}
			if slot >= cursor && slot < target {
				target = slot
			}
		}
	}
}

func advanceDelayCursor(kv gonats.KeyValue, current gonats.KeyValueEntry, next int64) (bool, gonats.KeyValueEntry, error) {
	value := []byte(strconv.FormatInt(next, 10))
	revision, err := kv.Update(delayCursorKey, value, current.Revision())
	if err == nil {
		return true, &delayKVEntry{KeyValueEntry: current, value: value, revision: revision}, nil
	}
	if !errors.Is(err, gonats.ErrKeyExists) {
		return false, nil, fmt.Errorf("nats delay advance cursor: %w", err)
	}
	latest, err := kv.Get(delayCursorKey)
	if err != nil {
		return false, nil, fmt.Errorf("nats delay refresh cursor: %w", err)
	}
	return false, latest, nil
}

// delayKVEntry overlays values returned by a successful CAS without another
// broker round trip. Only Value and Revision are used by cursor advancement.
type delayKVEntry struct {
	gonats.KeyValueEntry
	value    []byte
	revision uint64
}

func (e *delayKVEntry) Value() []byte    { return e.value }
func (e *delayKVEntry) Revision() uint64 { return e.revision }

func (t *natsTransport) dispatchDelaySlot(ctx context.Context, topo transport.Topology, kv gonats.KeyValue, minute int64, now time.Time, limit int) (int, bool, error) {
	watcher, err := kv.Watch(delaySlotPattern(minute), gonats.IgnoreDeletes(), gonats.Context(ctx))
	if err != nil {
		return 0, false, fmt.Errorf("nats delay watch slot: %w", err)
	}
	defer watcher.Stop()
	published := 0
	pending := false
	errorsCh := watcher.Error()
	for {
		select {
		case <-ctx.Done():
			return published, false, ctx.Err()
		case watchErr, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if watchErr != nil {
				return published, false, fmt.Errorf("nats delay watch slot: %w", watchErr)
			}
		case entry, ok := <-watcher.Updates():
			if !ok {
				return published, false, fmt.Errorf("nats delay watcher closed before initial snapshot completed")
			}
			if entry == nil {
				return published, !pending, nil
			}
			if published >= limit {
				return published, false, nil
			}
			didPublish, remains, err := t.dispatchDelayEntry(ctx, topo, kv, entry, now)
			if err != nil {
				return published, false, err
			}
			if didPublish {
				published++
			}
			pending = pending || remains
		}
	}
}

func (t *natsTransport) dispatchDelayEntry(ctx context.Context, topo transport.Topology, kv gonats.KeyValue, entry gonats.KeyValueEntry, now time.Time) (bool, bool, error) {
	var record delayRecord
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return false, true, fmt.Errorf("nats delay decode %q: %w", entry.Key(), err)
	}
	if (record.Version != delayRecordVersion && record.Version != legacyDelayRecordVersion) || record.ScheduleID == "" || record.RunAt <= 0 {
		return false, true, fmt.Errorf("nats delay invalid record %q", entry.Key())
	}
	if topo.Kind == transport.EventTopology && record.TransferID != "" && record.Target == "" {
		if err := quarantineDelayRecord(kv, entry, record, "missing subscriber target", now); err != nil {
			return false, true, err
		}
		return false, false, nil
	}
	if record.RunAt > now.UnixMilli() {
		return false, true, nil
	}
	if record.LeaseUntil > now.UnixMilli() {
		return false, true, nil
	}
	record.Owner = t.instanceID
	record.LeaseUntil = now.Add(delayLease(topo)).UnixMilli()
	claimed, err := json.Marshal(record)
	if err != nil {
		return false, true, fmt.Errorf("nats delay encode claim: %w", err)
	}
	claimRevision, err := kv.Update(entry.Key(), claimed, entry.Revision())
	if errors.Is(err, gonats.ErrKeyExists) {
		return false, true, nil
	}
	if err != nil {
		return false, true, fmt.Errorf("nats delay claim %q: %w", entry.Key(), err)
	}
	subject := t.broadcastSubject(topo)
	stream := t.streamName(topo)
	deleted := false
	if record.Target != "" {
		err := t.withSubscriberLane(ctx, topo, record.Target, "dispatch", func(lease *subscriberLaneLeaseHandle, _ subscriberLaneState) error {
			if err := lease.checkOwnership(ctx); err != nil {
				return err
			}
			_, publishErr := t.publishToStream(ctx, t.retryStreamName(topo, record.Target), t.retrySubject(topo, record.Target), record.MessageID, "delay:"+record.ScheduleID, record.Payload)
			if publishErr != nil {
				return publishErr
			}
			if err := lease.checkOwnership(ctx); err != nil {
				return err
			}
			if err := kv.Delete(entry.Key(), gonats.LastRevision(claimRevision)); err != nil {
				return fmt.Errorf("nats delay delete %q after publish: %w", entry.Key(), err)
			}
			deleted = true
			return nil
		})
		if err != nil {
			if !errors.Is(err, errSubscriberLaneUnavailable) && !errors.Is(err, errSubscriberLaneLeaseLost) {
				record.Owner = ""
				record.LeaseUntil = 0
				if released, marshalErr := json.Marshal(record); marshalErr == nil {
					_, _ = kv.Update(entry.Key(), released, claimRevision)
				}
				return false, true, fmt.Errorf("nats delay publish %q: %w", entry.Key(), err)
			}
			if errors.Is(err, errSubscriberLaneLeaseLost) {
				return false, true, fmt.Errorf("nats delay publish %q: %w", entry.Key(), err)
			}
			claimedEntry := &delayKVEntry{KeyValueEntry: entry, value: claimed, revision: claimRevision}
			if quarantineErr := quarantineDelayRecord(kv, claimedEntry, record, err.Error(), now); quarantineErr != nil {
				return false, true, quarantineErr
			}
			return false, false, nil
		}
	} else {
		_, err = t.publishToStream(ctx, stream, subject, record.MessageID, "delay:"+record.ScheduleID, record.Payload)
	}
	if err != nil {
		record.Owner = ""
		record.LeaseUntil = 0
		if released, marshalErr := json.Marshal(record); marshalErr == nil {
			_, _ = kv.Update(entry.Key(), released, claimRevision)
		}
		return false, true, fmt.Errorf("nats delay publish %q: %w", entry.Key(), err)
	}
	if !deleted {
		if err := kv.Delete(entry.Key(), gonats.LastRevision(claimRevision)); err != nil {
			return true, true, fmt.Errorf("nats delay delete %q after publish: %w", entry.Key(), err)
		}
	}
	if record.TransferID != "" {
		// The scheduled record is already durably published and deleted. A marker
		// cleanup failure leaks metadata but cannot lose or repeat the transfer by
		// itself, so it must not turn successful dispatch into failure.
		_ = kv.Delete("transfer." + record.TransferID)
	}
	return true, false, nil
}

type quarantinedDelayRecord struct {
	Version       int         `json:"v"`
	OriginalKey   string      `json:"original_key"`
	QuarantinedAt int64       `json:"quarantined_at"`
	Reason        string      `json:"reason"`
	Record        delayRecord `json:"record"`
}

func quarantineDelayRecord(kv gonats.KeyValue, entry gonats.KeyValueEntry, record delayRecord, reason string, now time.Time) error {
	record.Owner, record.LeaseUntil = "", 0
	quarantine := quarantinedDelayRecord{
		Version: delayRecordVersion, OriginalKey: entry.Key(), QuarantinedAt: now.UnixMilli(), Reason: reason, Record: record,
	}
	data, err := json.Marshal(quarantine)
	if err != nil {
		return fmt.Errorf("nats delay encode quarantine %q: %w", entry.Key(), err)
	}
	key := delayQuarantinePrefix + record.ScheduleID
	if _, err := kv.Create(key, data); err != nil && !errors.Is(err, gonats.ErrKeyExists) {
		return fmt.Errorf("nats delay quarantine %q: %w", entry.Key(), err)
	}
	if err := kv.Delete(entry.Key(), gonats.LastRevision(entry.Revision())); err != nil && !errors.Is(err, gonats.ErrKeyNotFound) && !errors.Is(err, gonats.ErrKeyDeleted) {
		return fmt.Errorf("nats delay delete quarantined %q: %w", entry.Key(), err)
	}
	return nil
}

func (t *natsTransport) recordSchedulerPoll(topo transport.Topology, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topo.Name()
	status := t.schedulerStatuses[key]
	status.Transport, status.Topology, status.Worker = "nats", key, "delayed"
	status.LastPoll = now
	status.Polls++
	t.schedulerStatuses[key] = status
}

func (t *natsTransport) recordSchedulerSuccess(topo transport.Topology, published int, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topo.Name()
	status := t.schedulerStatuses[key]
	status.LastSuccess = now
	status.Dispatches++
	status.Transfers += uint64(published)
	status.Commits += uint64(published)
	status.ConsecutiveErrors = 0
	status.Degraded = false
	t.schedulerStatuses[key] = status
}

func (t *natsTransport) recordSchedulerError(topo transport.Topology, operation string, err error, now time.Time) {
	if errors.Is(err, context.Canceled) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topo.Name()
	status := t.schedulerStatuses[key]
	status.Transport, status.Topology, status.Worker = "nats", key, "delayed"
	status.LastErrorAt = now
	status.LastErrorOperation = operation
	status.LastError = natsSchedulerError(err)
	status.Retries++
	status.ConsecutiveErrors++
	status.Degraded = true
	t.schedulerStatuses[key] = status
}

func (t *natsTransport) SchedulerHealth(topo transport.Topology) []transport.SchedulerStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	status, ok := t.schedulerStatuses[topo.Name()]
	if !ok {
		return nil
	}
	return []transport.SchedulerStatus{status}
}

func natsSchedulerError(err error) string {
	if err == nil {
		return ""
	}
	text := fmt.Sprintf("%T", err)
	if len(text) > transport.MaxSchedulerErrorBytes {
		text = text[:transport.MaxSchedulerErrorBytes]
	}
	return text
}

func natsSchedulerOperation(err error) string {
	text := err.Error()
	if strings.Contains(text, " delay publish ") {
		return "transfer"
	}
	if strings.Contains(text, " delay delete ") || strings.Contains(text, " delay advance cursor") {
		return "commit"
	}
	return "dispatch"
}

// migrateLegacyDelayed transfers the P4.2 timestamp_ID records into v2 keys.
// Create-before-delete makes interruption duplicate-prone rather than lossy.
func (t *natsTransport) migrateLegacyDelayed(ctx context.Context, topo transport.Topology, kv gonats.KeyValue, now time.Time, limit int) (int, error) {
	watcher, err := kv.Watch("*", gonats.IgnoreDeletes(), gonats.Context(ctx))
	if err != nil {
		return 0, fmt.Errorf("nats delay watch legacy records: %w", err)
	}
	defer watcher.Stop()
	migrated := 0
	errorsCh := watcher.Error()
	for {
		select {
		case <-ctx.Done():
			return migrated, ctx.Err()
		case watchErr, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if watchErr != nil {
				return migrated, fmt.Errorf("nats delay watch legacy records: %w", watchErr)
			}
		case entry, ok := <-watcher.Updates():
			if !ok {
				return migrated, fmt.Errorf("nats delay legacy watcher closed before initial snapshot completed")
			}
			if entry == nil || migrated >= limit {
				return migrated, nil
			}
			parts := strings.SplitN(entry.Key(), "_", 2)
			if len(parts) != 2 {
				continue
			}
			runAt, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return migrated, fmt.Errorf("nats delay invalid legacy key %q: %w", entry.Key(), err)
			}
			hash := sha256.Sum256([]byte(entry.Key()))
			scheduleID := fmt.Sprintf("legacy-%x", hash[:])
			record := delayRecord{
				Version: delayRecordVersion, ScheduleID: scheduleID, MessageID: parts[1],
				RunAt: runAt, Payload: entry.Value(),
			}
			if topo.Kind == transport.EventTopology {
				if err := quarantineDelayRecord(kv, entry, record, "legacy event delay has unknown subscriber provenance", now); err != nil {
					return migrated, err
				}
				migrated++
				continue
			}
			minute := delaySlot(time.UnixMilli(runAt))
			if runAt <= now.UnixMilli() {
				minute = delaySlot(now)
			}
			data, err := json.Marshal(record)
			if err != nil {
				return migrated, err
			}
			_, err = kv.Create(delayKey(minute, runAt, scheduleID), data)
			if err != nil && !errors.Is(err, gonats.ErrKeyExists) {
				return migrated, fmt.Errorf("nats delay migrate %q: %w", entry.Key(), err)
			}
			if err := kv.Delete(entry.Key(), gonats.LastRevision(entry.Revision())); err != nil && !errors.Is(err, gonats.ErrKeyExists) {
				return migrated, fmt.Errorf("nats delay delete legacy %q: %w", entry.Key(), err)
			}
			migrated++
		}
	}
}
