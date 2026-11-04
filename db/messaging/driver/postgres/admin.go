package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"gorm.io/gorm"
)

func validTenant(tenant *string) bool { return tenant == nil || messaging.ValidIdentity(*tenant, true) }
func validLimit(limit int) bool       { return limit >= 1 && limit <= 1000 }

func (s *Store) Get(ctx context.Context, tenant, id string) (record outbox.Record, err error) {
	if !messaging.ValidIdentity(tenant, true) || !messaging.ValidIdentity(id, false) {
		return record, messaging.ErrInvalidMessage
	}
	err = s.transaction(ctx, func(db *gorm.DB) error {
		var rows []row
		if e := db.Raw("SELECT * FROM "+s.outbox+" WHERE namespace=? AND tenant_id=? AND message_id=?", s.cfg.Namespace, tenant, id).Scan(&rows).Error; e != nil {
			return e
		}
		if len(rows) != 1 {
			return messaging.ErrNotFound
		}
		var e error
		record, e = rows[0].record()
		return e
	})
	if err != nil {
		return outbox.Record{}, err
	}
	return record, nil
}

// Cursors include their filter, preventing accidental reuse across tenant,
// namespace or state. A cursor is navigation, never an authorization token.
type cursor struct {
	V         int
	Namespace string
	Tenant    *string
	State     outbox.State
	Seq       int64
}

func (s *Store) List(ctx context.Context, req outbox.ListRequest) (page outbox.Page, err error) {
	if !validTenant(req.TenantID) || !validLimit(req.Limit) || (req.State != "" && req.State != outbox.Pending && req.State != outbox.Processing && req.State != outbox.Published && req.State != outbox.Dead) {
		return page, messaging.ErrInvalidConfig
	}
	var after int64
	if req.Cursor != "" {
		if len(req.Cursor) > 2048 {
			return page, messaging.ErrInvalidConfig
		}
		b, e := base64.RawURLEncoding.DecodeString(req.Cursor)
		if e != nil {
			return page, messaging.ErrInvalidConfig
		}
		var c cursor
		if json.Unmarshal(b, &c) != nil || c.V != 1 || c.Namespace != s.cfg.Namespace || c.State != req.State || c.Seq < 0 ||
			(c.Tenant == nil) != (req.TenantID == nil) || (c.Tenant != nil && *c.Tenant != *req.TenantID) {
			return page, messaging.ErrInvalidConfig
		}
		after = c.Seq
	}
	err = s.transaction(ctx, func(db *gorm.DB) error {
		q := "SELECT * FROM " + s.outbox + " WHERE namespace=? AND seq>?"
		args := []any{s.cfg.Namespace, after}
		if req.TenantID != nil {
			q += " AND tenant_id=?"
			args = append(args, *req.TenantID)
		}
		if req.State != "" {
			q += " AND state=?"
			args = append(args, req.State)
		}
		q += " ORDER BY seq LIMIT ?"
		args = append(args, req.Limit+1)
		var rows []row
		if e := db.Raw(q, args...).Scan(&rows).Error; e != nil {
			return e
		}
		hasNext := len(rows) > req.Limit
		if hasNext {
			rows = rows[:req.Limit]
		}
		for _, r := range rows {
			record, e := r.record()
			if e != nil {
				return e
			}
			page.Records = append(page.Records, record)
		}
		if hasNext {
			b, _ := json.Marshal(cursor{1, s.cfg.Namespace, req.TenantID, req.State, rows[len(rows)-1].Seq})
			page.NextCursor = base64.RawURLEncoding.EncodeToString(b)
		}
		return nil
	})
	if err != nil {
		return outbox.Page{}, err
	}
	return page, nil
}

// Replay only resets quarantined publications, not successful messages or inbox
// receipts. Keeping IDs prevents an already applied delivery from running again.
func (s *Store) Replay(ctx context.Context, tenant string, ids []string) (count int64, err error) {
	if !messaging.ValidIdentity(tenant, true) || !validLimit(len(ids)) {
		return 0, messaging.ErrInvalidConfig
	}
	for _, id := range ids {
		if !messaging.ValidIdentity(id, false) {
			return 0, messaging.ErrInvalidMessage
		}
	}
	err = s.transaction(ctx, func(db *gorm.DB) error {
		r := db.Exec("UPDATE "+s.outbox+` SET state='pending',attempts=0,available_at=clock_timestamp(),failed_at=NULL,error_code='',
 replay_count=replay_count+1,replayed_at=clock_timestamp() WHERE namespace=? AND tenant_id=? AND state='dead' AND message_id IN ?`, s.cfg.Namespace, tenant, ids)
		count = r.RowsAffected
		return r.Error
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Cleanup is explicit and bounded per table. The application must choose an
// inbox cutoff older than every supported broker replay / DLQ recovery window.
func (s *Store) Cleanup(ctx context.Context, req outbox.CleanupRequest) (result outbox.CleanupResult, err error) {
	if !validTenant(req.TenantID) || !validLimit(req.Limit) || (req.PublishedBefore.IsZero() && req.InboxBefore.IsZero()) ||
		(!req.PublishedBefore.IsZero() && req.PublishedBefore.After(time.Now())) || (!req.InboxBefore.IsZero() && req.InboxBefore.After(time.Now())) {
		return result, messaging.ErrInvalidConfig
	}
	err = s.transaction(ctx, func(db *gorm.DB) error {
		if !req.PublishedBefore.IsZero() {
			q := "WITH victims AS (SELECT seq FROM " + s.outbox + " WHERE namespace=? AND state='published' AND published_at<?"
			args := []any{s.cfg.Namespace, req.PublishedBefore}
			if req.TenantID != nil {
				q += " AND tenant_id=?"
				args = append(args, *req.TenantID)
			}
			q += " ORDER BY published_at,seq LIMIT ? FOR UPDATE SKIP LOCKED) DELETE FROM " + s.outbox + " o USING victims v WHERE o.seq=v.seq"
			args = append(args, req.Limit)
			r := db.Exec(q, args...)
			if r.Error != nil {
				return r.Error
			}
			result.Published = r.RowsAffected
		}
		if !req.InboxBefore.IsZero() {
			q := "WITH victims AS (SELECT namespace,consumer,tenant_id,message_id FROM " + s.inbox + " WHERE namespace=? AND processed_at<?"
			args := []any{s.cfg.Namespace, req.InboxBefore}
			if req.TenantID != nil {
				q += " AND tenant_id=?"
				args = append(args, *req.TenantID)
			}
			q += " ORDER BY processed_at,consumer,tenant_id,message_id LIMIT ? FOR UPDATE SKIP LOCKED) DELETE FROM " + s.inbox + " i USING victims v WHERE (i.namespace,i.consumer,i.tenant_id,i.message_id)=(v.namespace,v.consumer,v.tenant_id,v.message_id)"
			args = append(args, req.Limit)
			r := db.Exec(q, args...)
			if r.Error != nil {
				return r.Error
			}
			result.Inbox = r.RowsAffected
		}
		return nil
	})
	if err != nil {
		return outbox.CleanupResult{}, err
	}
	return result, nil
}
func (s *Store) Stats(ctx context.Context, tenant *string) (stats messaging.Stats, err error) {
	if !validTenant(tenant) {
		return stats, messaging.ErrInvalidConfig
	}
	err = s.transaction(ctx, func(db *gorm.DB) error {
		q := `SELECT count(*) FILTER (WHERE state='pending') AS pending,
 count(*) FILTER (WHERE state='processing') AS in_flight, count(*) FILTER (WHERE state='dead') AS dead,
 COALESCE(GREATEST(0,EXTRACT(EPOCH FROM clock_timestamp()-min(created_at) FILTER (WHERE state='pending'))),0) AS age
 FROM ` + s.outbox + " WHERE namespace=?"
		args := []any{s.cfg.Namespace}
		if tenant != nil {
			q += " AND tenant_id=?"
			args = append(args, *tenant)
		}
		var r struct {
			Pending, InFlight, Dead int64
			Age                     float64
		}
		if e := db.Raw(q, args...).Scan(&r).Error; e != nil {
			return e
		}
		stats = messaging.Stats{Pending: r.Pending, InFlight: r.InFlight, Dead: r.Dead, OldestPendingAge: time.Duration(r.Age * float64(time.Second))}
		return nil
	})
	if err != nil {
		return messaging.Stats{}, err
	}
	messaging.Observe(ctx, s.cfg.Observer, messaging.Observation{Operation: "outbox_stats", Outcome: "success", Stats: &stats})
	return stats, nil
}

var _ outbox.Admin = (*Store)(nil)
