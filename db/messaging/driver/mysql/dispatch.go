package mysql

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"gorm.io/gorm"
)

func (s *Store) Claim(ctx context.Context, req outbox.ClaimRequest) ([]outbox.Claim, error) {
	if req.Limit < 1 || req.Limit > 1000 || req.MaxAttempts < 1 || req.MaxAttempts > 1000000 || req.Lease < time.Millisecond || req.Lease > 24*time.Hour {
		return nil, messaging.ErrInvalidConfig
	}
	var claims []outbox.Claim
	err := s.transaction(ctx, func(db *gorm.DB) error {
		var exhausted []int64
		if err := db.Raw("SELECT seq FROM "+s.outbox+` WHERE namespace=? AND attempts>=? AND
 (state='pending' OR (state='processing' AND lease_until<=UTC_TIMESTAMP(6))) ORDER BY seq LIMIT ? FOR UPDATE SKIP LOCKED`, s.cfg.Namespace, req.MaxAttempts, req.Limit).Scan(&exhausted).Error; err != nil {
			return err
		}
		if len(exhausted) > 0 {
			if err := db.Exec("UPDATE "+s.outbox+" SET state='dead',failed_at=UTC_TIMESTAMP(6),error_code='attempts_exhausted',claim_token=NULL,lease_until=NULL WHERE seq IN ?", exhausted).Error; err != nil {
				return err
			}
		}
		// Separate index-ordered scans avoid a filesort over the OR predicate,
		// which can lock more rows than LIMIT and starve concurrent workers.
		var rows []row
		if err := db.Raw("SELECT * FROM "+s.outbox+` FORCE INDEX (outbox_expired) WHERE namespace=? AND state='processing' AND attempts<? AND lease_until<=UTC_TIMESTAMP(6)
 ORDER BY lease_until,seq LIMIT ? FOR UPDATE SKIP LOCKED`, s.cfg.Namespace, req.MaxAttempts, req.Limit).Scan(&rows).Error; err != nil {
			return err
		}
		if remaining := req.Limit - len(rows); remaining > 0 {
			var pending []row
			if err := db.Raw("SELECT * FROM "+s.outbox+` FORCE INDEX (outbox_ready) WHERE namespace=? AND state='pending' AND attempts<? AND available_at<=UTC_TIMESTAMP(6)
 ORDER BY available_at,seq LIMIT ? FOR UPDATE SKIP LOCKED`, s.cfg.Namespace, req.MaxAttempts, remaining).Scan(&pending).Error; err != nil {
				return err
			}
			rows = append(rows, pending...)
		}
		if len(rows) == 0 {
			return nil
		}
		ids := make([]int64, 0, len(rows))
		for _, r := range rows {
			ids = append(ids, r.Seq)
			if err := db.Exec("UPDATE "+s.outbox+" SET state='processing',attempts=attempts+1,claim_token=?,lease_until=TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(6)) WHERE seq=?", uuid.NewString(), req.Lease.Microseconds(), r.Seq).Error; err != nil {
				return err
			}
		}
		// No UPDATE RETURNING in MySQL. Read back while retaining the row locks.
		rows = nil
		if err := db.Raw("SELECT * FROM "+s.outbox+" WHERE seq IN ? ORDER BY available_at,seq", ids).Scan(&rows).Error; err != nil {
			return err
		}
		for _, r := range rows {
			m, e := r.message()
			if e != nil {
				return e
			}
			claims = append(claims, outbox.Claim{Message: m, Token: r.ClaimToken, Attempt: r.Attempts, LeaseUntil: utcWall(r.LeaseUntil)})
		}
		return nil
	})
	// A commit failure may have claimed some rows. Return none, and allow their
	// leases to expire; never publish from an unacknowledged transaction.
	if err != nil {
		return nil, err
	}
	return claims, nil
}
func (s *Store) settle(ctx context.Context, c outbox.Claim, assignment string, args ...any) error {
	if c.Message.Namespace != s.cfg.Namespace || !messaging.ValidIdentity(c.Message.ID, false) || !messaging.ValidIdentity(c.Message.TenantID, true) || c.Token == "" {
		return messaging.ErrInvalidMessage
	}
	return s.transaction(ctx, func(db *gorm.DB) error {
		// UTC_TIMESTAMP is constant at statement start. Acquire the lock separately
		// before evaluating lease expiry so a lock wait cannot authorize a late write.
		var ids []int64
		if err := db.Raw("SELECT seq FROM "+s.outbox+" WHERE namespace=? AND tenant_id=? AND message_id=? FOR UPDATE", s.cfg.Namespace, c.Message.TenantID, c.Message.ID).Scan(&ids).Error; err != nil {
			return err
		}
		if len(ids) != 1 {
			return messaging.ErrLeaseLost
		}
		values := append(append([]any(nil), args...), ids[0], c.Token)
		r := db.Exec("UPDATE "+s.outbox+" SET "+assignment+",claim_token=NULL,lease_until=NULL WHERE seq=? AND claim_token=? AND state='processing' AND lease_until>UTC_TIMESTAMP(6)", values...)
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return messaging.ErrLeaseLost
		}
		return nil
	})
}
func (s *Store) MarkPublished(ctx context.Context, c outbox.Claim) error {
	return s.settle(ctx, c, "state='published',published_at=UTC_TIMESTAMP(6),error_code=''")
}
func validCode(code string) bool {
	if code == "" || len(code) > 64 {
		return false
	}
	for _, r := range code {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') {
			return false
		}
	}
	return true
}
func (s *Store) Retry(ctx context.Context, c outbox.Claim, delay time.Duration, code string) error {
	if delay < 0 || delay > 24*time.Hour || !validCode(code) {
		return messaging.ErrInvalidConfig
	}
	return s.settle(ctx, c, "state='pending',available_at=TIMESTAMPADD(MICROSECOND,?,UTC_TIMESTAMP(6)),error_code=?", delay.Microseconds(), code)
}
func (s *Store) Quarantine(ctx context.Context, c outbox.Claim, code string) error {
	if !validCode(code) {
		return messaging.ErrInvalidConfig
	}
	return s.settle(ctx, c, "state='dead',failed_at=UTC_TIMESTAMP(6),error_code=?", code)
}
