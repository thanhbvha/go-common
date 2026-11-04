// Package inbox defines transactional duplicate protection for consumer effects.
package inbox

import (
	"encoding/hex"

	"github.com/thanhbvha/go-common/v2/db/messaging"
)

type Receipt struct {
	messaging.Identity
	Consumer    string
	Fingerprint string
}

func (r Receipt) Validate() error {
	if !messaging.ValidIdentity(r.Namespace, false) || !messaging.ValidIdentity(r.TenantID, true) || !messaging.ValidIdentity(r.ID, false) || !messaging.ValidIdentity(r.Consumer, false) || len(r.Fingerprint) != 64 {
		return messaging.ErrInvalidMessage
	}
	if _, err := hex.DecodeString(r.Fingerprint); err != nil {
		return messaging.ErrInvalidMessage
	}
	return nil
}

type Result string

const (
	Applied   Result = "applied"
	Duplicate Result = "duplicate"
)
