// Package eventbuswire shares the stable protocol between EventBus adapters.
package eventbuswire

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/thanhbvha/go-common/v2/db/messaging"
)

const (
	HeaderNamespace     = "transactional.namespace"
	HeaderTenant        = "transactional.tenant"
	HeaderSchemaVersion = "transactional.schema_version"
	HeaderMessageID     = "transactional.message_id"
)

// WireID maps the business identity to a stable broker-safe ID, including tenant
// and topology scope. Use HeaderMessageID for the original business message ID.
func WireID(m messaging.Message) string {
	b, _ := json.Marshal([]string{m.Namespace, m.TenantID, string(m.Kind), m.Type, m.ID})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
