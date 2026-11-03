package main

import (
	"context"
	"crypto/subtle"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/core"
)

type credential struct {
	token    string
	identity core.Identity
}

func environmentCredentials(expiry time.Time) []credential {
	var credentials []credential
	for _, p := range []struct{ environment, tenant, user, session string }{
		{"WS_V2_ALICE_TOKEN", "demo", "alice", "desktop"},
		{"WS_V2_ALICE_PHONE_TOKEN", "demo", "alice", "phone"},
		{"WS_V2_BOB_TOKEN", "demo", "bob", "desktop"},
		{"WS_V2_OTHER_TOKEN", "other", "alice", "desktop"},
	} {
		if token := os.Getenv(p.environment); token != "" {
			credentials = append(credentials, credential{token, core.Identity{TenantID: p.tenant, UserID: p.user, AccountID: p.tenant + "-members", SessionID: p.session, ExpiresAt: expiry}})
		}
	}
	return credentials
}

func matches(header, token string) bool {
	return token != "" && subtle.ConstantTimeCompare([]byte(header), []byte("Bearer "+token)) == 1
}

func (a *application) authenticate(r *http.Request) (core.Identity, error) {
	for _, c := range a.settings.credentials {
		if matches(r.Header.Get("Authorization"), c.token) && time.Now().Before(c.identity.ExpiresAt) {
			return c.identity, nil
		}
	}
	return core.Identity{}, core.ErrUnauthorized
}

type policy struct{ stopping *atomic.Bool }

func (p policy) CanPublish(_ context.Context, i core.Identity, event string) error {
	if p.stopping != nil && p.stopping.Load() {
		return core.ErrUnauthorized
	}
	if i.TenantID == "" || i.UserID == "" {
		return core.ErrUnauthorized
	}
	switch event {
	case "session.info", "system.ping", "room.join", "room.leave", "chat.send", "notify.user", "notify.session", "business.echo":
		return nil
	default:
		return core.ErrUnauthorized
	}
}
func (policy) CanSubscribe(_ context.Context, i core.Identity, room string) error {
	// This demo grants one lobby per authenticated tenant. Real deployments
	// should query their membership/entitlement service here.
	if i.TenantID == "" || i.UserID == "" || room != i.TenantID+":lobby" || strings.TrimSpace(room) != room {
		return core.ErrUnauthorized
	}
	return nil
}
