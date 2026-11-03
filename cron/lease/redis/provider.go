// Package redis supplies Redis ownership sessions for the cron scheduler.
// The client is borrowed and must outlive scheduler shutdown.
package redis

import (
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/cache"
	"github.com/thanhbvha/go-common/v2/cron"
)

type Provider struct{ client redis.UniversalClient }

func New(client redis.UniversalClient) *Provider { return &Provider{client: client} }
func (p *Provider) NewSession() (cron.Lease, error) {
	if p == nil {
		var lock *cache.RedisLock
		return lock.NewSession()
	}
	return cache.NewRedisLock(p.client)
}

var _ cron.LeaseProvider = (*Provider)(nil)
