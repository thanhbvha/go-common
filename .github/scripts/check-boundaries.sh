#!/usr/bin/env bash
set -euo pipefail
if go list -deps ./health ./foundation ./cron ./db/messaging ./db/messaging/outbox ./db/messaging/inbox | grep -E '^(gorm.io/|github.com/redis/|github.com/dgraph-io/|github.com/jackc/|go.mongodb.org/|github.com/thanhbvha/go-common/v2/eventbus)'; then
 echo 'Core package imports a datastore integration; move it to an adapter subpackage.' >&2
 exit 1
fi
