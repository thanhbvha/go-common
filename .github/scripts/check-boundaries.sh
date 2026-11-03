#!/usr/bin/env bash
set -euo pipefail
if go list -deps ./health ./foundation ./cron | grep -E '^(gorm.io/|github.com/redis/|github.com/dgraph-io/)'; then
 echo 'Core package imports a datastore integration; move it to an adapter subpackage.' >&2
 exit 1
fi
