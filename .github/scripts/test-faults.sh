#!/usr/bin/env bash
set -euo pipefail
# Embedded/miniredis fixtures only. Real Kafka and Redis topology drills run
# separately in test-brokers.sh. Repetition exposes uncertain-settlement races.
go test -race -p 1 -tags=integration -count=5 -shuffle=on -v -timeout=10m \
  -run 'Test(PublishResponseLoss|ConnectionFailureBeforePublishAndAck|TransferResponseLoss|AckResponseLoss|NATSAckWaitExpiry|NATSMaxAckPending|NATSDisconnectReconnect|DelaySchedulerRecovers|NATSDestinationFailure|Reference)' \
  ./eventbus/driver/redis ./eventbus/driver/nats ./examples/production
