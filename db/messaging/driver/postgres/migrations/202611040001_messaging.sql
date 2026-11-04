-- +goose Up
CREATE SCHEMA IF NOT EXISTS {{schema}};
CREATE TABLE {{schema}}.messaging_schema (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version integer NOT NULL
);
INSERT INTO {{schema}}.messaging_schema VALUES (true, 1);
CREATE TABLE {{schema}}.outbox (
    seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE NOT NULL,
    namespace text NOT NULL,
    tenant_id text NOT NULL,
    message_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('task', 'event')),
    type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    payload bytea NOT NULL,
    headers jsonb NOT NULL,
    ordering_key text NOT NULL,
    concurrency_key text NOT NULL,
    fingerprint text NOT NULL,
    immutable_hash text NOT NULL,
    scheduled_at timestamptz,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'processing', 'published', 'dead')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    claim_token text,
    lease_until timestamptz,
    published_at timestamptz,
    failed_at timestamptz,
    error_code text NOT NULL DEFAULT '',
    replay_count integer NOT NULL DEFAULT 0,
    replayed_at timestamptz,
    PRIMARY KEY (namespace, tenant_id, message_id),
    CHECK ((state = 'processing' AND claim_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (state <> 'processing' AND claim_token IS NULL AND lease_until IS NULL))
);
CREATE INDEX outbox_ready ON {{schema}}.outbox (namespace, available_at, seq) WHERE state = 'pending';
CREATE INDEX outbox_expired ON {{schema}}.outbox (namespace, lease_until, seq) WHERE state = 'processing';
CREATE INDEX outbox_terminal ON {{schema}}.outbox (namespace, state, seq);
CREATE INDEX outbox_published_cleanup ON {{schema}}.outbox (namespace, published_at, seq) WHERE state = 'published';
CREATE TABLE {{schema}}.inbox (
    namespace text NOT NULL,
    consumer text NOT NULL,
    tenant_id text NOT NULL,
    message_id text NOT NULL,
    fingerprint text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace, consumer, tenant_id, message_id)
);
CREATE INDEX inbox_cleanup ON {{schema}}.inbox (namespace, processed_at);

-- +goose Down
DROP TABLE {{schema}}.inbox;
DROP TABLE {{schema}}.outbox;
DROP TABLE {{schema}}.messaging_schema;
