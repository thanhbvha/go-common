-- +goose NO TRANSACTION
-- +goose Up
-- DDL implicitly commits on MySQL. Run in one deployment job, never startup.
CREATE TABLE {{database}}.messaging_schema (
    singleton boolean PRIMARY KEY CHECK (singleton = true),
    version integer NOT NULL
) ENGINE=InnoDB;
CREATE TABLE {{database}}.outbox (
    seq bigint NOT NULL AUTO_INCREMENT,
    namespace varbinary(256) NOT NULL,
    tenant_id varbinary(256) NOT NULL,
    message_id varbinary(256) NOT NULL,
    kind varchar(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL CHECK (kind IN ('task','event')),
    type varbinary(256) NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    payload mediumblob NOT NULL,
    headers json NOT NULL,
    ordering_key varbinary(256) NOT NULL,
    concurrency_key varbinary(256) NOT NULL,
    fingerprint char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    immutable_hash char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    scheduled_at varbinary(32),
    available_at datetime(6) NOT NULL,
    created_at datetime(6) NOT NULL,
    state varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','processing','published','dead')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    claim_token varchar(36) CHARACTER SET ascii COLLATE ascii_bin,
    lease_until datetime(6),
    published_at datetime(6),
    failed_at datetime(6),
    error_code varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    replay_count integer NOT NULL DEFAULT 0,
    replayed_at datetime(6),
    PRIMARY KEY (namespace,tenant_id,message_id),
    UNIQUE KEY outbox_seq (seq),
    KEY outbox_ready (namespace,state,available_at,seq),
    KEY outbox_expired (namespace,state,lease_until,seq),
    KEY outbox_terminal (namespace,state,seq),
    KEY outbox_published_cleanup (namespace,state,published_at,seq),
    CHECK ((state='processing' AND claim_token IS NOT NULL AND lease_until IS NOT NULL)
        OR (state<>'processing' AND claim_token IS NULL AND lease_until IS NULL))
) ENGINE=InnoDB;
CREATE TABLE {{database}}.inbox (
    seq bigint NOT NULL AUTO_INCREMENT,
    namespace varbinary(256) NOT NULL,
    consumer varbinary(256) NOT NULL,
    tenant_id varbinary(256) NOT NULL,
    message_id varbinary(256) NOT NULL,
    fingerprint char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    insert_token char(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    processed_at datetime(6) NOT NULL,
    PRIMARY KEY (namespace,consumer,tenant_id,message_id),
    UNIQUE KEY inbox_seq (seq),
    KEY inbox_cleanup (namespace,processed_at,seq)
) ENGINE=InnoDB;
-- Publish the version only after all DDL succeeds.
INSERT INTO {{database}}.messaging_schema VALUES (true,1);

-- +goose Down
DROP TABLE {{database}}.inbox;
DROP TABLE {{database}}.outbox;
DROP TABLE {{database}}.messaging_schema;
