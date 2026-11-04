# Transactional messaging examples

Run a complete producer → outbox dispatcher → EventBus → inbox consumer flow
using PostgreSQL, MySQL or MongoDB with Redis Streams, NATS JetStream or Kafka. The
executable creates uniquely named database fixtures and broker namespaces. Use
only disposable databases and brokers.

## Configure once, then run

Edit [`config.yaml`](config.yaml) with your disposable PostgreSQL/MySQL/MongoDB and
Redis/Kafka endpoints and credentials, then run from the repository root:

```bash
go run -race ./examples/messaging
```

The sample selects PostgreSQL + embedded NATS. PostgreSQL must already be running
and the configured database (`messaging_test` in the sample) must exist. MongoDB
must be a running replica set; match `replicaSet` in its URI to your deployment.
The YAML file configures connections; it does not start database/Redis/Kafka
services. NATS starts automatically as an owned embedded JetStream server.

### MongoDB standalone and connection failures

If the command fails at `mongodb.connect`, check that `mongodb.uri` points to a
replica set and that its `replicaSet` option matches the server. A local MongoDB
running without `--replSet` cannot run this example's transactions. Removing the
URI's `replicaSet` option does not make standalone MongoDB transaction-capable.

To use a separate disposable single-node replica set without changing an existing
MongoDB service on port 27017:

```bash
docker run -d --name messaging-example-mongo -p 127.0.0.1:27018:27017 \
  mongo:8.0.16 mongod --replSet rs0 --bind_ip_all
# Wait until mongosh can connect, then initialize this new instance once:
docker exec messaging-example-mongo mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
```

Set `mongodb.uri` in your YAML to
`mongodb://127.0.0.1:27018/?replicaSet=rs0&directConnection=true`, wait for the
primary election, and rerun the example. The application user needs permission to
create and drop its temporary databases, collections, validators and indexes.
Remove only this disposable server when finished:

```bash
docker rm -f -v messaging-example-mongo
```

Failures at `redis.connect` indicate a Redis address, authentication, selected
database or connectivity problem. Step diagnostics omit raw driver errors and
credentials. Existing environment overrides still take precedence over YAML;
check them when edits to the file appear to have no effect.

### Select a combination

Change `driver` and `broker` in YAML, or override them per run:

```bash
go run -race ./examples/messaging -driver mongodb
go run -race ./examples/messaging -driver mysql -broker redis
go run -race ./examples/messaging -driver mongodb -broker all
go run -race ./examples/messaging -driver all -broker all
```

| Flag | Values | Default |
|---|---|---|
| `-config` | YAML path; empty string for environment only | `config.yaml` in the working directory, otherwise `examples/messaging/config.yaml` |
| `-driver` | `postgres`, `mysql`, `mongodb`, `all` | YAML `driver`, otherwise `postgres` |
| `-broker` | `redis`, `nats`, `kafka`, `all` | YAML `broker`, otherwise `nats` |

From `examples/messaging`, `go run -race .` uses the same sample file. For a private
configuration, copy it to the ignored `examples/messaging/config.local.yaml`:

```bash
cp examples/messaging/config.yaml examples/messaging/config.local.yaml
# Edit config.local.yaml with your connection settings.
go run -race ./examples/messaging -config examples/messaging/config.local.yaml
```

Use the private copy or environment variables for real credentials; the checked-in
sample contains only dummy local settings. Only selected drivers/brokers require
connection settings. All selections and required fields are checked before fixture
writes begin. Missing files, malformed YAML and invalid selections fail the run.
The CLI never prints DSNs, URIs, passwords or raw decoder/driver errors.

Existing fixture environment variables override their YAML connection fields:

| Environment variable | YAML field |
|---|---|
| `ORM_POSTGRES_DSN` | `postgres.dsn` |
| `ORM_MYSQL_DSN` | `mysql.dsn` |
| `MONGODB_INTEGRATION_URI` | `mongodb.uri` |
| `EVENTBUS_REDIS_ADDR` | `redis.addr` |
| `EVENTBUS_KAFKA_BROKERS` | `kafka.brokers` (comma-separated in env, list in YAML) |

An explicitly empty environment variable clears that field; unset it to use YAML.
Redis also accepts `username`, `password` and `db` in YAML. MongoDB TLS, CA and
authentication settings remain part of its URI. No process environment variables
are changed by the loader. To retain environment-only operation:

```bash
ORM_POSTGRES_DSN='host=127.0.0.1 port=5432 user=postgres dbname=messaging_test sslmode=disable' \
  go run -race ./examples/messaging -config ''
```

## PostgreSQL example

`sql.go` creates a private PostgreSQL schema and migrates with an isolated Goose
metadata search path. The shared flow in `main.go` uses ORM transactions for
order/outbox and inbox/projection writes.

## MySQL example

Select `-driver mysql` and set `mysql.dsn` (or `ORM_MYSQL_DSN`) to an existing
disposable **Oracle MySQL 8.4/InnoDB** database. MySQL 5.7, MariaDB and TiDB are not
supported by the ORM backend. `sql.go` creates a uniquely named database with
InnoDB tables, runs the MySQL embedded migration using a dedicated Goose pool,
and uses the same typed ORM producer/consumer flow as PostgreSQL. The configured
user needs CREATE/DROP and table/index DDL privileges for the temporary database.

```yaml
mysql:
  dsn: "root@tcp(127.0.0.1:3306)/messaging_test"
```

The initial database (`messaging_test` here) must already exist. At completion the
example drops its generated database; it does not delete the configured database.
Outbox/inbox live in `<generated_database>.outbox` and `.inbox` while it runs.
MySQL DDL implicitly commits, so production migrations belong to a deployment job.
See the [MySQL driver guide](../../db/messaging/driver/mysql/README.md).

## MongoDB example

`mongodb.go` registers two database aliases in one `db/mongodb.Manager` using the
same `ClientGroup`:

| Alias | Owned database contents |
|---|---|
| `business` | `orders` and `effects` projections |
| `audit` | Messaging outbox, inbox and schema marker |

The fixture runs `mongomessaging.Provision` in a deployment phase, then opens the
store with `New`. Store configuration uses `Alias: "audit"` and
`TransactionAliases: []string{"business"}`. Producers declare both aliases with
`WithTransactionAcross` and use `mongomessaging.TransactionOptions()` for snapshot
reads, majority writes and primary reads.

The consumer uses `consumer.WrapInbox` with an inferred `*dbmongodb.Transaction`
and transaction repositories on `tx.Context()`. Receipt and projection commit together across the
two databases. IDs are created outside retryable callbacks; no callback publishes
to a broker or invokes an external service.

Every database/broker run verifies:

- A failed producer transaction leaves neither the order nor the outbox record.
- A successful producer commits order and outbox atomically.
- An abandoned claim is recovered after lease expiry.
- A lost broker acceptance response retries the same message identity.
- The main publication has three claims, two publish calls and one committed
  projection; replaying its committed receipt is a valid duplicate.

MongoDB additionally verifies immutable append replay/conflict handling and exact
`int64` payload value `9223372036854775807` through broker delivery and projection.
An independent receipt injects one retryable native transaction error after a DB
write: the callback executes twice, its projection commits once, and another
receipt replay skips the callback. This is a fixture simulation, not an actual
broker outage or primary election. The summary reports the main order's projection
separately from this independent retry receipt's projection.

## Lifecycle and cleanup

The executable drains its dispatcher, closes EventBus/broker clients, drops only
its uniquely named schema/databases and closes database clients. Embedded NATS
storage is removed afterward. Redis/Kafka resources use unique namespaces/topics;
the executable does not delete resources on an externally owned broker. Use
throwaway brokers so stopping their fixture removes those resources.

The native commit/retry budget is configured on the managed MongoDB client. In an
application, align it with handler/shutdown policy, supervise the dispatcher with
the application lifecycle, and run provisioning in a dedicated deployment job.
This executable is a fixture harness.

## Automated verification

Create and remove all database/broker containers automatically:

```bash
bash .github/scripts/test-messaging.sh
# Optionally repeat with the other supported PostgreSQL versions:
MESSAGING_POSTGRES_VERSION=16 bash .github/scripts/test-messaging.sh
MESSAGING_POSTGRES_VERSION=17 bash .github/scripts/test-messaging.sh
```

The script runs the library contract/fault suites, `TestMessagingExample` and
`TestMessagingExampleMongoDB` and `TestMessagingExampleMySQL` with all three brokers under `-race`, then invokes
the real CLI loading the sample YAML with `-driver all -broker all`. Its isolated
fixture environment overrides the sample connection values. The integration tests
also load fixture endpoints from temporary YAML files without environment
overrides. Required fixtures fail if missing; no fallback to an existing local service occurs. PostgreSQL 18 is the default; MySQL 8.4 is fixed to match the ORM support matrix.

MongoDB-only fixture profiles also run this example and CLI with embedded NATS,
plus standalone rejection when that negative fixture is available:

```bash
bash .github/scripts/test-mongodb.sh replica
bash .github/scripts/test-mongodb.sh secure
```

See the [framework guide](../../db/messaging/README.md) and
[MongoDB driver guide](../../db/messaging/driver/mongodb/README.md) for APIs,
retention and native transaction deadline behavior.
