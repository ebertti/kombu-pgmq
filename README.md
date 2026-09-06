# kombu-pgmq

[![CI](https://github.com/ebertti/kombu-pgmq/actions/workflows/ci.yml/badge.svg)](https://github.com/ebertti/kombu-pgmq/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/kombu-pgmq.svg)](https://pypi.org/project/kombu-pgmq/)

Kombu transport for PGMQ — use PostgreSQL as the Celery broker, without Redis or RabbitMQ.

## Installation

`kombu-pgmq` ships two Transport classes, each talking to PGMQ a different way. Neither is installed by default — pick the one you need (or both):

```bash
pip install kombu-pgmq[pgmq]     # TransportPGMQ (default): the official `pgmq` client
pip install kombu-pgmq[psycopg]  # TransportPsycopg: raw SQL over psycopg 3
pip install kombu-pgmq[all]      # installs both
```

Both ultimately run on `psycopg` 3 — `kombu-pgmq[pgmq]` already installs it too, since the official `pgmq` client depends on it directly. So `psycopg` itself isn't the differentiator; the choice is really about the `pgmq` package (the official client) and its extra dependencies (`pgmq` itself, `orjson`, `psycopg_pool`):

**Which one to use:**
- **`TransportPGMQ`** (recommended default) — the official client. Less to think about: it already knows how to pool connections and tracks PGMQ's SQL API upstream. Costs two extra dependencies (`pgmq`, `orjson`) and couples you to that library's own API surface and quirks (e.g. it currently emits a deprecation warning on `list_queues()`).
- **`TransportPsycopg`** — raw SQL calls over `psycopg` 3 directly, no `pgmq`/`orjson` dependency. Smaller footprint, full visibility/control over the exact SQL executed, no exposure to the official client's own release cycle or behavior changes. Pick this if any of that matters more to you than the convenience of the official client.

`kombu-pgmq[all]` installs both — useful if you want to pick the backend at runtime (via `transport_options`) without reinstalling, which is exactly why this project's own test suite uses it.

## PostgreSQL setup

`kombu-pgmq` requires the [`pgmq` extension](https://github.com/tembo-io/pgmq) to be installed in your PostgreSQL database. It does not install or enable the extension automatically — this must be done once by a superuser:

```sql
CREATE EXTENSION IF NOT EXISTS pgmq;
```

If the extension is not present when the first connection is made, `kombu-pgmq` raises a `RuntimeError` with a clear message. To disable this check (e.g. if the extension is managed externally or your provider blocks `pg_extension` reads), set:

```python
app.conf.broker_transport_options = {
    "check_extension": False,
}
```

### Provider compatibility

The `pgmq` extension is not bundled with standard PostgreSQL — it must be installed separately, and not all managed providers allow it.

| Provider                            | PGMQ | Notes                                                                                                                           |
|-------------------------------------|:----:|---------------------------------------------------------------------------------------------------------------------------------|
| **Self-hosted / Docker**            |  ✅  | Use the [`ghcr.io/pgmq/pg18-pgmq:v1.10.0`](ghcr.io/pgmq/pg18-pgmq:v1.10.0) image (this repo's `docker-compose.yml`)             |
| **Supabase**                        |  ✅  | Available — run `CREATE EXTENSION pgmq;` once                                                                                   |
| **Neon**                            |  ✅  | Available — run `CREATE EXTENSION pgmq;` once                                                                                   |
| **Tembo Cloud**                     |  ✅  | Available — run `CREATE EXTENSION pgmq;` or enable via the dashboard                                                            |
| **AWS RDS**                         |  ❌  | Not in the [RDS supported extensions list](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html)         |
| **AWS Aurora**                      |  ❌  | Same as RDS                                                                                                                     |
| **Google Cloud SQL**                |  ❌  | Not in the [Cloud SQL extensions list](https://cloud.google.com/sql/docs/postgres/extensions)                                   |
| **Azure Database for PostgreSQL**   |  ❌  | Not in the [Azure supported extensions](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-extensions) |
| **DigitalOcean Managed PostgreSQL** |  ❌  | Not in the managed extension list                                                                                               |
| **Aiven**                           |  ❌  | Not in the [Aiven extension list](https://aiven.io/docs/products/postgresql/reference/list-of-extensions)                       |
| **Render Managed PostgreSQL**       |  ❌  | Custom extensions not supported                                                                                                 |

> If your provider is not supported, options are: (1) run your own PostgreSQL on a VM/container with the extension installed, or (2) use Supabase or Neon as a compatible managed alternative.

### Self-hosted / Docker

The quickest way to get a PGMQ-enabled Postgres running locally is with the pre-built Docker image from Tembo:

```bash
docker run -d \
  --name pgmq \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  temboio/pg16-pgmq:latest
```

Then enable the extension once:

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres \
  -c "CREATE EXTENSION IF NOT EXISTS pgmq;"
```

This repo's `docker-compose.yml` already does both steps — `docker compose up -d` is enough for local development.

## Usage

```python
from celery import Celery

app = Celery(
    "myapp",
    broker_transport="kombu_pgmq.transport:TransportPGMQ",  # or TransportPsycopg
    broker_url="postgresql://user:pass@localhost:5432/mydb",
)

app.conf.broker_transport_options = {
    "visibility_timeout": 60,  # seconds, default 30
    "pool": True,  # see "Connection pooling" below, default varies per transport
}
```

`kombu_pgmq.transport:Transport` is also exported as an alias for `TransportPGMQ`, and the `pgmq://` broker URL scheme (registered on `import kombu_pgmq`) always resolves to `TransportPGMQ` too — use `broker_transport` explicitly to pick `TransportPsycopg`.

### Fanout, topic exchanges, and remote control

Both transports support `direct`, `topic` and `fanout` exchanges, backed by a small bindings table (`kombu_pgmq_bindings`, created automatically on first use) instead of Kombu's default in-memory-only bindings — which is what actually makes this work across separate worker processes, not just within one. `celery inspect`/`celery control` (pidbox) and `celery events` both ride on top of this (they're plain fanout/topic exchanges under the hood) and need no special configuration.

Two things worth knowing:

- PGMQ caps queue names at 47 characters. Celery generates longer auto-named queues for things like pidbox replies (`<uuid>.reply.celery.pidbox`); `kombu-pgmq` shortens any queue name over the limit deterministically (same input always maps to the same shortened PGMQ queue) before it reaches PGMQ — transparent, nothing to configure.
- Unbinding a queue from an exchange without deleting the queue itself (`queue_unbind`) isn't reliable against a persisted bindings table — the same limitation exists in Kombu's own Redis transport. In practice this doesn't come up: pidbox/events/normal usage always bind once and later drop the whole queue, never unbind-in-place.

### Connection pooling

Each transport can either check out a connection from a `psycopg_pool.ConnectionPool` on every call, or hold a single connection open and reuse it. Set it explicitly via `transport_options["pool"]` (`True`/`False`) — both transports default to `pool=True`.

**Single-threaded access** (`pytest tests/test_performance.py -m perf -s`, one `Channel`, sequential calls) consistently showed **no pooling being faster** for both transports — checking a connection in/out of the pool costs more than it saves when nothing else is contending for connections:

| | send | read+ack |
|---|---|---|
| psycopg, no pool | ~1700 msg/s | ~600–680 msg/s |
| psycopg, pool | ~860–1080 msg/s | ~700–880 msg/s |
| pgmq client, no pool | ~1400–2450 msg/s | ~750 msg/s |
| pgmq client, pool | ~930–980 msg/s | ~505–510 msg/s |

`tests/test_performance.py` measures both the raw Channel primitives (`test_channel_primitives_throughput`, skipping Kombu's Producer/Consumer/QoS layer) and the actual Kombu `Channel` (`test_channel_throughput`, i.e. `Producer.publish` / `basic_get` / `ack`) — the numbers above are consistent across both, meaning Kombu itself adds negligible overhead on top of the PGMQ calls.

**Concurrent access** tells a different story. `tests/test_performance_concurrency.py` shares one `Channel` across N threads sending concurrently (`pytest tests/test_performance_concurrency.py -m perf -s`) — with `pool=False` every thread fights over the same underlying connection (psycopg serializes concurrent use of one connection internally, so it's safe, just increasingly congested); with `pool=True` each thread can get its own connection, up to the pool's max size:

| threads | psycopg, no pool | psycopg, pool | pgmq, no pool | pgmq, pool |
|---:|---:|---:|---:|---:|
| 1  | 1166 msg/s | 582 msg/s  | 1661 msg/s | 991 msg/s  |
| 2  | 1090 msg/s | 1506 msg/s | 1613 msg/s | 1755 msg/s |
| 4  | 1074 msg/s | 2561 msg/s | 1788 msg/s | 2443 msg/s |
| 8  | 1665 msg/s | 2547 msg/s | 2079 msg/s | 2542 msg/s |
| 16 | 1390 msg/s | 2439 msg/s | 2445 msg/s | 3044 msg/s |

![pool vs no-pool throughput as concurrent threads increase, for both transports](docs/pool-vs-no-pool.png)

Across repeated runs, **pooling starts winning consistently once ~4 threads share the same `Channel`** — below that, a single connection is still faster (or roughly tied); the more threads pile on beyond that, the bigger the pool's advantage, since a single shared connection caps out while the pool can actually parallelize across its connections.

So: the `pool=True` default is the safer choice if you don't know how many threads will share a `Channel`. If you're certain a `Channel` is only ever used by one thread at a time (the common case — one Celery worker process/thread per connection), set `pool=False` for the small sequential-throughput edge shown in the table above.

## How does it compare to Redis / RabbitMQ?

`tests/test_performance_brokers.py` runs the exact same send / read+ack loop (`Producer.publish` / `Queue.get` / `ack`, N=200, sequential, single connection) against Redis and RabbitMQ using Kombu's own built-in transports, so the numbers are directly comparable to the ones above:

| broker | send | read+ack |
|---|---:|---:|
| kombu-pgmq (pgmq client) | ~700–1000 msg/s | ~400–450 msg/s |
| kombu-pgmq (psycopg) | ~1000–1030 msg/s | ~470–495 msg/s |
| Redis | ~2500–3000 msg/s | ~840–910 msg/s |
| RabbitMQ | ~19000 msg/s | ~2750–2800 msg/s |

RabbitMQ and Redis are purpose-built message brokers (persistent connection, purpose-built wire protocol, in-memory or Erlang-native queue engine) — they're 3–20x faster here, and that's expected. PGMQ's pitch was never raw throughput: it's "no extra infra, your queue lives in the same Postgres you already run and backup, with real transactional guarantees (a task enqueue can commit atomically with the business-logic row that triggered it)". If you need RabbitMQ/Redis-level throughput, use RabbitMQ/Redis; if you want one less moving part in your stack and PGMQ's throughput is enough for your workload, that's what `kombu-pgmq` is for.

Run it yourself: `docker compose up -d` (also starts `redis` and `rabbitmq`, only needed for this file) then `pytest tests/test_performance_brokers.py -m perf -s`.

## Development

Start a local PostgreSQL instance with the PGMQ extension pre-installed:

```bash
docker compose up -d
```

This exposes Postgres on `localhost:5432` (user `postgres`, password `postgres`, db `postgres`) with the `pgmq` extension already created.
