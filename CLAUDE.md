# kombu-pgmq

Kombu transport for PGMQ — lets you use PostgreSQL (via the PGMQ extension) as the Celery broker, without Redis or RabbitMQ.

## Goal

Implement `kombu.transport.virtual.Transport` + `Channel` mapping Kombu operations to PGMQ:

| Kombu / Celery      | PGMQ                       |
|---------------------|----------------------------|
| publish             | `pgmq.send()`              |
| receive             | `pgmq.read(vt=...)`        |
| ack                 | `pgmq.delete(msg_id)`      |
| reject / redelivery | visibility timeout expires |
| queue               | PGMQ queue                 |

## Project structure

```
kombu-pgmq/
├── CLAUDE.md
├── AGENTS.md
├── pyproject.toml
├── src/
│   └── kombu_pgmq/
│       ├── __init__.py
│       └── transport.py    # Channel (base) + ChannelPGMQ/ChannelPsycopg + TransportPGMQ/TransportPsycopg
└── tests/
    ├── test_transport.py
    ├── test_celery_integration.py
    ├── test_performance.py
    ├── test_performance_concurrency.py
    └── test_performance_brokers.py
```

## MVP scope

Supported in the first release:

- task queues
- routing_key → queue
- ack
- reject
- redelivery via visibility timeout
- configurable visibility_timeout

Out of scope for now (do not implement without an explicit request):

- priority
- fanout / topic exchange
- remote control
- celery events

## Design decisions

- Use `kombu.transport.virtual` as the base — do not reimplement full AMQP.
- **Do not delete the message in `_get()`**: read → keep it invisible via VT → delete only on ACK.
- Store `msg_id` in `properties.delivery_info["pgmq_msg_id"]` for the later ACK — not in the message body, so the task payload the worker sees stays untouched (same pattern `kombu.transport.SQS` uses for the receipt handle).
- Two concrete Transport/Channel classes, not one Transport with a `backend` option: `TransportPGMQ`/`ChannelPGMQ` (default, official `pgmq` client, `pip install kombu-pgmq[pgmq]`) and `TransportPsycopg`/`ChannelPsycopg` (raw SQL, `pip install kombu-pgmq[psycopg]`). Both subclass a shared `Channel` base in `transport.py` that implements all the Kombu-facing plumbing (`_new_queue`, `_get`, `basic_ack`, etc.) in terms of primitives (`_send`, `_read`, `_delete_message`, ...) each subclass implements. No separate `backends/` package/abstraction — deliberately merged into `transport.py` after that indirection stopped pulling its weight. Neither PGMQ lib is a base dependency — each subclass's lazy `cached_property` guards its import and raises a clear `ImportError` naming the extra to install if picked without it. `kombu-pgmq[all]` installs both; `Transport` is exported as an alias for `TransportPGMQ`.
- Broker URL: `pgmq://user:pass@host:5432/db` (via `TRANSPORT_ALIASES`, always resolves to `TransportPGMQ`, requires `import kombu_pgmq` first) or `broker_transport = "kombu_pgmq.transport:TransportPGMQ"` / `"...:TransportPsycopg"` + `broker_url = "postgresql://..."`.
- Both channels accept `pool: bool` (via `transport_options["pool"]`), defaulting to `True` for both. `tests/test_performance.py` (marked `perf`, excluded by default) showed no-pool consistently faster than pooled for both under sequential single-worker access; `tests/test_performance_concurrency.py` (N threads sharing one `Channel`) found pooling starts winning consistently around ~4 concurrent threads for both. Default is `True` (the safer choice when the caller doesn't control how many threads share a `Channel`) — see README's "Connection pooling" section for the numbers and when to flip it to `False`.
- `tests/test_performance_brokers.py` runs the same send/read+ack loop against Redis and RabbitMQ via Kombu's own built-in transports (`docker-compose.yml`'s `redis`/`rabbitmq` services, only needed for this file). Both are 3–20x faster than either kombu-pgmq channel — expected, they're purpose-built brokers. PGMQ's value proposition is "one less moving part, transactional guarantees with the rest of your Postgres data", not raw throughput — see README's "How does it compare to Redis / RabbitMQ?" section.
- `kombu-pgmq[pgmq]` already installs `psycopg` transitively (the official `pgmq` client depends on `psycopg[binary,pool]` itself), so `psycopg` presence isn't the differentiator between the two extras. `[psycopg]` *is* the lighter-weight, more decoupled option though: it skips the `pgmq`/`orjson` dependencies and the official client's own API surface/quirks entirely — `[pgmq]` trades that for built-in pooling and upstream-tracked SQL.
- `requires-python = ">=3.10"` — checked directly against PyPI (`requires_python` on each package's JSON metadata) rather than guessing: `psycopg`/`psycopg-binary`/`psycopg-pool`/`pgmq` all require `>=3.10` (the tightest constraint in the dependency tree; `kombu`/`celery` only need `>=3.9`). Nothing in our own code needs newer than 3.10 either (only `X | None` return annotations, PEP 604, itself a 3.10 feature). Verified empirically too, not just by metadata: ran the full suite against a real Python 3.10 venv (`uv venv --python 3.10`) before trusting it. CI (`ci.yml`) runs the test matrix across 3.10–3.14 on every PR.

## Engineering Principles

### 1. Think before coding
State your assumptions; if there's uncertainty or multiple interpretations, ask/present options — don't choose silently. If a simpler approach exists, say so. If something is unclear, stop and name what's confusing.

### 2. Simplicity first
The minimum code that solves the problem, nothing speculative: no features, abstractions, "flexibility/configurability", or error handling that wasn't requested. If you wrote 200 lines and it could be done in 50, rewrite it. Ask yourself: "would a senior engineer say this is overcomplicated?"

### 3. Surgical changes
Touch only what's necessary and follow the existing style — don't refactor what isn't broken or "improve" adjacent code/comments. Remove imports/variables/functions your changes made orphaned; don't delete pre-existing dead code (mention it instead). Every changed line should trace back to the user's request.

### 4. Goal-driven execution
Turn the task into a verifiable criterion (e.g., "fix the bug" → "write a test that reproduces the bug, then make it pass"). For multi-step tasks, state a brief plan (Step → verify: check). Strong criteria allow independent iteration; weak criteria ("make it work") require constant clarification.
