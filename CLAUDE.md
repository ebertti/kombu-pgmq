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
│       ├── transport.py
│       └── connection.py
└── tests/
    ├── test_transport.py
    └── test_celery_integration.py
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
- Store `msg_id` in the message body for the later ACK (`_pgmq_msg_id`).
- Use `psycopg` directly (not the official PGMQ Python client) to control pooling and avoid an extra dependency — revisable decision.
- Broker URL: `pgmq://user:pass@host:5432/db` or `broker_transport = "kombu_pgmq.transport:Transport"` + `broker_url = "postgresql://..."`.

## Engineering Principles

### 1. Think before coding
State your assumptions; if there's uncertainty or multiple interpretations, ask/present options — don't choose silently. If a simpler approach exists, say so. If something is unclear, stop and name what's confusing.

### 2. Simplicity first
The minimum code that solves the problem, nothing speculative: no features, abstractions, "flexibility/configurability", or error handling that wasn't requested. If you wrote 200 lines and it could be done in 50, rewrite it. Ask yourself: "would a senior engineer say this is overcomplicated?"

### 3. Surgical changes
Touch only what's necessary and follow the existing style — don't refactor what isn't broken or "improve" adjacent code/comments. Remove imports/variables/functions your changes made orphaned; don't delete pre-existing dead code (mention it instead). Every changed line should trace back to the user's request.

### 4. Goal-driven execution
Turn the task into a verifiable criterion (e.g., "fix the bug" → "write a test that reproduces the bug, then make it pass"). For multi-step tasks, state a brief plan (Step → verify: check). Strong criteria allow independent iteration; weak criteria ("make it work") require constant clarification.
