# AGENTS.md — kombu-pgmq

Guidance for AI agents (Devin, Codex, Copilot, etc.) working in this repository.

## Project context

`kombu-pgmq` is a Kombu transport that uses PGMQ (a PostgreSQL extension) as the Celery broker.
Read `CLAUDE.md` before any task — it has the design decision map and the MVP scope.

## Engineering Principles

### 1. Think before coding
State your assumptions; if there's uncertainty or multiple interpretations, ask/present options — don't choose silently. If a simpler approach exists, say so. If something is unclear, stop and name what's confusing.

### 2. Simplicity first
The minimum code that solves the problem, nothing speculative: no features, abstractions, "flexibility/configurability", or error handling that wasn't requested. If you wrote 200 lines and it could be done in 50, rewrite it. Ask yourself: "would a senior engineer say this is overcomplicated?"

### 3. Surgical changes
Touch only what's necessary and follow the existing style — don't refactor what isn't broken or "improve" adjacent code/comments. Remove imports/variables/functions your changes made orphaned; don't delete pre-existing dead code (mention it instead). Every changed line should trace back to the user's request.

### 4. Goal-driven execution
Turn the task into a verifiable criterion (e.g., "fix the bug" → "write a test that reproduces the bug, then make it pass"). For multi-step tasks, state a brief plan (Step → verify: check). Strong criteria allow independent iteration; weak criteria ("make it work") require constant clarification.

## What not to do

- Do not implement fanout, topic exchange, priority, or celery events — out of MVP scope.
- Do not delete messages in `_get()` — deletion only happens on ACK (`pgmq.delete(msg_id)`).
- Do not add dependencies without explicit approval from the author.
- Do not refactor adjacent code that wasn't touched by the task.
- Do not speculate about future requirements — implement only what was asked.

## Expected message flow

```
publish  → pgmq.send(queue, payload)
receive  → pgmq.read(queue, vt=visibility_timeout, qty=1)
           └── msg_id stored in message["properties"]["delivery_info"]["pgmq_msg_id"]
ack      → pgmq.delete(queue, msg_id)
reject (requeue=False) → pgmq.delete(queue, msg_id)
reject (requeue=True)  → no delete; VT expires and the message reappears
```

## How to run tests

```bash
docker compose up -d          # or: podman compose up -d
uv sync --group dev           # or: pip install -e ".[psycopg]" pytest celery
pytest
```

Tests run against the real Postgres+pgmq instance from `docker-compose.yml` (no mocking), parametrized over both transports (`TransportPGMQ`, `TransportPsycopg`).

`tests/test_performance.py` (single-worker), `tests/test_performance_concurrency.py` (multi-threaded) and `tests/test_performance_brokers.py` (vs. Redis/RabbitMQ, needs the `redis`/`rabbitmq` docker-compose services) benchmark throughput. All are excluded from the default `pytest` run (marked `perf`) since they're benchmarks, not correctness checks. Run them explicitly:

```bash
pytest tests/test_performance.py tests/test_performance_concurrency.py tests/test_performance_brokers.py -m perf -s
```

`docker/Dockerfile.smoketest` + `docker/smoketest.py` verify that `pip install kombu-pgmq[pgmq]` alone (no dev deps, no `[psycopg]`, no `celery`) actually works standalone against a real PGMQ:

```bash
podman compose up -d postgres
podman build -f docker/Dockerfile.smoketest -t kombu-pgmq-smoketest .
podman run --rm --network kombu-pgmq_default kombu-pgmq-smoketest
```

## CI/CD

- `.github/workflows/ci.yml`: on every PR into `main`, runs `ruff check`/`ruff format --check` and the default `pytest` suite (Postgres+pgmq via `docker compose up -d --wait postgres`; perf tests excluded, same as local) across a Python 3.10–3.14 matrix (`requires-python`'s floor is 3.10, matching `psycopg`/`pgmq`'s own `requires_python`).
- `.github/workflows/release.yml`: publishes to PyPI on push of a `vX.Y.Z` tag. Verifies the tag matches `pyproject.toml`'s `version` before building. Uses PyPI Trusted Publishing (OIDC, `uv publish`) — no secrets, but requires a one-time setup on pypi.org (project's "Publishing" settings → add this repo/workflow/`pypi` environment as a trusted publisher) before the first release. To release: bump `version` in `pyproject.toml`, then `git tag vX.Y.Z && git push --tags`.

## References

- [Kombu virtual transport](https://github.com/celery/kombu/blob/main/kombu/transport/virtual/base.py)
- [Kombu SQS transport](https://github.com/celery/kombu/blob/main/kombu/transport/SQS/__init__.py) — closest reference implementation (vt/receipt-handle semantics)
- [PGMQ](https://github.com/pgmq/pgmq) / [PGMQ Python client](https://github.com/pgmq/pgmq-py)
- `plan/inicial.md` — initial project rationale
