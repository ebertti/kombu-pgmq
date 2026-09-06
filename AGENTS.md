# AGENTS.md — kombu-pgmq

Guidance for AI agents (Claude, Codex, Copilot, etc.) working in this repository.

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
           └── msg_id stored in message["_pgmq_msg_id"]
ack      → pgmq.delete(queue, msg_id)
reject   → no delete; VT expires and the message reappears
```

## How to run tests

> To be filled in once the test structure is defined.

## References

- [Kombu virtual transport](https://github.com/celery/kombu/blob/main/kombu/transport/virtual/base.py)
- [PGMQ Python client](https://github.com/tembo-io/pgmq)
- `plan/inicial.md` — initial project rationale
