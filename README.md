# kombu-pgmq

Kombu transport for PGMQ — use PostgreSQL as the Celery broker, without Redis or RabbitMQ.

## Development

Start a local PostgreSQL instance with the PGMQ extension pre-installed:

```bash
docker compose up -d
```

This exposes Postgres on `localhost:5432` (user `postgres`, password `postgres`, db `postgres`) with the `pgmq` extension already created.
