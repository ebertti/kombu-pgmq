import hashlib
from queue import Empty

from kombu.transport import virtual
from kombu.utils.objects import cached_property

try:
    import psycopg
    from psycopg.types.json import Jsonb
    from psycopg_pool import ConnectionPool
except ImportError:
    psycopg = None

try:
    from pgmq import PGMQueue
except ImportError:
    PGMQueue = None

#: PGMQ caps queue names at 47 characters. Celery's pidbox/events generate
#: longer auto-named queues (e.g. `<uuid>.reply.celery.pidbox`), so long
#: names are deterministically shortened before ever reaching PGMQ. Only
#: applied at the PGMQ call boundary — the Kombu-facing queue name (bindings
#: table, delivery_info, etc.) is always the original, untruncated one.
PGMQ_MAX_QUEUE_NAME_LENGTH = 47


def _pgmq_name(queue):
    if len(queue) <= PGMQ_MAX_QUEUE_NAME_LENGTH:
        return queue
    digest = hashlib.sha1(queue.encode()).hexdigest()[:8]
    return f"{queue[:38]}_{digest}"


class Channel(virtual.Channel):
    """Kombu <-> PGMQ plumbing shared by both concrete channels below.

    Subclasses only need to implement the primitives that actually talk to
    PGMQ: `_queue_exists`, `_create_queue`, `_send`, `_read`,
    `_delete_message`, `_purge_queue`, `_drop_queue`, `_queue_size` and
    `_close_conn`.
    """

    visibility_timeout = 30
    check_extension = True

    from_transport_options = virtual.Channel.from_transport_options + (
        "visibility_timeout",
        "pool",
        "check_extension",
    )

    # Exchange/queue bindings must be persisted (not just kept in the
    # in-process `self.state` the base class defaults to) so that fanout and
    # topic routing work across separate worker processes. This flag makes
    # the base `queue_bind()` call our `_queue_bind()` for every exchange
    # type, not just fanout — same approach kombu's own Redis transport uses.
    supports_fanout = True

    @cached_property
    def _dsn(self):
        client = self.connection.client
        dbname = (client.virtual_host or "").lstrip("/") or "postgres"
        return (
            f"postgresql://{client.userid}:{client.password}@"
            f"{client.hostname}:{client.port or self.transport.default_port}/"
            f"{dbname}"
        )

    @cached_property
    def _bindings_conn(self):
        import psycopg

        conn = psycopg.connect(self._dsn, autocommit=True)
        if self.check_extension:
            row = conn.execute("SELECT 1 FROM pg_extension WHERE extname = 'pgmq'").fetchone()
            if row is None:
                raise RuntimeError(
                    "The 'pgmq' PostgreSQL extension is not installed in this database. "
                    "Run: CREATE EXTENSION IF NOT EXISTS pgmq; "
                    "See the README for provider-specific instructions. "
                    "To skip this check: transport_options={'check_extension': False}."
                )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kombu_pgmq_bindings (
                exchange text NOT NULL,
                routing_key text NOT NULL DEFAULT '',
                pattern text NOT NULL DEFAULT '',
                queue text NOT NULL,
                PRIMARY KEY (exchange, routing_key, queue)
            )
            """
        )
        return conn

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        self._bindings_conn.execute(
            """
            INSERT INTO kombu_pgmq_bindings (exchange, routing_key, pattern, queue)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (exchange, routing_key, queue) DO NOTHING
            """,
            (exchange, routing_key or "", pattern or "", queue),
        )

    def get_table(self, exchange):
        rows = self._bindings_conn.execute(
            "SELECT routing_key, pattern, queue FROM kombu_pgmq_bindings WHERE exchange = %s",
            (exchange,),
        ).fetchall()
        return [tuple(row) for row in rows]

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        for _, _, queue in self.get_table(exchange):
            self._send(_pgmq_name(queue), message)

    def _new_queue(self, queue, **kwargs):
        queue = _pgmq_name(queue)
        if not self._queue_exists(queue):
            self._create_queue(queue)

    def _has_queue(self, queue, **kwargs):
        return self._queue_exists(_pgmq_name(queue))

    def _put(self, queue, message, **kwargs):
        self._send(_pgmq_name(queue), message)

    def _get(self, queue, timeout=None):
        pgmq_queue = _pgmq_name(queue)
        result = self._read(pgmq_queue, self.visibility_timeout)
        if result is None:
            raise Empty()
        msg_id, envelope = result
        envelope["properties"]["delivery_info"]["pgmq_msg_id"] = msg_id
        # The actual PGMQ queue a message came from, needed for ack/reject.
        # Not always equal to `routing_key` (e.g. fanout/topic exchanges,
        # where the routing key differs from the bound queue name), and
        # already shortened to fit PGMQ's 47-char limit.
        envelope["properties"]["delivery_info"]["pgmq_queue"] = pgmq_queue
        return envelope

    def _purge(self, queue):
        return self._purge_queue(_pgmq_name(queue))

    def _delete(self, queue, exchange=None, routing_key=None, pattern=None, *args, **kwargs):
        if exchange is not None:
            self._bindings_conn.execute(
                "DELETE FROM kombu_pgmq_bindings WHERE exchange = %s AND queue = %s",
                (exchange, queue),
            )
        self._drop_queue(_pgmq_name(queue))

    def _size(self, queue):
        return self._queue_size(_pgmq_name(queue))

    def basic_ack(self, delivery_tag, multiple=False):
        info = self.qos.get(delivery_tag).delivery_info
        self._delete_message(info["pgmq_queue"], info["pgmq_msg_id"])
        super().basic_ack(delivery_tag, multiple=multiple)

    def basic_reject(self, delivery_tag, requeue=False):
        if not requeue:
            info = self.qos.get(delivery_tag).delivery_info
            self._delete_message(info["pgmq_queue"], info["pgmq_msg_id"])
        # Never let the base class re-`_put` the message (would duplicate it
        # in PGMQ): requeue is handled natively by the visibility timeout.
        self.qos.reject(delivery_tag, requeue=False)

    def close(self):
        if "_bindings_conn" in self.__dict__:
            self._bindings_conn.close()
        self._close_conn()
        super().close()


class ChannelPsycopg(Channel):
    """PGMQ access via raw SQL calls over psycopg 3.

    Checks out a connection from a `psycopg_pool.ConnectionPool` on every
    call by default (`pool=True`). Set `transport_options["pool"] = False` to
    hold a single connection open and reuse it instead — faster if this
    channel is only ever used by one thread at a time.
    """

    pool = True

    @cached_property
    def _conn(self):
        if psycopg is None:
            raise ImportError(
                "psycopg is not installed. Install it with: pip install kombu-pgmq[psycopg]"
            )
        return psycopg.connect(self._dsn, autocommit=True)

    @cached_property
    def _pgpool(self):
        if psycopg is None:
            raise ImportError(
                "psycopg is not installed. Install it with: pip install kombu-pgmq[psycopg]"
            )
        return ConnectionPool(self._dsn, open=True)

    def _execute(self, sql, params):
        if self.pool:
            with self._pgpool.connection() as conn:
                return conn.execute(sql, params).fetchone()
        return self._conn.execute(sql, params).fetchone()

    def _queue_exists(self, queue):
        row = self._execute("SELECT 1 FROM pgmq.list_queues() WHERE queue_name = %s", (queue,))
        return row is not None

    def _create_queue(self, queue):
        self._execute("SELECT pgmq.create(%s)", (queue,))

    def _send(self, queue, message):
        row = self._execute("SELECT pgmq.send(%s, %s)", (queue, Jsonb(message)))
        return row[0]

    def _read(self, queue, vt) -> tuple | None:
        row = self._execute("SELECT msg_id, message FROM pgmq.read(%s, %s, 1)", (queue, vt))
        if row is None:
            return None
        return row[0], row[1]

    def _delete_message(self, queue, msg_id):
        row = self._execute("SELECT pgmq.delete(%s, %s)", (queue, msg_id))
        return row[0]

    def _purge_queue(self, queue):
        row = self._execute("SELECT pgmq.purge_queue(%s)", (queue,))
        return row[0]

    def _drop_queue(self, queue):
        row = self._execute("SELECT pgmq.drop_queue(%s)", (queue,))
        return row[0]

    def _queue_size(self, queue):
        row = self._execute("SELECT queue_length FROM pgmq.metrics(%s)", (queue,))
        return row[0]

    def _close_conn(self):
        if "_pgpool" in self.__dict__:
            self._pgpool.close()
        if "_conn" in self.__dict__:
            self._conn.close()


class ChannelPGMQ(Channel):
    """PGMQ access via the official `pgmq` client.

    By default (`pool=True`) it uses the client's own `psycopg_pool`
    connection pool. Set `transport_options["pool"] = False` to open a single
    connection instead and pass it explicitly to every call, bypassing the
    pool checkout — cheaper for a single worker talking to PGMQ sequentially.
    """

    pool = True

    @cached_property
    def _client(self):
        if PGMQueue is None:
            raise ImportError(
                "pgmq is not installed. Install it with: pip install kombu-pgmq[pgmq]"
            )
        return PGMQueue(conn_string=self._dsn, init_extension=False)

    @cached_property
    def _extra_conn(self):
        import psycopg

        return psycopg.connect(self._dsn, autocommit=True)

    def _kwargs(self):
        return {} if self.pool else {"conn": self._extra_conn}

    def _queue_exists(self, queue):
        return any(q.queue_name == queue for q in self._client.list_queues(**self._kwargs()))

    def _create_queue(self, queue):
        self._client.create_queue(queue, **self._kwargs())

    def _send(self, queue, message):
        return self._client.send(queue, message, **self._kwargs())

    def _read(self, queue, vt) -> tuple | None:
        msg = self._client.read(queue, vt=vt, qty=1, **self._kwargs())
        if msg is None:
            return None
        return msg.msg_id, msg.message

    def _delete_message(self, queue, msg_id):
        return self._client.delete(queue, msg_id, **self._kwargs())

    def _purge_queue(self, queue):
        return self._client.purge(queue, **self._kwargs())

    def _drop_queue(self, queue):
        return self._client.drop_queue(queue, **self._kwargs())

    def _queue_size(self, queue):
        return self._client.metrics(queue, **self._kwargs()).queue_length

    def _close_conn(self):
        if "_extra_conn" in self.__dict__:
            self._extra_conn.close()
        if "_client" in self.__dict__:
            self._client.close()


class TransportPsycopg(virtual.Transport):
    Channel = ChannelPsycopg

    driver_type = "pgmq"
    driver_name = "pgmq-psycopg"
    default_port = 5432
    polling_interval = 1.0
    implements = virtual.Transport.implements.extend(
        exchange_type=frozenset(["direct", "topic", "fanout"]),
    )


class TransportPGMQ(virtual.Transport):
    Channel = ChannelPGMQ

    driver_type = "pgmq"
    driver_name = "pgmq"
    default_port = 5432
    polling_interval = 1.0
    implements = virtual.Transport.implements.extend(
        exchange_type=frozenset(["direct", "topic", "fanout"]),
    )


# Default/backwards-compatible alias: `broker_transport = "kombu_pgmq.transport:Transport"`.
Transport = TransportPGMQ
