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


class Channel(virtual.Channel):
    """Kombu <-> PGMQ plumbing shared by both concrete channels below.

    Subclasses only need to implement the primitives that actually talk to
    PGMQ: `_queue_exists`, `_create_queue`, `_send`, `_read`,
    `_delete_message`, `_purge_queue`, `_drop_queue`, `_queue_size` and
    `_close_conn`.
    """

    visibility_timeout = 30

    from_transport_options = virtual.Channel.from_transport_options + (
        "visibility_timeout",
        "pool",
    )

    @cached_property
    def _dsn(self):
        client = self.connection.client
        dbname = (client.virtual_host or "").lstrip("/") or "postgres"
        return (
            f"postgresql://{client.userid}:{client.password}@"
            f"{client.hostname}:{client.port or self.transport.default_port}/"
            f"{dbname}"
        )

    def _new_queue(self, queue, **kwargs):
        if not self._queue_exists(queue):
            self._create_queue(queue)

    def _has_queue(self, queue, **kwargs):
        return self._queue_exists(queue)

    def _put(self, queue, message, **kwargs):
        self._send(queue, message)

    def _get(self, queue, timeout=None):
        result = self._read(queue, self.visibility_timeout)
        if result is None:
            raise Empty()
        msg_id, envelope = result
        envelope["properties"]["delivery_info"]["pgmq_msg_id"] = msg_id
        return envelope

    def _purge(self, queue):
        return self._purge_queue(queue)

    def _delete(self, queue, *args, **kwargs):
        self._drop_queue(queue)

    def _size(self, queue):
        return self._queue_size(queue)

    def basic_ack(self, delivery_tag, multiple=False):
        info = self.qos.get(delivery_tag).delivery_info
        self._delete_message(info["routing_key"], info["pgmq_msg_id"])
        super().basic_ack(delivery_tag, multiple=multiple)

    def basic_reject(self, delivery_tag, requeue=False):
        if not requeue:
            info = self.qos.get(delivery_tag).delivery_info
            self._delete_message(info["routing_key"], info["pgmq_msg_id"])
        # Never let the base class re-`_put` the message (would duplicate it
        # in PGMQ): requeue is handled natively by the visibility timeout.
        self.qos.reject(delivery_tag, requeue=False)

    def close(self):
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


class TransportPGMQ(virtual.Transport):
    Channel = ChannelPGMQ

    driver_type = "pgmq"
    driver_name = "pgmq"
    default_port = 5432
    polling_interval = 1.0


# Default/backwards-compatible alias: `broker_transport = "kombu_pgmq.transport:Transport"`.
Transport = TransportPGMQ
