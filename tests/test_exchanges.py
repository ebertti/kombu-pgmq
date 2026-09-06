"""Topic and fanout exchange routing.

Each binding is made on its own `Connection`/`Channel` to prove the binding
table is actually persisted in Postgres (visible across separate processes in
a real deployment), not just kept in the in-memory `state` the Kombu base
class defaults to — that in-memory default is exactly what made fanout/topic
not really work across worker processes before this.
"""

from kombu import Connection, Exchange, Producer, Queue

from kombu_pgmq.transport import TransportPGMQ, TransportPsycopg

CONNECTION_KWARGS = dict(
    hostname="localhost",
    port=5432,
    userid="postgres",
    password="postgres",
    virtual_host="postgres",
)
TRANSPORTS = {"pgmq": TransportPGMQ, "psycopg": TransportPsycopg}


def _connection(backend_name):
    return Connection(
        transport=TRANSPORTS[backend_name],
        transport_options={"visibility_timeout": 5},
        **CONNECTION_KWARGS,
    )


def test_topic_exchange_routes_across_separate_channels(
    backend_name, exchange_name, make_queue_name
):
    errors_queue = make_queue_name()
    all_logs_queue = make_queue_name()
    info_queue = make_queue_name()
    exchange = Exchange(exchange_name, type="topic")

    conn_errors = _connection(backend_name)
    channel_errors = conn_errors.channel()
    Queue(errors_queue, exchange=exchange, routing_key="logs.*.error")(channel_errors).declare()

    conn_all = _connection(backend_name)
    channel_all = conn_all.channel()
    Queue(all_logs_queue, exchange=exchange, routing_key="logs.#")(channel_all).declare()

    conn_info = _connection(backend_name)
    channel_info = conn_info.channel()
    Queue(info_queue, exchange=exchange, routing_key="logs.*.info")(channel_info).declare()

    conn_pub = _connection(backend_name)
    try:
        Producer(conn_pub.channel()).publish(
            {"msg": "boom"}, exchange=exchange, routing_key="logs.api.error", declare=[exchange]
        )

        message = channel_errors.basic_get(errors_queue)
        assert message is not None
        assert message.payload == {"msg": "boom"}
        message.ack()

        message = channel_all.basic_get(all_logs_queue)
        assert message is not None
        assert message.payload == {"msg": "boom"}
        message.ack()

        assert channel_info.basic_get(info_queue) is None
    finally:
        for conn in (conn_errors, conn_all, conn_info, conn_pub):
            conn.close()


def test_fanout_exchange_delivers_to_all_bound_queues(backend_name, exchange_name, make_queue_name):
    queue_a = make_queue_name()
    queue_b = make_queue_name()
    exchange = Exchange(exchange_name, type="fanout")

    conn_a = _connection(backend_name)
    channel_a = conn_a.channel()
    Queue(queue_a, exchange=exchange)(channel_a).declare()

    conn_b = _connection(backend_name)
    channel_b = conn_b.channel()
    Queue(queue_b, exchange=exchange)(channel_b).declare()

    conn_pub = _connection(backend_name)
    try:
        Producer(conn_pub.channel()).publish(
            {"event": "broadcast"}, exchange=exchange, declare=[exchange]
        )

        message = channel_a.basic_get(queue_a)
        assert message is not None
        assert message.payload == {"event": "broadcast"}
        message.ack()

        message = channel_b.basic_get(queue_b)
        assert message is not None
        assert message.payload == {"event": "broadcast"}
        message.ack()
    finally:
        for conn in (conn_a, conn_b, conn_pub):
            conn.close()
