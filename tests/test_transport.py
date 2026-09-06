import time

import pytest
from kombu import Connection, Producer, Queue

from kombu_pgmq.transport import TransportPGMQ, TransportPsycopg

CONNECTION_KWARGS = dict(
    hostname="localhost",
    port=5432,
    userid="postgres",
    password="postgres",
    virtual_host="postgres",
)

TRANSPORTS = {"pgmq": TransportPGMQ, "psycopg": TransportPsycopg}


def _publish(connection, queue_name, body):
    channel = connection.channel()
    queue = Queue(queue_name, routing_key=queue_name)
    Producer(channel).publish(body, routing_key=queue_name, declare=[queue])
    return channel


def test_put_get_ack(connection, queue_name):
    channel = _publish(connection, queue_name, {"hello": "world"})

    message = channel.basic_get(queue_name)
    assert message is not None
    assert message.payload == {"hello": "world"}

    message.ack()
    assert channel.basic_get(queue_name) is None


def test_reject_requeue_redelivers_after_visibility_timeout(connection, queue_name):
    channel = _publish(connection, queue_name, {"n": 1})

    message = channel.basic_get(queue_name)
    message.reject(requeue=True)

    assert channel.basic_get(queue_name) is None  # still invisible during VT

    time.sleep(1.5)  # visibility_timeout=1 set in the `connection` fixture

    redelivered = channel.basic_get(queue_name)
    assert redelivered is not None
    assert redelivered.payload == {"n": 1}
    redelivered.ack()


def test_reject_discard_never_redelivers(connection, queue_name):
    channel = _publish(connection, queue_name, {"n": 2})

    message = channel.basic_get(queue_name)
    message.reject(requeue=False)

    time.sleep(1.5)
    assert channel.basic_get(queue_name) is None


def test_size_purge_has_queue_delete(connection, queue_name):
    channel = connection.channel()
    channel.queue_declare(queue_name)
    assert channel._has_queue(queue_name) is True

    Producer(channel).publish({"a": 1}, routing_key=queue_name)
    Producer(channel).publish({"a": 2}, routing_key=queue_name)
    time.sleep(0.2)

    assert channel._size(queue_name) == 2
    assert channel._purge(queue_name) == 2
    assert channel._size(queue_name) == 0

    channel._delete(queue_name)
    assert channel._has_queue(queue_name) is False


@pytest.mark.parametrize("backend_name", ["pgmq", "psycopg"])
@pytest.mark.parametrize("pool", [True, False])
def test_pool_option(backend_name, pool, queue_name):
    conn = Connection(
        transport=TRANSPORTS[backend_name],
        transport_options={"pool": pool, "visibility_timeout": 1},
        **CONNECTION_KWARGS,
    )
    try:
        channel = _publish(conn, queue_name, {"ok": True})
        message = channel.basic_get(queue_name)
        assert message is not None
        assert message.payload == {"ok": True}
        message.ack()
    finally:
        conn.close()
