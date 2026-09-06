"""How does kombu-pgmq compare to the brokers Celery normally uses?

Same send / read+ack loop as test_performance.py's test_channel_throughput
(Producer.publish / basic_get / ack), just pointed at Redis and RabbitMQ via
Kombu's own built-in transports instead of ours. Requires the `redis` and
`rabbitmq` services from docker-compose.yml.
"""

import time

import pytest
from kombu import Connection, Producer, Queue

from kombu_pgmq.transport import TransportPGMQ, TransportPsycopg

N = 200

pytestmark = pytest.mark.perf

PGMQ_CONNECTION_KWARGS = dict(
    hostname="localhost",
    port=5432,
    userid="postgres",
    password="postgres",
    virtual_host="postgres",
)

SCENARIOS = {
    "kombu-pgmq (pgmq)": dict(transport=TransportPGMQ, connection_kwargs=PGMQ_CONNECTION_KWARGS),
    "kombu-pgmq (psycopg)": dict(
        transport=TransportPsycopg, connection_kwargs=PGMQ_CONNECTION_KWARGS
    ),
    "redis": dict(
        transport="redis",
        connection_kwargs=dict(hostname="localhost", port=6379, virtual_host=0),
    ),
    "rabbitmq": dict(
        transport="pyamqp",
        connection_kwargs=dict(
            hostname="localhost", port=5672, userid="guest", password="guest", virtual_host="/"
        ),
    ),
}


def _run_ops(send, read_and_delete):
    t0 = time.perf_counter()
    for i in range(N):
        send(i)
    send_elapsed = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(N):
        read_and_delete()
    read_ack_elapsed = time.perf_counter() - t0

    return send_elapsed, read_ack_elapsed


@pytest.mark.parametrize("label", list(SCENARIOS))
def test_broker_throughput(label, queue_name):
    scenario = SCENARIOS[label]
    conn = Connection(transport=scenario["transport"], **scenario["connection_kwargs"])
    channel = conn.channel()
    channel.queue_declare(queue_name)
    producer = Producer(channel)
    queue = Queue(queue_name, channel=channel)
    try:
        send_elapsed, read_ack_elapsed = _run_ops(
            send=lambda i: producer.publish({"i": i}, routing_key=queue_name),
            read_and_delete=lambda: queue.get(no_ack=False).ack(),
        )
    finally:
        channel.queue_delete(queue_name)
        channel.close()
        conn.close()

    print(
        f"\n[{label}] send: {N / send_elapsed:.1f} msg/s "
        f"({send_elapsed:.3f}s for {N} msgs) | "
        f"read+ack: {N / read_ack_elapsed:.1f} msg/s "
        f"({read_ack_elapsed:.3f}s for {N} msgs)"
    )
