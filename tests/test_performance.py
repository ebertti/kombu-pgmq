import time

import pytest
from kombu import Connection, Producer

from kombu_pgmq.transport import TransportPGMQ, TransportPsycopg

CONNECTION_KWARGS = dict(
    hostname="localhost",
    port=5432,
    userid="postgres",
    password="postgres",
    virtual_host="postgres",
)
TRANSPORTS = {"pgmq": TransportPGMQ, "psycopg": TransportPsycopg}
N = 200

pytestmark = pytest.mark.perf

SCENARIOS = [(backend_name, pool) for backend_name in ("psycopg", "pgmq") for pool in (False, True)]


def _scenario_id(backend_name, pool):
    return f"{backend_name}-{'pool' if pool else 'no-pool'}"


def _connection(backend_name, pool):
    return Connection(
        transport=TRANSPORTS[backend_name],
        transport_options={"pool": pool, "visibility_timeout": 30},
        **CONNECTION_KWARGS,
    )


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


def _report(label, send_elapsed, read_ack_elapsed):
    print(
        f"\n[{label}] send: {N / send_elapsed:.1f} msg/s "
        f"({send_elapsed:.3f}s for {N} msgs) | "
        f"read+ack: {N / read_ack_elapsed:.1f} msg/s "
        f"({read_ack_elapsed:.3f}s for {N} msgs)"
    )


# --- raw Channel primitives (no Producer/Consumer/QoS involved) ---


@pytest.mark.parametrize("backend_name,pool", SCENARIOS, ids=[_scenario_id(*s) for s in SCENARIOS])
def test_channel_primitives_throughput(backend_name, pool, queue_name):
    conn = _connection(backend_name, pool)
    channel = conn.channel()
    channel._create_queue(queue_name)
    try:
        send_elapsed, read_ack_elapsed = _run_ops(
            send=lambda i: channel._send(queue_name, {"i": i}),
            read_and_delete=lambda: channel._delete_message(
                queue_name, channel._read(queue_name, 30)[0]
            ),
        )
    finally:
        channel._drop_queue(queue_name)
        channel.close()
        conn.close()

    _report(f"primitives:{_scenario_id(backend_name, pool)}", send_elapsed, read_ack_elapsed)


# --- through Kombu's Channel (Producer.publish / basic_get / ack) ---


@pytest.mark.parametrize("backend_name,pool", SCENARIOS, ids=[_scenario_id(*s) for s in SCENARIOS])
def test_channel_throughput(backend_name, pool, queue_name):
    conn = _connection(backend_name, pool)
    channel = conn.channel()
    channel.queue_declare(queue_name)
    producer = Producer(channel)
    try:
        send_elapsed, read_ack_elapsed = _run_ops(
            send=lambda i: producer.publish({"i": i}, routing_key=queue_name),
            read_and_delete=lambda: channel.basic_get(queue_name).ack(),
        )
    finally:
        channel._delete(queue_name)
        channel.close()
        conn.close()

    _report(f"channel:{_scenario_id(backend_name, pool)}", send_elapsed, read_ack_elapsed)
