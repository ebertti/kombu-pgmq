"""When does `pool=True` actually pay off?

tests/test_performance.py showed no-pool winning under sequential,
single-threaded access. This file shares ONE Channel across multiple
threads sending concurrently, to find the thread-count threshold where a
real connection pool starts beating a single shared connection.

With `pool=False` all threads fight over the same underlying psycopg
connection (psycopg serializes concurrent use of one connection internally,
so it's safe, just increasingly congested). With `pool=True` each thread can
get its own connection from the pool, up to its max size.
"""

import threading
import time

import pytest
from kombu import Connection

from kombu_pgmq.transport import TransportPGMQ, TransportPsycopg

CONNECTION_KWARGS = dict(
    hostname="localhost",
    port=5432,
    userid="postgres",
    password="postgres",
    virtual_host="postgres",
)
TRANSPORTS = {"pgmq": TransportPGMQ, "psycopg": TransportPsycopg}
OPS_PER_THREAD = 40
THREAD_COUNTS = [1, 2, 4, 8, 16]

pytestmark = pytest.mark.perf


def _concurrent_send_throughput(backend_name, pool, num_threads, queue_name):
    conn = Connection(
        transport=TRANSPORTS[backend_name],
        transport_options={"pool": pool, "visibility_timeout": 30},
        **CONNECTION_KWARGS,
    )
    channel = conn.channel()
    channel._create_queue(queue_name)

    def worker(idx):
        for i in range(OPS_PER_THREAD):
            channel._send(queue_name, {"thread": idx, "i": i})

    threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(num_threads)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall_elapsed = time.perf_counter() - t0

    channel._drop_queue(queue_name)
    channel.close()
    conn.close()

    return (num_threads * OPS_PER_THREAD) / wall_elapsed


@pytest.mark.parametrize("num_threads", THREAD_COUNTS)
@pytest.mark.parametrize("pool", [False, True], ids=["no-pool", "pool"])
@pytest.mark.parametrize("backend_name", ["psycopg", "pgmq"])
def test_concurrent_send_throughput(backend_name, pool, num_threads, queue_name):
    throughput = _concurrent_send_throughput(backend_name, pool, num_threads, queue_name)
    label = "pool" if pool else "no-pool"
    print(
        f"\n[{backend_name}-{label}] {num_threads:2d} threads: "
        f"{throughput:.1f} msg/s aggregate "
        f"({OPS_PER_THREAD * num_threads} msgs total)"
    )
