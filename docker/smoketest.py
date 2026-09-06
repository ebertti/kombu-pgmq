"""Smoke test for a `pip install kombu-pgmq[pgmq]` install — no dev/test
dependencies, no psycopg extra, nothing but what that one extra provides.

Run against the `postgres` service from docker-compose.yml.
"""

from kombu import Connection, Producer, Queue

import kombu_pgmq  # noqa: F401  (registers the `pgmq://` URL scheme)

QUEUE = "smoketest_queue"

conn = Connection("pgmq://postgres:postgres@postgres:5432/postgres")
channel = conn.channel()
queue = Queue(QUEUE, routing_key=QUEUE)
Producer(channel).publish({"hello": "docker"}, routing_key=QUEUE, declare=[queue])

message = channel.basic_get(QUEUE)
assert message is not None, "no message received"
assert message.payload == {"hello": "docker"}, f"unexpected payload: {message.payload}"
message.ack()

assert channel.basic_get(QUEUE) is None, "message was not consumed"

channel._delete(QUEUE)
channel.close()
conn.close()

print("OK: kombu-pgmq[pgmq] works standalone")
