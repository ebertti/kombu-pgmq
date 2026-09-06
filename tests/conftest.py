import uuid

import psycopg
import pytest
from kombu import Connection

from kombu_pgmq.transport import TransportPGMQ, TransportPsycopg

DSN = "postgresql://postgres:postgres@localhost:5432/postgres"

CONNECTION_KWARGS = dict(
    hostname="localhost",
    port=5432,
    userid="postgres",
    password="postgres",
    virtual_host="postgres",
)

TRANSPORTS = {"pgmq": TransportPGMQ, "psycopg": TransportPsycopg}


@pytest.fixture(params=["pgmq", "psycopg"])
def backend_name(request):
    return request.param


@pytest.fixture
def queue_name():
    name = f"test_{uuid.uuid4().hex[:12]}"
    yield name
    conn = psycopg.connect(DSN, autocommit=True)
    try:
        conn.execute("SELECT pgmq.drop_queue(%s)", (name,))
    except Exception:
        pass
    finally:
        conn.close()


@pytest.fixture
def exchange_name():
    return f"test_exchange_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def make_queue_name():
    created = []

    def _make():
        name = f"test_{uuid.uuid4().hex[:12]}"
        created.append(name)
        return name

    yield _make

    conn = psycopg.connect(DSN, autocommit=True)
    try:
        for name in created:
            try:
                conn.execute("SELECT pgmq.drop_queue(%s)", (name,))
            except Exception:
                pass
    finally:
        conn.close()


@pytest.fixture
def connection(backend_name):
    conn = Connection(
        transport=TRANSPORTS[backend_name],
        transport_options={"visibility_timeout": 1},
        **CONNECTION_KWARGS,
    )
    yield conn
    conn.close()
