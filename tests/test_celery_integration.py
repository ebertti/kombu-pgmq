from celery import Celery
from celery.contrib.testing.tasks import ping  # noqa: F401 (registers celery.ping)
from celery.contrib.testing.worker import start_worker


def test_task_runs_end_to_end(queue_name):
    app = Celery("kombu_pgmq_test")
    app.conf.broker_transport = "kombu_pgmq.transport:Transport"
    app.conf.broker_url = "postgresql://postgres:postgres@localhost:5432/postgres"
    app.conf.result_backend = "cache+memory://"
    app.conf.task_default_queue = queue_name

    @app.task(name="add")
    def add(x, y):
        return x + y

    with start_worker(app, pool="solo"):
        result = add.delay(2, 3)
        assert result.get(timeout=10) == 5


def test_control_inspect_ping(queue_name):
    """`celery inspect`/`control` (pidbox) rides on a fanout exchange under
    the hood — this is really a fanout-routing test in Celery's clothing."""
    app = Celery("kombu_pgmq_test_control")
    app.conf.broker_transport = "kombu_pgmq.transport:Transport"
    app.conf.broker_url = "postgresql://postgres:postgres@localhost:5432/postgres"
    app.conf.result_backend = "cache+memory://"
    app.conf.task_default_queue = queue_name

    with start_worker(app, pool="solo"):
        pong = app.control.inspect(timeout=5).ping()
        assert pong
        assert all(reply == {"ok": "pong"} for reply in pong.values())
