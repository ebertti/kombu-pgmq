from kombu.transport import TRANSPORT_ALIASES

from .transport import Transport, TransportPGMQ, TransportPsycopg

TRANSPORT_ALIASES["pgmq"] = "kombu_pgmq.transport:TransportPGMQ"

__all__ = ["Transport", "TransportPGMQ", "TransportPsycopg"]
