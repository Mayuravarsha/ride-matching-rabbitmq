"""RabbitMQ helpers shared by every service."""

from __future__ import annotations

import logging
import os
import time

import pika

log = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RIDE_QUEUE = "ride_match"
DB_QUEUE = "database"
DEAD_LETTER_QUEUE = "rejected"


def connect(host: str = RABBITMQ_HOST, attempts: int = 30, delay: float = 2.0) -> pika.BlockingConnection:
    """Connect, retrying while the broker starts (replaces the fixed sleep(10))."""
    params = pika.ConnectionParameters(host=host, heartbeat=60, blocked_connection_timeout=30)
    for attempt in range(1, attempts + 1):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            if attempt == attempts:
                raise
            log.info("RabbitMQ not ready (attempt %d/%d), retrying in %.0fs", attempt, attempts, delay)
            time.sleep(delay)
    raise RuntimeError("unreachable")


def declare_queues(channel) -> None:
    """Durable work queues. Messages a consumer rejects go to a dead-letter queue
    instead of being redelivered forever."""
    channel.queue_declare(queue=DEAD_LETTER_QUEUE, durable=True)
    args = {"x-dead-letter-exchange": "", "x-dead-letter-routing-key": DEAD_LETTER_QUEUE}
    for q in (RIDE_QUEUE, DB_QUEUE):
        channel.queue_declare(queue=q, durable=True, arguments=args)
