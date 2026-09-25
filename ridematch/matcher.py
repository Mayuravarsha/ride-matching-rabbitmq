"""Ride matching worker.

Takes one ride at a time (prefetch 1) and acknowledges it only after the
simulated trip finishes, so if the worker dies mid-ride RabbitMQ hands the
ride to another worker. Malformed messages are rejected to the dead-letter
queue instead of crashing the worker and being redelivered forever.
"""

from __future__ import annotations

import json
import logging
import os
import time

import requests

from . import mq

log = logging.getLogger("matcher")


def parse(body: bytes) -> dict | None:
    try:
        ride = json.loads(body)
        ride["time"] = float(ride["time"])
        return ride if ride["time"] >= 0 else None
    except (ValueError, KeyError, TypeError):
        return None


def make_callback(consumer_id: str, sleep=time.sleep, time_scale: float = 1.0):
    def callback(ch, method, properties, body):
        ride = parse(body)
        if ride is None:
            log.warning("%s rejected malformed message %s", consumer_id, properties.message_id)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        log.info("%s matched %s: %s -> %s", consumer_id, properties.message_id,
                 ride.get("source"), ride.get("destination"))
        sleep(ride["time"] * time_scale)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("%s finished %s", consumer_id, properties.message_id)
    return callback


def register(producer: str, consumer_id: str, attempts: int = 30) -> None:
    for _ in range(attempts):
        try:
            requests.post(f"http://{producer}/new_ride_matching_consumer",
                          data={"consumer_id": consumer_id}, timeout=5).raise_for_status()
            return
        except requests.RequestException:
            time.sleep(2)
    raise RuntimeError(f"could not register with producer at {producer}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    consumer_id = os.environ.get("CONSUMER_ID") or os.environ.get("HOSTNAME", "matcher")
    register(os.environ.get("PRODUCER_ADDRESS", "producer:5000"), consumer_id)
    ch = mq.connect().channel()
    mq.declare_queues(ch)
    ch.basic_qos(prefetch_count=1)
    scale = float(os.environ.get("RIDE_TIME_SCALE", "1"))
    ch.basic_consume(queue=mq.RIDE_QUEUE, on_message_callback=make_callback(consumer_id, time_scale=scale))
    log.info("%s waiting for rides", consumer_id)
    ch.start_consuming()


if __name__ == "__main__":
    main()
