"""HTTP entry point.

    POST /new_ride                     form or JSON: source, destination, time, cost, seats
                                       -> 202 {"task_id": ...}
    POST /new_ride_matching_consumer   a matching worker registers itself
    GET  /consumers                    registered workers
    GET  /health
"""

from __future__ import annotations

import json
import threading
from uuid import uuid4

import pika
from flask import Flask, jsonify, request

from . import mq

REQUIRED = {"source": str, "destination": str, "time": int, "cost": float, "seats": int}


def validate(data: dict) -> tuple[dict | None, str | None]:
    ride = {}
    for field, cast in REQUIRED.items():
        if field not in data or str(data[field]).strip() == "":
            return None, f"missing field '{field}'"
        try:
            ride[field] = cast(data[field])
        except (TypeError, ValueError):
            return None, f"'{field}' must be {cast.__name__}"
    if ride["time"] < 0 or ride["seats"] < 1 or ride["cost"] < 0:
        return None, "time and cost must be non-negative and seats at least 1"
    return ride, None


class Publisher:
    """One long-lived connection with publisher confirms, reconnecting if it drops."""

    def __init__(self, connect=mq.connect):
        self._connect = connect
        self._lock = threading.Lock()
        self._channel = None

    def _get_channel(self):
        if self._channel is None or self._channel.is_closed:
            conn = self._connect()
            ch = conn.channel()
            mq.declare_queues(ch)
            ch.confirm_delivery()          # basic_publish now raises if the broker refuses
            self._channel = ch
        return self._channel

    def publish(self, task_id: str, body: bytes) -> None:
        props = pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                                     message_id=task_id, content_type="application/json")
        with self._lock:
            for attempt in range(2):
                try:
                    ch = self._get_channel()
                    ch.basic_publish(exchange="", routing_key=mq.RIDE_QUEUE, body=body, properties=props)
                    ch.basic_publish(exchange="", routing_key=mq.DB_QUEUE, body=body, properties=props)
                    return
                except (pika.exceptions.AMQPError, OSError):
                    self._channel = None
                    if attempt == 1:
                        raise


def create_app(publisher: Publisher | None = None) -> Flask:
    app = Flask(__name__)
    publisher = publisher or Publisher()
    consumers: dict[str, dict] = {}

    @app.post("/new_ride")
    def new_ride():
        data = request.get_json(silent=True) or request.form.to_dict()
        ride, error = validate(data)
        if error:
            return jsonify(error=error), 400
        task_id = "task_" + uuid4().hex
        try:
            publisher.publish(task_id, json.dumps(ride).encode())
        except Exception:  # noqa: BLE001
            app.logger.exception("publish failed")
            return jsonify(error="message broker unavailable"), 503
        return jsonify(task_id=task_id), 202

    @app.post("/new_ride_matching_consumer")
    def register():
        data = request.get_json(silent=True) or request.form.to_dict()
        cid = data.get("consumer_id")
        if not cid:
            return jsonify(error="missing consumer_id"), 400
        consumers[cid] = {"consumer_id": cid, "ip_address": request.remote_addr}
        return jsonify(consumers[cid]), 200

    @app.get("/consumers")
    def list_consumers():
        return jsonify(list(consumers.values()))

    @app.get("/health")
    def health():
        return jsonify(status="ok")

    return app


app = create_app()
