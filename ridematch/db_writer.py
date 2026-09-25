"""Persists every ride to MongoDB.

With at-least-once delivery the same message can arrive twice (for example
if the writer dies after inserting but before acknowledging). The original
insert_one then raised DuplicateKeyError on the redelivery, crashed and was
restarted forever. Writes are now upserts keyed by the message id, so
processing a message twice is harmless.
"""

from __future__ import annotations

import json
import logging
import os

from . import mq

log = logging.getLogger("db_writer")


def make_callback(collection):
    def callback(ch, method, properties, body):
        try:
            ride = json.loads(body)
        except ValueError:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        ride["_id"] = properties.message_id
        collection.replace_one({"_id": ride["_id"]}, ride, upsert=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log.info("stored %s", ride["_id"])
    return callback


def main() -> None:
    import pymongo

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    client = pymongo.MongoClient(os.environ.get("MONGO_URL", "mongodb://mongodb:27017"),
                                 serverSelectionTimeoutMS=30_000)
    client.admin.command("ping")
    collection = client["ride_matching"]["ride_details"]
    ch = mq.connect().channel()
    mq.declare_queues(ch)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=mq.DB_QUEUE, on_message_callback=make_callback(collection))
    log.info("database writer ready")
    ch.start_consuming()


if __name__ == "__main__":
    main()
