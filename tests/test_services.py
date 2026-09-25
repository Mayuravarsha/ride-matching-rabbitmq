import json
from types import SimpleNamespace

import pika
import pytest

from ridematch import db_writer, matcher
from ridematch.producer import Publisher, create_app, validate

RIDE = {"source": "PES", "destination": "Airport", "time": "3", "cost": "450", "seats": "2"}


class FakeChannel:
    def __init__(self, fail_times=0):
        self.published, self.acks, self.nacks = [], [], []
        self.fail_times, self.is_closed = fail_times, False

    def basic_publish(self, exchange, routing_key, body, properties):
        if self.fail_times:
            self.fail_times -= 1
            raise pika.exceptions.AMQPConnectionError("down")
        self.published.append((routing_key, json.loads(body), properties))

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacks.append((delivery_tag, requeue))

    # used by Publisher._get_channel
    def queue_declare(self, **kw): pass
    def confirm_delivery(self): pass


def publisher_with(channel):
    pub = Publisher(connect=lambda: SimpleNamespace(channel=lambda: channel))
    return pub


def client(channel):
    app = create_app(publisher_with(channel))
    app.testing = True
    return app.test_client()


def test_validate():
    ride, err = validate(RIDE)
    assert err is None and ride == {"source": "PES", "destination": "Airport", "time": 3,
                                    "cost": 450.0, "seats": 2}
    assert validate({**RIDE, "time": ""})[1] == "missing field 'time'"
    assert "int" in validate({**RIDE, "seats": "two"})[1]
    assert validate({**RIDE, "seats": "0"})[1]


def test_new_ride_publishes_to_both_queues_with_same_id():
    ch = FakeChannel()
    r = client(ch).post("/new_ride", data=RIDE)
    assert r.status_code == 202
    task = r.json["task_id"]
    assert [q for q, _, _ in ch.published] == ["ride_match", "database"]
    for _, body, props in ch.published:
        assert props.message_id == task and props.delivery_mode == 2
        assert body["time"] == 3


def test_new_ride_accepts_json():
    ch = FakeChannel()
    assert client(ch).post("/new_ride", json={**RIDE, "time": 5}).status_code == 202


def test_invalid_ride_is_not_published():
    ch = FakeChannel()
    r = client(ch).post("/new_ride", data={"source": "x"})
    assert r.status_code == 400 and ch.published == []


def test_publisher_reconnects_once_then_gives_up():
    ch = FakeChannel(fail_times=1)
    assert client(ch).post("/new_ride", data=RIDE).status_code == 202
    ch = FakeChannel(fail_times=5)
    assert client(ch).post("/new_ride", data=RIDE).status_code == 503


def test_consumer_registration():
    c = client(FakeChannel())
    assert c.post("/new_ride_matching_consumer", data={"consumer_id": "C1"}).status_code == 200
    c.post("/new_ride_matching_consumer", data={"consumer_id": "C1"})   # re-register is idempotent
    assert [x["consumer_id"] for x in c.get("/consumers").json] == ["C1"]
    assert c.post("/new_ride_matching_consumer", data={}).status_code == 400


def delivery(tag=1):
    return SimpleNamespace(delivery_tag=tag), SimpleNamespace(message_id="task_1")


def test_matcher_acks_after_the_ride():
    ch, slept = FakeChannel(), []
    method, props = delivery()
    matcher.make_callback("C1", sleep=slept.append)(ch, method, props, json.dumps(RIDE).encode())
    assert slept == [3.0] and ch.acks == [1] and ch.nacks == []


@pytest.mark.parametrize("body", [b"not json", json.dumps({"source": "x"}).encode(),
                                  json.dumps({**RIDE, "time": "-1"}).encode()])
def test_matcher_dead_letters_bad_messages(body):
    ch = FakeChannel()
    method, props = delivery(7)
    matcher.make_callback("C1", sleep=lambda s: None)(ch, method, props, body)
    assert ch.nacks == [(7, False)] and ch.acks == []


class FakeCollection:
    def __init__(self):
        self.docs = {}

    def replace_one(self, flt, doc, upsert):
        assert upsert
        self.docs[flt["_id"]] = doc


def test_db_writer_is_idempotent_on_redelivery():
    col, ch = FakeCollection(), FakeChannel()
    cb = db_writer.make_callback(col)
    method, props = delivery()
    body = json.dumps({"source": "PES", "time": 3}).encode()
    cb(ch, method, props, body)
    cb(ch, method, props, body)          # redelivered copy
    assert list(col.docs) == ["task_1"] and ch.acks == [1, 1]
