# Ride Matching over RabbitMQ

A containerised ride-hailing backend built for the Cloud Computing
hackathon at PES University. A ride request is published to two
RabbitMQ queues:
- matching workers take rides from one queue and can be scaled out
- a separate writer saves every ride to MongoDB from the other

A slow database write never delays a match, and a worker that crashes
mid-ride never loses the ride.

```
                       ┌──────────────► ride_match queue ──► matcher ×N  (prefetch 1, ack after the ride)
 POST /new_ride ─► Flask producer                                   │ reject
   (validates,         │  same task id, persistent, confirmed       ▼
    202 + task id)     └──────────────► database queue  ──► db_writer ──► MongoDB (upsert by task id)
                                                                    │ reject
                                                                    ▼
                                                            rejected (dead-letter queue)
```

## Delivery guarantees

| Guarantee | How |
|---|---|
| A ride is never lost once the API returns 202 | durable queues, persistent messages, **publisher confirms** before replying |
| A worker crash never drops a ride | manual acks sent only **after** the ride finishes, `prefetch_count=1`, so unacked rides are redelivered to another worker |
| Redelivery never creates duplicates | the MongoDB writer **upserts by task id**, so it's idempotent |
| A malformed message can't take the system down | it is rejected to a **dead-letter queue** instead of crashing workers in a redelivery loop |
| Matching throughput scales | `docker compose up --scale matcher=N`; RabbitMQ load-balances across workers |

These are tested end to end: `scripts/e2e.sh` starts the stack, sends 9
rides and checks that each was stored exactly once and that the rides were
spread across the matchers. It then **kills a matcher in the middle of a
ride** and checks that another matcher picks the ride up.

```
$ ./scripts/e2e.sh
rides stored: 9
rides finished: 9 by 3 matchers
killed matcher bfba3de37376 mid-ride
CRASH ride picked up by 2 matchers
e2e OK
```

## Run it

```bash
docker compose up --build              # producer on :8484, RabbitMQ UI on :15672 (guest/guest)
docker compose up --build --scale matcher=5

curl -X POST localhost:8484/new_ride \
     -d "source=PES&destination=Airport&time=5&cost=450&seats=2"
# {"task_id": "task_3f2c..."}

curl localhost:8484/consumers          # registered matching workers
```

`time` is the simulated ride length in seconds. A matcher is busy for that
long before it acknowledges the ride.

## API

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/new_ride` | form or JSON: `source`, `destination`, `time`, `cost`, `seats` | `202 {"task_id"}`, `400` invalid, `503` broker down |
| POST | `/new_ride_matching_consumer` | `consumer_id` | worker registration |
| GET | `/consumers` | | registered workers |
| GET | `/health` | | used by the compose health check |

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest                  # unit tests with fake channels, no broker needed
./scripts/e2e.sh        # full stack in Docker, including the crash test
```

GitHub Actions runs both on every push.

## Layout

```
ridematch/mq.py          connection with retry, durable queues + dead-letter queue
ridematch/producer.py    Flask API with publisher confirms
ridematch/matcher.py     matching worker
ridematch/db_writer.py   idempotent MongoDB writer
docker-compose.yaml      RabbitMQ, MongoDB, producer, 3 matchers, writer (with health checks)
tests/                   unit tests
scripts/e2e.sh           end-to-end and crash test
```

## What changed from the hackathon version

The original code is in the first commit of this repository. Fixes since then:

* **Redelivered messages crashed the database writer forever.**
  `insert_one` raised `DuplicateKeyError` on the second copy, and the
  message was redelivered again after every restart. Writes are now upserts.
* **A ride without a `time` field crashed every matcher in turn.** Each one
  hit a `KeyError`, died, and the message went to the next worker. Bad
  messages are now dead-lettered.
* The producer replied before RabbitMQ had confirmed the message, and
  opened a new connection for every request. It now keeps one confirmed
  channel and returns the task id.
* Fixed `sleep(10)` start-up waits are replaced with connection retries and
  compose health checks. Images are pinned, and the matchers are one
  scalable service instead of three copied blocks.
