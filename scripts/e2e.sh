#!/usr/bin/env bash
# End-to-end check against the real stack:
#   1. every ride reaches MongoDB exactly once
#   2. rides are spread over the matcher replicas
#   3. a matcher killed mid-ride does not lose it: RabbitMQ redelivers it
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=ridematch_e2e
trap 'docker compose down -v >/dev/null 2>&1' EXIT

docker compose up -d --build --wait
N=9
for i in $(seq 1 $N); do
  curl -sf -X POST localhost:8484/new_ride -d "source=S$i&destination=D$i&time=2&cost=100&seats=1" >/dev/null
done

count() { docker compose exec -T mongodb mongosh --quiet ride_matching --eval 'db.ride_details.countDocuments()'; }
for _ in $(seq 30); do [ "$(count)" -ge $N ] && break; sleep 1; done
echo "rides stored: $(count)"; [ "$(count)" -eq $N ]

sleep 8   # let the matchers work through the queue
workers=$(docker compose logs matcher | grep -c "finished task_" || true)
distinct=$(docker compose logs matcher --no-log-prefix | grep "finished task_" | awk '{print $4}' | sort -u | wc -l)
echo "rides finished: $workers by $distinct matchers"; [ "$workers" -eq $N ] && [ "$distinct" -ge 2 ]

# crash test: a 20 s ride, kill whichever matcher took it
curl -sf -X POST localhost:8484/new_ride -d "source=CRASH&destination=TEST&time=20&cost=1&seats=1" >/dev/null
sleep 3
victim=$(docker compose ps -q matcher | while read -r c; do
  [ "$(docker logs "$c" 2>&1 | grep -c "matched .*CRASH")" -gt 0 ] && echo "$c"; done | head -1)
docker kill "$victim" >/dev/null
echo "killed matcher ${victim:0:12} mid-ride"
for _ in $(seq 40); do
  [ "$(docker compose logs matcher | grep "matched .*CRASH" | grep -vc "${victim:0:12}")" -gt 0 ] && break; sleep 1
done
docker compose logs matcher --no-log-prefix | grep "CRASH" | awk '{print $4}' | sort -u | wc -l | \
  { read -r n; echo "CRASH ride picked up by $n matchers"; [ "$n" -ge 2 ]; }
echo "e2e OK"
