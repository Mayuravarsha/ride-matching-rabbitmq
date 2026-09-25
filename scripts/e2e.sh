#!/usr/bin/env bash
# End-to-end check against the real stack:
#   1. every ride reaches MongoDB exactly once
#   2. rides are spread over the matcher replicas
#   3. a matcher killed mid-ride does not lose it: RabbitMQ redelivers it
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=ridematch_e2e
trap 'docker compose down -v >/dev/null 2>&1 || true' EXIT

docker compose up -d --build --wait
N=9
for i in $(seq 1 $N); do
  curl -sf -X POST localhost:8484/new_ride -d "source=S$i&destination=D$i&time=2&cost=100&seats=1" >/dev/null
done

count() { docker compose exec -T mongodb mongosh --quiet ride_matching --eval 'db.ride_details.countDocuments()'; }
for _ in $(seq 30); do [ "$(count)" -ge $N ] && break; sleep 1; done
stored=$(count)
echo "rides stored: $stored"
[ "$stored" -eq "$N" ]

sleep 8   # let the matchers work through the queue
workers=$(docker compose logs matcher | grep -c "finished task_" || true)
distinct=$(docker compose logs matcher --no-log-prefix | grep "finished task_" | awk '{print $4}' | sort -u | wc -l)
echo "rides finished: $workers by $distinct matchers"
[ "$workers" -eq "$N" ] && [ "$distinct" -ge 2 ]

# Crash test: a 20 s ride, kill whichever matcher took it.
curl -sf -X POST localhost:8484/new_ride -d "source=CRASH&destination=TEST&time=20&cost=1&seats=1" >/dev/null
sleep 3

# Inspect each container's own logs.  Using `docker compose logs` here is
# ambiguous because its service/container prefixes shift the awk fields.
victim=""
while read -r container; do
  [ -n "$container" ] || continue
  if docker logs "$container" 2>&1 | grep -q "matched .*: CRASH"; then
    victim="$container"
    break
  fi
done < <(docker compose ps -q matcher)

if [ -z "$victim" ]; then
  echo "Could not find the matcher processing the CRASH ride" >&2
  docker compose logs --no-color matcher >&2 || true
  exit 1
fi

docker kill "$victim" >/dev/null
echo "killed matcher ${victim:0:12} mid-ride"

# The killed container plus at least one different matcher must have seen the
# ride.  Check containers individually so prefixes cannot corrupt the count.
redelivered=0
for _ in $(seq 40); do
  redelivered=0
  while read -r container; do
    [ -n "$container" ] || continue
    [ "$container" = "$victim" ] && continue
    if docker logs "$container" 2>&1 | grep -q "matched .*: CRASH"; then
      redelivered=1
      break
    fi
  done < <(docker compose ps -q matcher)
  [ "$redelivered" -eq 1 ] && break
  sleep 1
done

[ "$redelivered" -eq 1 ]
echo "CRASH ride was redelivered to a different matcher"
echo "e2e OK"
