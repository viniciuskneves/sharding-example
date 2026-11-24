#!/usr/bin/env bash
# Demo 1: the write limit. Traffic outgrows a database, and the two usual
# fixes are tried before sharding.
#
# A node can only do so many writes per second. This demo shows what reaching
# that limit looks like from the outside as traffic grows, in three acts:
#
#   1. One node, traffic keeps growing    -> fine, fine, fine, then collapse
#   2. Scale UP: a machine twice as big   -> fixed, until traffic doubles again
#   3. Scale OUT with replicas (mirrors)  -> not fixed at all
#
# A normal node can do 200 writes per second (5 ms per write).
#
# What to look for: below the limit every write is answered in a few
# milliseconds. Above it the node does not just lose the extra writes: a
# queue builds up, everybody waits, and most clients give up.
set -euo pipefail
cd "$(dirname "$0")"

# This demo uses its own compose file, so every compose command needs "-f".
COMPOSE="docker compose -f docker-compose.1-write-limit.yml"

# The router, as published on your machine by the compose file.
ROUTER=http://localhost:8000

# The nodes, by the names they have inside the Compose network.
ONE_NODE=http://shard-a:8000
THREE_NODES=http://shard-a:8000,http://shard-b:8000,http://shard-c:8000

# How long each level of traffic is kept up.
SECONDS_PER_STAGE=5

# Print a bold heading.
say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# start <nodes> [write cost in ms]
# (Re)create the containers with the router pointing at the given nodes, and
# wait until the router answers. The optional second argument changes how
# long a write takes on every node (see x-limits in the compose file).
# Recreating also empties the nodes, so every act starts clean.
start() {
  NODES=$1 WRITE_COST_MS=${2:-5} $COMPOSE up -d --force-recreate >/dev/null 2>&1
  until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done
}

# traffic <writes per second>
# Send writes at a steady rate for SECONDS_PER_STAGE seconds and print one
# line saying how it went.
#
#   -q RATE             send this many requests per second, no faster. This is
#                       the "traffic": clients arrive at their own pace,
#                       whether or not the database keeps up.
#   -n TOTAL            stop after this many requests (rate x seconds)
#   -c 1000             allow up to 1000 requests in flight at the same time,
#                       so slow answers never hold back new requests
#   -t 1s               a client waits 1 second for its answer, then gives up
#   -u ms               print all times in milliseconds
#   --rand-regex-url    user-[0-9]{2} becomes one of 100 keys (user-00 to
#                       user-99). The same keys are updated over and over, so
#                       storage never fills: this demo is about speed only.
#
# oha's report is long, so awk picks three numbers out of it:
#   "[200] 250 responses"   -> writes answered in time
#   "[1602] timeout"        -> writes the client gave up on
#   "50.00% in 7.4 ms"      -> the typical (median) wait of the answered ones
traffic() {
  local rate=$1
  $COMPOSE run --rm oha --no-tui -u ms -q "$rate" -n $((rate * SECONDS_PER_STAGE)) -c 1000 -t 1s \
    -m PUT -d hello --rand-regex-url 'http://router:8000/kv/user-[0-9]{2}' 2>/dev/null |
    awk -v rate="$rate" '
      /\[200\]/     { gsub(/[^0-9 ]/, "", $0); ok = $2 }
      /\] timeout/  { gsub(/[^0-9]/, "", $1); timeouts = $1 }
      /50\.00% in/  { wait = $3 }
      END {
        printf "  %4d writes/s -> %5d answered, %5d gave up after 1 s, typical wait %4.0f ms\n",
               rate, ok, timeouts, wait
      }'
}

# ---------- Act 1 ----------

say "=== Act 1: one node (limit: 200 writes/s), traffic keeps growing ==="
start $ONE_NODE

# 50, 100 and 150 writes/s are under the limit: everything is answered, and
# the wait stays close to the 5 ms a write costs.
#
# 300 writes/s is over the limit. Every second, 300 writes arrive and 200 are
# done, so 100 more are left waiting than the second before. After a couple of
# seconds a new write has more than a second of work ahead of it, and its
# client gives up. Fewer writes get answered in time than at 150 writes/s:
# the node also keeps working through writes whose clients already left.
traffic 50
traffic 100
traffic 150
traffic 300

# ---------- Act 2 ----------

say "=== Act 2: scale UP, one machine twice as fast (limit: 400 writes/s) ==="
start $ONE_NODE 2.5

# 300 writes/s, which broke the node in act 1, is now comfortable.
# Then traffic doubles once more and the same collapse returns at 600.
#
# Scaling up works, but only moves the limit. Each doubling needs a machine
# twice as big, bigger machines cost more than twice as much, and at some
# point there is no bigger machine to buy.
traffic 300
traffic 600

# ---------- Act 3 ----------

say "=== Act 3: scale OUT with replicas, three normal nodes mirrored (limit: still 200 writes/s) ==="
start $THREE_NODES

# Three machines instead of one, yet 300 writes/s collapses exactly as it did
# in act 1. Every node stores every key, so every node must do every write:
# the cluster is as fast as ONE node, however many are added.
traffic 150
traffic 300

# Each of the three nodes was asked to do every write.
say "   Status: who did the work"
printf '  '
curl -s $ROUTER/status

# Stop and remove the containers and the network. Data lives in memory only,
# so nothing is left behind.
say "Cleaning up: docker compose down"
$COMPOSE down >/dev/null 2>&1

say "Done."
