#!/usr/bin/env bash
# Demo 3: sharding. The two limits from demos 1 and 2 are lifted by giving
# each key to ONE node instead of copying every key to all of them.
#
# The nodes are exactly the same as before: room for 1000 keys and 200 writes
# per second each. Only the router changed. Its "modulo" strategy picks the
# node for a key with:  hash(key) % number of nodes
#
# Each half of the demo runs a load that FAILED earlier, first on one node
# (to see it fail again) and then on three shards:
#
#   Part 1, write speed (demo 1):  300 writes/s collapsed one node
#   Part 2, storage     (demo 2):  3000 new keys, one node refused 2000
#
# What to look for: three shards take about three times the writes and hold
# about three times the keys. "About", because the keys are spread almost
# evenly, not perfectly: the fullest or busiest shard reaches its own limit
# a little before the others do.
set -euo pipefail
cd "$(dirname "$0")"

# This demo uses its own compose file, so every compose command needs "-f".
COMPOSE="docker compose -f docker-compose.3-sharding.yml"

# The router, as published on your machine by the compose file.
ROUTER=http://localhost:8000

# The nodes, by the names they have inside the Compose network.
ONE_NODE=http://shard-a:8000
THREE_SHARDS=http://shard-a:8000,http://shard-b:8000,http://shard-c:8000

# How long each level of traffic is kept up.
SECONDS_PER_STAGE=5

# Print a bold heading.
say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# call <METHOD> <URL> [body]
# Runs one HTTP request and prints: the request, the status code, the response.
call() {
  printf '  %-4s %-32s -> ' "$1" "${2#http://}${3:+  ($3)}"
  curl -s -o /tmp/sharding-demo-body -w '%{http_code}  ' -X "$1" ${3:+-d "$3"} "$2" || printf 'ERR  '
  cat /tmp/sharding-demo-body 2>/dev/null || echo
}

# (Re)create the containers with the router pointing at the given nodes, and
# wait until the router answers. Recreating also empties the nodes, so every
# experiment starts clean.
start() {
  NODES=$1 $COMPOSE up -d --force-recreate >/dev/null 2>&1
  until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done
}

# traffic <writes per second>
# The load from demo 1: writes at a steady rate for SECONDS_PER_STAGE seconds,
# where a client gives up after 1 second. Prints one line saying how it went.
# (demo-1-write-limit.sh explains every oha option used here.)
#
# One difference from demo 1: the writes go to 900 different keys
# (user-100 to user-999) instead of 100. Which shard a key lands on is fixed
# by its hash, and with only 100 keys one shard could easily own 40 of them
# and get 40% of the traffic. More keys make for a fairer split.
traffic() {
  local rate=$1
  $COMPOSE run --rm oha --no-tui -u ms -q "$rate" -n $((rate * SECONDS_PER_STAGE)) -c 1000 -t 1s \
    -m PUT -d hello --rand-regex-url 'http://router:8000/kv/user-[1-9][0-9]{2}' 2>/dev/null |
    awk -v rate="$rate" '
      /\[200\]/     { gsub(/[^0-9 ]/, "", $0); ok = $2 }
      /\] timeout/  { gsub(/[^0-9]/, "", $1); timeouts = $1 }
      /50\.00% in/  { wait = $3 }
      END {
        printf "  %4d writes/s -> %5d answered, %5d gave up after 1 s, typical wait %4.0f ms\n",
               rate, ok, timeouts, wait
      }'
}

# The load from demo 2: 3000 writes, each with a new key, sent as fast as the
# cluster answers them (50 at a time). Prints the throughput and how many
# responses had each status code.
# (demo-2-storage-limit.sh explains every oha option used here.)
fill() {
  $COMPOSE run --rm oha --no-tui -n 3000 -c 50 -m PUT -d hello \
    --rand-regex-url 'http://router:8000/kv/[a-z]{12}' 2>/dev/null |
    grep -E 'Requests/sec|responses'
}

# ---------- Part 1: write speed ----------

say "=== Part 1: write speed. BEFORE, one node (limit: 200 writes/s) ==="
start $ONE_NODE

# The failure from demo 1, act 1: over the limit, most clients give up.
traffic 300

say "=== Part 1: write speed. AFTER, three shards (200 writes/s each) ==="
start $THREE_SHARDS

# 300 writes/s: each shard gets roughly 100 of them, half of what it can do.
# Everything is answered within a few milliseconds.
#
# 500 writes/s: two and a half times what a single node can do, and still
# everything is answered. The busiest shard is now close to its own limit of
# 200, so the typical wait creeps up a little.
traffic 300
traffic 500

# "writesHandled" shows the work divided between the shards: roughly a third
# each, and the three numbers add up to the 4000 writes that were sent.
# Compare with mirroring in demo 1, where EVERY node handled every write.
# "keys" shows the keys divided the same way, not quite evenly. (They add up
# to a little under 900: the keys are picked at random, so a few of the 900
# possible ones were never written.)
say "   Status: who did the work"
call GET $ROUTER/status

# ---------- Part 2: storage ----------

say "=== Part 2: storage. BEFORE, one node (room for 1000 keys) ==="
start $ONE_NODE

# The failure from demo 2: 1000 keys fit and 2000 are refused with 507.
# Takes about 15 seconds, at the 200 writes/s of one node.
say "   Load: 3000 writes with new keys"
fill

say "=== Part 2: storage. AFTER, three shards (room for 1000 keys each) ==="
start $THREE_SHARDS

# The response says which shard owns the key ("node"). The read goes to the
# same shard, because the router computes the owner the same way every time.
say "1. One write and one read"
call PUT $ROUTER/kv/greeting hello
call GET $ROUTER/kv/greeting

# Takes about 5 seconds instead of 15.
#
# Requests/sec: close to 600, three times one node. This load is sent as fast
#   as the cluster answers, so the shards working side by side shows directly.
# [200]: nearly all of the 3000 writes fit.
# [507]: a few dozen are still refused. The keys are random, so the split is
#   not exactly 1000 / 1000 / 1000: one shard is offered a few more than it
#   has room for and refuses them, while another still has places left.
#   The exact numbers change from run to run.
say "2. Load: 3000 writes with new keys"
fill

# At least one shard is completely full (1000/1000) and at least one is not.
# That is the price of spreading keys by hash: you cannot use every last
# place, so the usable room is a little under 3 x 1000.
say "   Status after the load"
call GET $ROUTER/status

# Stop and remove the containers and the network. Data lives in memory only,
# so nothing is left behind.
say "Cleaning up: docker compose down"
$COMPOSE down >/dev/null 2>&1

say "Done."
