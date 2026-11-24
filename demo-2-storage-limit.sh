#!/usr/bin/env bash
# Demo 2: the storage limit. Data outgrows a database, and adding copies
# does not make room.
#
# Demo 1 was about how FAST a node can write. This one is about how MUCH it
# can hold. The same load (3000 writes, each with a new key) is sent twice:
#
#   1. to ONE node
#   2. to THREE nodes that mirror each other (replication, as in the CAP demo)
#
# Each node has room for 1000 keys and can do 200 writes per second.
#
# What to look for: both runs give the SAME numbers. Three nodes hold no more
# than one, because every node stores every key. That gap, together with the
# write limit from demo 1, is what sharding (demo 3) is for.
set -euo pipefail
cd "$(dirname "$0")"

# This demo uses its own compose file, so every compose command needs "-f".
COMPOSE="docker compose -f docker-compose.2-storage-limit.yml"

# The router, as published on your machine by the compose file.
ROUTER=http://localhost:8000

# The nodes, by the names they have inside the Compose network.
ONE_NODE=http://shard-a:8000
THREE_NODES=http://shard-a:8000,http://shard-b:8000,http://shard-c:8000

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
# wait until the router answers. Recreating also wipes the nodes' in-memory
# data, so each experiment starts empty.
start() {
  NODES=$1 $COMPOSE up -d --force-recreate >/dev/null 2>&1
  until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done
}

# Send 3000 writes through the router with oha, 50 at a time.
#
#   --no-tui            plain text output instead of the live dashboard
#   -n 3000             total number of requests
#   -c 50               how many requests are in flight at the same time
#   -m PUT -d hello     each request stores the value "hello"
#   --rand-regex-url    build every URL from a pattern: [a-z]{12} becomes 12
#                       random letters, so every request writes a NEW key
#
# oha runs in a container on the Compose network, hence "router" and not
# "localhost". Its report is long, so only two parts are shown: the
# throughput and how many responses had each status code. Run the command
# without the grep to see the full report, including latencies.
load() {
  $COMPOSE run --rm oha --no-tui -n 3000 -c 50 -m PUT -d hello \
    --rand-regex-url 'http://router:8000/kv/[a-z]{12}' 2>/dev/null |
    grep -E 'Requests/sec|responses'
}

experiment() {
  say "=== $1 ==="
  start "$2"

  # A write and a read, to see who stores a key and who answers for it.
  say "1. One write and one read"
  call PUT $ROUTER/kv/greeting hello
  call GET $ROUTER/kv/greeting

  # The node(s) start with 1 key (the greeting) and room for 1000.
  say "   Status before the load"
  call GET $ROUTER/status

  # Takes about 15 seconds: 3000 writes at 200 per second.
  #
  # [200]: 999 writes fit (the greeting took the first of the 1000 places).
  # [507]: the other 2001 were refused, the storage is full.
  # Requests/sec: about 200 in BOTH experiments, the write limit from demo 1.
  #   Nothing times out here, unlike in demo 1: oha sends the next write only
  #   when an earlier one is answered, so it never gets ahead of the node.
  say "2. Load: 3000 writes with new keys"
  load

  # One node:    1000/1000 keys, 3001 writes handled.
  # Three nodes: EACH has 1000/1000 keys and handled 3001 writes. The same
  #              1000 keys three times over, and three times the total work.
  say "   Status after the load"
  call GET $ROUTER/status

  # Refused with 507, whichever experiment this is.
  say "3. One more new key"
  call PUT $ROUTER/kv/one-more hello
}

experiment "One node" $ONE_NODE
experiment "Three nodes, mirrored" $THREE_NODES

# Stop and remove the containers and the network. Data lives in memory only,
# so nothing is left behind.
say "Cleaning up: docker compose down"
$COMPOSE down >/dev/null 2>&1

say "Done."
