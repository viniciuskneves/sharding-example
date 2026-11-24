#!/usr/bin/env bash
# Demo 6: adding a shard. The cluster grows from three shards to four, and
# the keys that are already stored have to end up where the router will look
# for them. How many keys must move depends on how keys are assigned.
#
# The same steps are run twice, with two ways of assigning keys by hash:
#
#   modulo -> hash(key) % number of shards (as in demo 3)
#   ring   -> consistent hashing: each shard owns pieces of a circle, and a
#             new shard only takes over some of those pieces
#
# The steps:
#   1. Store 1800 keys on three shards.
#   2. Add the fourth shard: the router now spreads keys over four.
#      Some keys can no longer be found.
#   3. Rebalance: move every key to the shard that owns it now.
#
# What to look for: the fourth shard should end up with a quarter of the
# keys, so moving a quarter of them is the least that can be done.
#   modulo moves about 75% of ALL keys, most of them between the OLD shards.
#   ring   moves about 25%, and every one of them goes to the new shard.
set -euo pipefail
cd "$(dirname "$0")"

# This demo uses its own compose file, so every compose command needs "-f".
COMPOSE="docker compose -f docker-compose.6-adding-a-shard.yml"

# The router, as published on your machine by the compose file.
ROUTER=http://localhost:8000

# The shards, by the names they have inside the Compose network.
THREE_SHARDS=http://shard-a:8000,http://shard-b:8000,http://shard-c:8000
FOUR_SHARDS=$THREE_SHARDS,http://shard-d:8000

# The keys of this demo: user-0001 to user-1800. curl expands the [...] in a
# URL into one request per number, keeping the leading zeros.
ALL_KEYS="$ROUTER/kv/user-[0001-1800]"

# Print a bold heading.
say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# call <METHOD> <URL> [body]
# Runs one HTTP request and prints: the request, the status code, the response.
call() {
  printf '  %-4s %-26s -> ' "$1" "${2#http://}${3:+  ($3)}"
  curl -s -o /tmp/sharding-demo-body -w '%{http_code}  ' -X "$1" ${3:+-d "$3"} "$2" || printf 'ERR  '
  cat /tmp/sharding-demo-body 2>/dev/null || echo
}

# start <strategy>
# (Re)create ALL containers, with the router using the given strategy over
# three shards, and wait until the router answers. Recreating empties the
# shards, so each run starts clean. shard-d is started too, but the router
# does not know about it yet.
start() {
  STRATEGY=$1 NODES=$THREE_SHARDS $COMPOSE up -d --force-recreate >/dev/null 2>&1
  until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done
}

# add_fourth_shard <strategy>
# Tell the router to spread the keys over four shards instead of three.
#
# "up -d router" without --force-recreate replaces only what changed, and
# only the router's settings did. The shards keep running and keep their
# data. The router can be replaced this freely because it holds nothing: it
# works out the owner of a key from the key (compare demo 5, act 4).
add_fourth_shard() {
  STRATEGY=$1 NODES=$FOUR_SHARDS $COMPOSE up -d router >/dev/null 2>&1
  until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done
}

# Store all 1800 keys through the router.
#   -Z --parallel-max 50   send up to 50 requests at the same time
#   -o /dev/null           throw the responses away
store_all_keys() {
  curl -Z --parallel-max 50 -s -o /dev/null -X PUT -d hello "$ALL_KEYS"
}

# Read all 1800 keys through the router and count the outcomes.
#   -w '%{http_code}\n'    print only the status code of each response
# awk then counts how many were 200 (found) and how many 404 (not found).
read_all_keys() {
  curl -Z --parallel-max 50 -s -o /dev/null -w '%{http_code}\n' "$ALL_KEYS" |
    awk '{ seen[$1]++ }
         END { printf "  read all 1800 keys -> %d found, %d not found (404)\n", seen[200], seen[404] }'
}

experiment() {
  say "=== Shards by $2 ==="
  start "$1"

  # About 600 keys on each of the three shards. All 1800 can be read.
  say "1. Three shards: store 1800 keys"
  store_all_keys
  call GET $ROUTER/status
  read_all_keys

  # Nothing was moved, as the status shows: the three old shards hold exactly
  # what they held, and shard-d is empty. But the router now works out the
  # owner of every key over FOUR shards, and for many keys the answer changed.
  # It looks for them where they are not: 404, although the data is all there.
  #
  # modulo: about 1350 keys not found. hash % 3 and hash % 4 give the same
  #         shard for only a quarter of the keys.
  # ring:   about 450 keys not found: only those in the pieces of the circle
  #         that shard-d took over.
  say "2. Add shard-d: the router now spreads keys over four shards"
  add_fourth_shard "$1"
  call GET $ROUTER/status
  read_all_keys

  # The router lists what every shard holds and moves each key that sits on
  # the wrong shard: copy to the new owner, delete the old copy.
  #
  # modulo: "movedPercent" about 75. Three quarters of everything stored
  #         has to be copied, to gain a quarter more room.
  # ring:   "movedPercent" about 25, the least possible.
  #
  # "tookSeconds" is the time the shards spent on those copies and deletes.
  # They go through the same write queues as the application's own writes,
  # so in a live system a rebalance competes with the traffic.
  say "3. Rebalance: move every key to the shard that owns it now"
  call POST $ROUTER/rebalance

  # All 1800 keys can be read again, and every shard holds about 450.
  #
  # "writesHandled" shows who paid for the move (compare with step 2):
  # modulo: every OLD shard did hundreds of extra writes, because most keys
  #         moved from one old shard to another.
  # ring:   the old shards only did deletes for the keys they gave away, and
  #         everything that moved went to shard-d.
  say "   Afterwards"
  call GET $ROUTER/status
  read_all_keys
}

experiment modulo "HASH, modulo"
experiment ring "HASH, ring (consistent hashing)"

# Stop and remove the containers and the network. Data lives in memory only,
# so nothing is left behind.
say "Cleaning up: docker compose down"
$COMPOSE down >/dev/null 2>&1

say "Done."
