#!/usr/bin/env bash
# Demo 8: replicated shards. Sharding and replication together: the keys are
# split over three shards, and every shard is TWO nodes holding the same keys.
#
# Demo 7 ended with a dead shard taking its third of the data with it.
# Replicas are the answer to that, and they bring the CAP example's questions
# back, once for every shard.
#
# Six nodes, each with room for 1000 keys and 200 writes/s:
#   shard a: shard-a1, shard-a2
#   shard b: shard-b1, shard-b2
#   shard c: shard-c1, shard-c2
#
#   Act 1, all healthy:     twice the machines of demo 3, the same capacity.
#   Act 2, a replica dies:  nothing is lost, reads and writes go on.
#   Act 3, it comes back:   empty and behind. It gives wrong answers until
#                           it is repaired from the other replica.
#   Act 4, strict writes:   the other way to handle a dead replica: refuse
#                           writes to its shard, so replicas never disagree.
#
# What to look for: shards give capacity, replicas give safety, and a real
# cluster needs both. The number of shards decides how much it can do; the
# replicas decide what happens when a node fails.
set -euo pipefail
cd "$(dirname "$0")"

# This demo uses its own compose file, so every compose command needs "-f".
COMPOSE="docker compose -f docker-compose.8-replicated-shards.yml"

# The router, as published on your machine by the compose file.
ROUTER=http://localhost:8000

# The users of this demo: user-0001 to user-1800. curl expands the [...] in a
# URL into one request per number, keeping the leading zeros.
ALL_USERS="$ROUTER/kv/user-[0001-1800]"

# Two keys whose homes are known (the hash decides, and never changes):
#   user-0002 lives on shard b (shard-b1 and shard-b2)
#   user-0001 lives on shard c (shard-c1 and shard-c2)

# How long each run of traffic is kept up.
SECONDS_PER_STAGE=5

# Print a bold heading.
say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# call <METHOD> <URL> [body]
# Runs one HTTP request and prints: the request, the status code, the response.
call() {
  printf '  %-4s %-36s -> ' "$1" "${2#http://}${3:+  ($3)}"
  curl -s -o /tmp/sharding-demo-body -w '%{http_code}  ' -X "$1" ${3:+-d "$3"} "$2" || printf 'ERR  '
  cat /tmp/sharding-demo-body 2>/dev/null || echo
}

# Print how many keys every node holds, on one line.
# The router's /status has this, but for six nodes its JSON is hard to read.
# grep -o pulls out the pieces  "shard-a1":{"keys":558  (or ...{"error  for a
# node that is down), sed tidies them up, paste puts them on one line.
keys_per_node() {
  printf '  '
  curl -s $ROUTER/status |
    grep -o '"shard-[a-z0-9]*":{"\(keys":[0-9]*\|error\)' |
    sed 's/"//g; s/:{keys:/: /; s/:{error/: DOWN/' |
    paste -sd, - | sed 's/,/   /g'
}

# Read all 1800 users through the router, 50 at a time, and count the
# outcomes: 200 is found, 404 is "no such key", 502 is "nobody could answer".
read_all_users() {
  curl -Z --parallel-max 50 -s -o /dev/null -w '%{http_code}\n' "$ALL_USERS" |
    awk '{ seen[$1]++ }
         END { printf "  read all 1800 users -> %d found, %d not found (404), %d failed (502)\n",
                      seen[200], seen[404], seen[502] }'
}

# traffic <writes per second>
# Writes at a steady rate for SECONDS_PER_STAGE seconds, to 900 of the users
# that are already stored. A client gives up after 1 second. Prints one line
# saying how it went. (demo-1-write-limit.sh explains every oha option.)
traffic() {
  local rate=$1
  $COMPOSE run --rm oha --no-tui -u ms -q "$rate" -n $((rate * SECONDS_PER_STAGE)) -c 1000 -t 1s \
    -m PUT -d hello --rand-regex-url 'http://router:8000/kv/user-0[1-9][0-9]{2}' 2>/dev/null |
    awk -v rate="$rate" '
      /\[200\]/     { gsub(/[^0-9 ]/, "", $0); ok = $2 }
      /\] timeout/  { gsub(/[^0-9]/, "", $1); timeouts = $1 }
      /50\.00% in/  { wait = $3 }
      END {
        printf "  %4d writes/s -> %5d answered, %5d gave up after 1 s, typical wait %4.0f ms\n",
               rate, ok, timeouts, wait
      }'
}

# Wait until the router answers.
wait_for_router() { until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done; }

# ---------- Act 1 ----------

say "=== Act 1: all six nodes healthy ==="
$COMPOSE up -d --force-recreate >/dev/null 2>&1
wait_for_router

# The two replicas of a shard hold exactly the same number of keys, and the
# three shards hold about 600 each: 1800 keys, stored 3600 times.
say "1. Store 1800 users"
curl -Z --parallel-max 50 -s -o /dev/null -X PUT -d hello "$ALL_USERS"
keys_per_node

# "storedOn" names both replicas of the key's shard.
say "2. One write"
call PUT $ROUTER/kv/user-0002 hello

# 500 writes/s: all answered, as with the three plain shards of demo 3.
#
# 900 writes/s: collapse. Six nodes that could each do 200 writes/s do NOT
# add up to 1200. Both replicas of a shard do every write of that shard, so
# a shard is as fast as one node (demo 1, act 3), and the cluster is as fast
# as its three SHARDS: about 600. The same goes for room: 3000 keys, not 6000.
#
# The replicas cost three extra machines and add no capacity at all. What
# they buy shows in the next act.
say "3. Traffic"
traffic 500
traffic 900

# ---------- Act 2 ----------

say "=== Act 2: a replica dies (shard-b1 is stopped) ==="
$COMPOSE stop shard-b1 >/dev/null 2>&1

# shard-b1 is down; shard-b2 has everything that shard b held.
say "1. Keys per node"
keys_per_node

# All 1800 found. For a key of shard b the router asks shard-b1, gets no
# answer, and asks shard-b2 instead.
# In demo 7, with one node per shard, the same failure cost 639 users.
say "2. All users"
read_all_users

# Still accepted: "storedOn" shard-b2, "missed" shard-b1.
# The write is safe on one replica, and the cluster stays fully available.
# The price: shard-b1 does not have it, and falls a little further behind
# with every write it misses.
say "3. A write to the shard with the dead replica (user-0002)"
call PUT $ROUTER/kv/user-0002 hello

# ---------- Act 3 ----------

say "=== Act 3: the replica comes back (shard-b1 is started) ==="
$COMPOSE start shard-b1 >/dev/null 2>&1
until curl -sf http://localhost:8003/status >/dev/null; do sleep 0.3; done

# shard-b1 is up, with 0 keys. The nodes keep their data in memory, so it
# lost everything. Even a node that kept its disk would be missing the
# writes made while it was away.
say "1. Keys per node"
keys_per_node

# 639 users "not found". Nothing is lost this time: shard-b2 has them all.
# But the router asks shard-b1 first, shard-b1 is alive, and it truthfully
# says it has no such key. A replica that is back but BEHIND is worse than a
# dead one: it answers, and the answers are wrong.
say "2. All users"
read_all_users

# The router compares the replicas of every shard and copies over what one
# has and the other lacks: 639 keys to shard-b1.
#
# "tookSeconds": about 3. The copies are writes like any other and wait in
# shard-b1's queue (639 writes at 200 per second).
say "3. Repair: copy to every replica the keys it is missing"
call POST $ROUTER/repair

# Both replicas of shard b hold 639 keys again, and all 1800 users are found.
say "   Afterwards"
keys_per_node
read_all_users

# ---------- Act 4 ----------

# Act 2 accepted a write that only one of two replicas stored. That kept
# the cluster available and let the replicas drift apart.
#
# The other choice: a write only counts if EVERY replica of the shard stored
# it. Only the router is replaced here; the nodes keep running and keep
# their data.

say "=== Act 4: strict writes (every replica must store a write), then shard-b1 dies again ==="
WRITES_NEED=all $COMPOSE up -d router >/dev/null 2>&1
wait_for_router
$COMPOSE stop shard-b1 >/dev/null 2>&1

# 503, refused: shard-b1 cannot store it, so the write does not count.
# The two replicas of shard b can never disagree this way, but one dead node
# has made a third of the keys read-only.
say "1. A write to the shard with the dead replica (user-0002)"
call PUT $ROUTER/kv/user-0002 hello

# Accepted, on both replicas. The other shards are not affected: the choice
# between staying available and staying consistent is made shard by shard.
say "2. A write to another shard (user-0001)"
call PUT $ROUTER/kv/user-0001 hello

# All 1800 found. Reads do not need both replicas.
say "3. All users"
read_all_users

# Accept the write and let the replicas disagree (act 2), or refuse it and
# stay consistent (act 4): that is the AP-or-CP choice of the CAP example,
# here inside one shard. The way to get both for a single failure is the one
# the CAP example ends with: three replicas per shard, and a write counts
# when a majority of them has it.

# Stop and remove the containers and the network. Data lives in memory only,
# so nothing is left behind.
say "Cleaning up: docker compose down"
$COMPOSE down >/dev/null 2>&1

say "Done."
