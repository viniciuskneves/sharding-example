#!/usr/bin/env bash
# Demo 4: the key choice matters. Sharding only helps if the work actually
# spreads over the shards, and HOW keys are assigned decides whether it does.
#
# Three shards throughout, each with room for 1000 keys and 200 writes/s, so
# the cluster could do close to 600 writes/s (demo 3). The keys are users,
# user-0000 to user-2999. Two ways of assigning them are compared:
#
#   range  -> by the key's place in order: shard-a has user-0000 to user-0999,
#             shard-b has user-1000 to user-1999, shard-c has user-2000 and up
#   modulo -> by the key's hash, as in demo 3
#
#   Act 1, range:   writes to the NEWEST users all hit one shard -> collapse.
#                   But a scan over neighbouring keys asks just one shard.
#   Act 2, modulo:  the same writes spread out -> fine.
#                   But the same scan now has to ask every shard.
#   Act 3, one hot key: 80% of the writes go to a single user -> collapse,
#                   whichever of the two strategies is used.
#
# What to look for: in every collapse, two of the three shards are almost
# idle. The cluster has the capacity; the keys just do not let it be used.
set -euo pipefail
cd "$(dirname "$0")"

# This demo uses its own compose file, so every compose command needs "-f".
COMPOSE="docker compose -f docker-compose.4-key-choice.yml"

# The router, as published on your machine by the compose file.
ROUTER=http://localhost:8000

# The traffic in every act: 500 writes per second for 5 seconds.
# One node can do 200 writes/s, three shards together close to 600.
RATE=500
SECONDS_PER_STAGE=5

# Key patterns for oha. Each write picks a key at random from the pattern.
#
# The newest users: user-2000 to user-2999. Applications mostly write to
# their most recent records (new sign-ups, today's orders, the latest
# events), and with keys handed out in order those are all neighbours.
NEWEST_USERS='user-2[0-9]{3}'
#
# One very popular user. Each write picks one of the five alternatives
# between the "|" signs with equal chance: four of them are user-1500, the
# fifth is any user. So about 80% of the writes go to user-1500.
ONE_HOT_USER='user-(1500|1500|1500|1500|[0-2][0-9]{3})'

# Print a bold heading.
say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# call <METHOD> <URL> [body]
# Runs one HTTP request and prints: the request, the status code, the response.
call() {
  printf '  %-4s %-48s -> ' "$1" "${2#http://}${3:+  ($3)}"
  curl -s -o /tmp/sharding-demo-body -w '%{http_code}  ' -X "$1" ${3:+-d "$3"} "$2" || printf 'ERR  '
  cat /tmp/sharding-demo-body 2>/dev/null || echo
}

# start <strategy>
# (Re)create the containers with the router using the given strategy, and
# wait until the router answers. Recreating also empties the shards, so every
# experiment starts clean.
start() {
  STRATEGY=$1 $COMPOSE up -d --force-recreate >/dev/null 2>&1
  until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done
}

# traffic <key pattern>
# Writes at a steady RATE for SECONDS_PER_STAGE seconds, to keys picked from
# the pattern. A client gives up after 1 second. Prints one line saying how
# it went. (demo-1-write-limit.sh explains every oha option used here.)
traffic() {
  $COMPOSE run --rm oha --no-tui -u ms -q $RATE -n $((RATE * SECONDS_PER_STAGE)) -c 1000 -t 1s \
    -m PUT -d hello --rand-regex-url "http://router:8000/kv/$1" 2>/dev/null |
    awk -v rate=$RATE '
      /\[200\]/     { gsub(/[^0-9 ]/, "", $0); ok = $2 }
      /\] timeout/  { gsub(/[^0-9]/, "", $1); timeouts = $1 }
      /50\.00% in/  { wait = $3 }
      END {
        printf "  %4d writes/s -> %5d answered, %5d gave up after 1 s, typical wait %4.0f ms\n",
               rate, ok, timeouts, wait
      }'
}

# Store user-1000 to user-1099 (100 neighbouring keys), then count the keys
# in exactly that stretch with a scan.
#
# curl expands [1000-1099] in a URL into 100 requests, one per number.
# "-o /dev/null" throws the 100 responses away.
scan_neighbours() {
  curl -s -o /dev/null -X PUT -d hello "$ROUTER/kv/user-[1000-1099]"
  call GET "$ROUTER/scan?from=user-1000&to=user-1099"
}

# ---------- Act 1 ----------

say "=== Act 1: shards by RANGE (shard-a: user-0xxx, shard-b: user-1xxx, shard-c: user-2xxx) ==="
start range

# Collapse, at a rate the cluster as a whole could easily take. Every one of
# these keys belongs to shard-c, so shard-c alone gets 500 writes/s and it
# can do 200. This is one node's limit all over again (demo 1), with two
# more nodes standing by.
say "1. Traffic: writes to the newest users (user-2000 to user-2999)"
traffic "$NEWEST_USERS"

# shard-a and shard-b: 0 keys, 0 writes handled.
# shard-c: all of it. (Its count is still rising: it keeps working through
# queued writes whose clients gave up long ago.)
say "   Status: who did the work"
call GET $ROUTER/status

# The upside of range: neighbouring keys live together.
# "nodesAsked": 1. Only shard-b can have keys between user-1000 and
# user-1099, so the router asks nobody else.
say "2. Scan: count the users from user-1000 to user-1099"
scan_neighbours

# ---------- Act 2 ----------

say "=== Act 2: shards by HASH (modulo) ==="
start modulo

# Everything is answered. The hash scatters these neighbouring keys over all
# three shards, so each gets about a third of the 500 writes/s.
say "1. Traffic: the same writes to the newest users"
traffic "$NEWEST_USERS"

# About a third of the keys and of the writes on each shard.
say "   Status: who did the work"
call GET $ROUTER/status

# The downside of hashing: neighbouring keys do NOT live together.
# "nodesAsked": 3. The 100 users are spread over all the shards, so the
# router has to ask every one of them and add up the answers. With 100 shards
# it would be 100 requests for the same small scan, and the scan would be as
# slow as the slowest shard.
say "2. Scan: the same count"
scan_neighbours

# ---------- Act 3 ----------

# A key lives on exactly one shard, however shards are chosen. If most of
# the traffic is for ONE key, most of the traffic is for one shard.
#
# In both runs below, the shard that owns user-1500 gets well over 400 of the
# 500 writes/s and collapses, while the other two handle about 35 each.
#
# About 700 writes are still answered, and those quickly: they are mostly the
# writes to OTHER users, which land on the two healthy shards. So the "typical
# wait" looks fine here. It is the popular user that nobody can write to.

say "=== Act 3: one hot key (80% of the writes go to user-1500), shards by RANGE ==="
start range
traffic "$ONE_HOT_USER"
# shard-b owns user-1500 (it is in user-1000 to user-1999).
say "   Status: who did the work"
call GET $ROUTER/status

say "=== Act 3: one hot key (80% of the writes go to user-1500), shards by HASH ==="
start modulo
traffic "$ONE_HOT_USER"
# A different shard owns user-1500 now (wherever its hash put it), with the
# same outcome.
say "   Status: who did the work"
call GET $ROUTER/status

# Stop and remove the containers and the network. Data lives in memory only,
# so nothing is left behind.
say "Cleaning up: docker compose down"
$COMPOSE down >/dev/null 2>&1

say "Done."
