#!/usr/bin/env bash
# Demo 7: across shards, and when one fails. Anything that touches a single
# key involves a single shard. This demo is about everything else: questions
# and writes that span several shards, and what a slow or dead shard does to
# them.
#
# Three shards with keys assigned by hash, as in demo 3, holding 1800 users
# and two accounts.
#
#   Act 1, all healthy:   a count over all shards and a write to two shards
#                         both work.
#   Act 2, a SLOW shard:  keys on the other shards are as fast as ever.
#                         Anything that asks every shard is as slow as the
#                         slowest one.
#   Act 3, a DEAD shard:  a third of the keys fail, two thirds keep working.
#                         The count can refuse, or answer with less.
#   Act 4, a write across two shards, one of them dead: half of it happens.
#   Act 5, the shard comes back: empty. Its third of the data is gone.
#
# What to look for: sharding limits the damage of a failure to one shard's
# share of the keys, which a single database cannot do. But whatever needs
# ALL the shards is only as fast, and only as available, as the worst one.
set -euo pipefail
cd "$(dirname "$0")"

# This demo uses its own compose file, so every compose command needs "-f".
COMPOSE="docker compose -f docker-compose.7-across-shards.yml"

# The router, as published on your machine by the compose file.
ROUTER=http://localhost:8000

# shard-c, reached directly on its own port. Only used to flip its "slow"
# switch, which is not something that should go through the router.
SHARD_C=http://localhost:8003

# The users of this demo: user-0001 to user-1800. curl expands the [...] in a
# URL into one request per number, keeping the leading zeros.
ALL_USERS="$ROUTER/kv/user-[0001-1800]"

# Three keys whose homes are known (the hash decides, and never changes):
#   user-0008 lives on shard-a
#   user-0002 lives on shard-b
#   user-0001 lives on shard-c

# Print a bold heading.
say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# call <METHOD> <URL> [body]
# Runs one HTTP request and prints: the request, the status code, how long
# the answer took, and the response.
call() {
  printf '  %-4s %-36s -> ' "$1" "${2#http://}${3:+  ($3)}"
  curl -s -o /tmp/sharding-demo-body -w '%{http_code} in %{time_total}s  ' -X "$1" ${3:+-d "$3"} "$2" || printf 'ERR  '
  cat /tmp/sharding-demo-body 2>/dev/null || echo
}

# Read all 1800 users through the router, 50 at a time, and count the
# outcomes: 200 is found, 404 is "no such key", 502 is "its shard is down".
read_all_users() {
  curl -Z --parallel-max 50 -s -o /dev/null -w '%{http_code}\n' "$ALL_USERS" |
    awk '{ seen[$1]++ }
         END { printf "  read all 1800 users -> %d found, %d not found (404), %d failed, shard down (502)\n",
                      seen[200], seen[404], seen[502] }'
}

# ---------- Act 1 ----------

say "=== Act 1: all three shards healthy ==="
$COMPOSE up -d --force-recreate >/dev/null 2>&1
until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done

# About 600 users on each shard.
say "1. Store 1800 users"
curl -Z --parallel-max 50 -s -o /dev/null -X PUT -d hello "$ALL_USERS"
call GET $ROUTER/status

# One request that writes TWO keys: both accounts start with 100.
# "written" shows they live on different shards (alice on shard-b, bob on
# shard-c), so the router had to make two separate writes.
say "2. A write to two shards: open two accounts with 100 each"
call PUT $ROUTER/kv-many '{"account-alice":"100","account-bob":"100"}'

# A single key: one shard is asked, the others are not involved.
say "3. A read of one key"
call GET $ROUTER/kv/user-0008

# No shard knows the total. "nodesAsked": 3, and the router adds up the
# three answers: 1802 (the users and the two accounts). With everything
# healthy that takes a few milliseconds ("tookMs").
say "4. A question for the whole cluster: how many keys are there?"
call GET $ROUTER/count

# ---------- Act 2 ----------

say "=== Act 2: one SLOW shard (shard-c takes 1 second to answer anything) ==="
# A switch built into the nodes for this demo. It stands in for a machine
# that is overloaded, has a failing disk, or sits behind a bad network.
curl -s -o /dev/null -X PUT -d 1000 $SHARD_C/delay

# A few milliseconds, as before. shard-a has nothing to do with shard-c.
say "1. A key on a healthy shard (user-0008, shard-a)"
call GET $ROUTER/kv/user-0008

# One second. Only the keys that live on shard-c are affected: a third.
say "2. A key on the slow shard (user-0001, shard-c)"
call GET $ROUTER/kv/user-0001

# One second as well, although two of the three shards answered at once.
# The router cannot give the total before the LAST shard has answered, so
# anything that asks every shard is as slow as the slowest of them. With
# more shards, the chance that one of them is having a bad moment grows.
say "3. The count"
call GET $ROUTER/count

# Back to normal.
curl -s -o /dev/null -X PUT -d 0 $SHARD_C/delay

# ---------- Act 3 ----------

say "=== Act 3: one DEAD shard (shard-b is stopped) ==="
$COMPOSE stop shard-b >/dev/null 2>&1

# 502: the router cannot reach the shard that owns this key.
say "1. A key on the dead shard (user-0002, shard-b)"
call GET $ROUTER/kv/user-0002

# Works as if nothing had happened.
say "2. A key on a healthy shard (user-0001, shard-c)"
call GET $ROUTER/kv/user-0001

# 1161 users are still served and 639 are not: exactly shard-b's share.
# A single database that goes down takes everything with it. Here the damage
# stops at the edge of one shard.
say "3. All users"
read_all_users

# 503: the router refuses. Without shard-b it cannot know the total, and it
# will not give a number that it knows is wrong.
say "4. The count"
call GET $ROUTER/count

# The other choice: answer with what can be reached, and say so.
# "complete": false and "missing": ["shard-b"]. The count is real but too low.
#
# Refuse, or answer with less? That is the consistency-or-availability
# choice of the CAP example, now made per question.
say "5. The count, allowed to be incomplete"
call GET "$ROUTER/count?partial=yes"

# ---------- Act 4 ----------

say "=== Act 4: a write across two shards while shard-b is still dead ==="

# Move 10 from alice to bob: alice goes from 100 to 90, bob from 100 to 110.
# Both changes belong together, so they are sent as one request.
#
# "written": bob has his 110 (shard-c is up).
# "failed": alice was NOT set to 90 (shard-b is down). She still has 100,
# on a shard nobody can reach right now.
#
# 10 were created out of nothing. On a single database the two changes could
# be one transaction: both or neither. Across shards each write stands alone.
say "1. Transfer 10 from alice (shard-b) to bob (shard-c)"
call PUT $ROUTER/kv-many '{"account-alice":"90","account-bob":"110"}'

# 110.
say "2. Bob's account"
call GET $ROUTER/kv/account-bob

# ---------- Act 5 ----------

say "=== Act 5: shard-b comes back ==="
$COMPOSE start shard-b >/dev/null 2>&1
until curl -sf $ROUTER/count >/dev/null; do sleep 0.3; done

# The shard answers again, but it is EMPTY: 0 keys. The nodes in this demo
# keep their data in memory, and it died with the process. A real shard
# would lose its share the same way if its disk failed.
say "1. Status"
call GET $ROUTER/status

# 404 now instead of 502: the shard is there, the keys are not.
# 639 users are gone for good.
say "2. All users"
read_all_users

# Alice's account is gone too (404), while bob kept his 110.
say "3. Alice's account"
call GET $ROUTER/kv/account-alice

# Sharding made the failure smaller, not harmless: each shard is still the
# only place where its keys live. Protecting them takes copies of every
# shard, which is replication, and brings back the questions of the CAP
# example for each shard.

# Stop and remove the containers and the network. Data lives in memory only,
# so nothing is left behind.
say "Cleaning up: docker compose down"
$COMPOSE down >/dev/null 2>&1

say "Done."
