#!/usr/bin/env bash
# Demo 5: directory-based sharding. The router keeps a table of keys that
# were moved by hand, so a hot spot can be taken apart. What that fixes, what
# it does not, and what it costs.
#
# Setup: three shards share the keys by hash, exactly as in demo 3. A fourth
# node, shard-d, starts empty and only gets keys that are moved to it. Each
# node has room for 1000 keys and can do 200 writes/s.
#
# The "directory" is the router's table of exceptions: "this key lives on
# that node". Any key that is not in it lives where its hash says.
#
#   Act 1, a hot SHARD:  two busy keys happen to share a shard -> collapse.
#                        Move one of them -> fixed.
#   Act 2, a hot KEY:    one key gets more writes than a whole shard can do.
#                        Move it to its own shard -> its neighbours recover,
#                        the key itself is still in trouble.
#   Act 3, a hot KEY:    give that one shard a faster machine -> fixed.
#   Act 4, the cost:     restart the router -> the table is gone, and so is
#                        the way to the moved key.
#
# What to look for: a directory can change WHERE a key lives. It cannot make
# one key use more than one shard, but it can make sure that shard is the
# right one for the job.
set -euo pipefail
cd "$(dirname "$0")"

# This demo uses its own compose file, so every compose command needs "-f".
COMPOSE="docker compose -f docker-compose.5-directory.yml"

# The router, as published on your machine by the compose file.
ROUTER=http://localhost:8000

# How long each run of traffic is kept up.
SECONDS_PER_STAGE=5

# Print a bold heading.
say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# call <METHOD> <URL> [body]
# Runs one HTTP request and prints: the request, the status code, the response.
call() {
  printf '  %-4s %-40s -> ' "$1" "${2#http://}${3:+  ($3)}"
  curl -s -o /tmp/sharding-demo-body -w '%{http_code}  ' -X "$1" ${3:+-d "$3"} "$2" || printf 'ERR  '
  cat /tmp/sharding-demo-body 2>/dev/null || echo
}

# start [write cost of shard-d in ms]
# (Re)create the containers and wait until the router answers. Recreating
# empties the shards AND the router's directory, so every experiment starts
# clean. The optional argument changes how long a write takes on shard-d
# only; without it shard-d is as fast as the others (5 ms per write).
start() {
  SHARD_D_WRITE_COST_MS=${1:-5} $COMPOSE up -d --force-recreate >/dev/null 2>&1
  until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done
}

# traffic <label> <writes per second> <key pattern>
# Writes at a steady rate for SECONDS_PER_STAGE seconds, to keys picked from
# the pattern. A client gives up after 1 second. Prints one line saying how
# it went, starting with the label.
# (demo-1-write-limit.sh explains every oha option used here.)
traffic() {
  local label=$1 rate=$2 pattern=$3
  $COMPOSE run --rm oha --no-tui -u ms -q "$rate" -n $((rate * SECONDS_PER_STAGE)) -c 1000 -t 1s \
    -m PUT -d hello --rand-regex-url "http://router:8000/kv/$pattern" 2>/dev/null |
    awk -v rate="$rate" -v label="$label" '
      /\[200\]/     { gsub(/[^0-9 ]/, "", $0); ok = $2 }
      /\] timeout/  { gsub(/[^0-9]/, "", $1); timeouts = $1 }
      /50\.00% in/  { wait = $3 }
      END {
        printf "  %-16s %4d writes/s -> %5d answered, %5d gave up after 1 s, typical wait %4.0f ms\n",
               label, rate, ok, timeouts, wait
      }'
}

# ---------- Act 1: a hot shard ----------

# user-0002 and user-0003 are two busy users with 150 writes/s each. Either
# one alone is well within what a shard can do (200 writes/s).
#
# Each write picks one of the two at random, so 300 writes/s in total.
TWO_BUSY_USERS='user-(0002|0003)'

say "=== Act 1: a hot shard. BEFORE, keys placed by hash only ==="
start

# Bad luck: the hash puts both users on shard-b ("node" in the responses).
# With three shards, any two keys share one a third of the time.
say "1. Where do the two busy users live?"
call PUT $ROUTER/kv/user-0002 hello
call PUT $ROUTER/kv/user-0003 hello

# shard-b gets all 300 writes/s and can do 200: collapse, while shard-a and
# shard-c do nothing at all.
say "2. Traffic: 150 writes/s to each of the two"
traffic "both users" 300 "$TWO_BUSY_USERS"

say "=== Act 1: a hot shard. AFTER, one of the two moved by hand ==="
start
call PUT $ROUTER/kv/user-0002 hello >/dev/null
call PUT $ROUTER/kv/user-0003 hello >/dev/null

# One request to the router's directory: "user-0003 lives on shard-a now".
# The router copies the value over, notes the new home in its table, and
# removes the old copy.
say "1. Move user-0003 to shard-a, which has nothing to do"
call PUT $ROUTER/directory/user-0003 shard-a

# The read follows the key to its new home.
call GET $ROUTER/kv/user-0003

# Everything is answered. shard-b and shard-a get 150 writes/s each, both
# under their limit. Nothing was added to the cluster: the same three shards
# take the same traffic that broke them a moment ago.
say "2. Traffic: the same 150 writes/s to each of the two"
traffic "both users" 300 "$TWO_BUSY_USERS"

# The work is now split between shard-a and shard-b.
say "   Status: who did the work"
call GET $ROUTER/status

# ---------- Act 2: a hot key ----------

# The hot key from demo 4: user-1500 alone gets 400 writes/s, twice what a
# shard can do. On top of that, 100 writes/s go to all the other users.
#
# The two kinds of traffic are sent at the same time (the "&" runs a command
# in the background, "wait" waits for both) but counted separately, to see
# who is affected.
#
# The pattern for "everybody else" ends in [1-9], so it produces user-0001,
# user-0002, ... but never a key ending in 0, and so never user-1500 itself.
hot_key_traffic() {
  traffic "user-1500" 400 'user-1500' >/tmp/sharding-demo-hot &
  traffic "everybody else" 100 'user-[0-2][0-9]{2}[1-9]' >/tmp/sharding-demo-others &
  wait
  cat /tmp/sharding-demo-hot /tmp/sharding-demo-others
}

say "=== Act 2: a hot key. BEFORE, keys placed by hash only ==="
start

# user-1500: about 400 of its 2000 writes are answered. Its shard (shard-a)
# falls further behind every second.
#
# everybody else: about a quarter of THEIR writes fail too. Those are the
# users who happen to live on shard-a: they are stuck in the same queue,
# behind the hot key's writes. The users on shard-b and shard-c are fine.
say "1. Traffic: 400 writes/s to user-1500, 100 writes/s to everybody else"
hot_key_traffic

say "=== Act 2: a hot key. AFTER, the hot key moved to a shard of its own ==="
start
call PUT $ROUTER/kv/user-1500 hello >/dev/null

# shard-d holds nothing else, so user-1500 has no neighbours there.
say "1. Move user-1500 to shard-d"
call PUT $ROUTER/directory/user-1500 shard-d

# everybody else: all 500 writes are answered. The neighbours on shard-a no
# longer share a queue with the hot key.
#
# user-1500: no better than before. It still gets 400 writes/s and shard-d,
# like any shard, can do 200. The directory decides which shard a key lives
# on; the key still lives on ONE shard.
say "2. Traffic: the same 400 + 100 writes/s"
hot_key_traffic

# shard-d holds 1 key and did all the work for it.
say "   Status: who did the work"
call GET $ROUTER/status

# ---------- Act 3: a faster machine for the hot key ----------

# The hot key cannot be spread over several shards, but its ONE shard can be
# made faster. This is scaling up, as in demo 1, with a difference: thanks to
# the directory only the node that needs it is upgraded. shard-a, shard-b and
# shard-c stay the small machines they were.

say "=== Act 3: a hot key. AFTER, its own shard is a faster machine (500 writes/s) ==="
# 2 ms per write on shard-d instead of 5: 500 writes per second.
start 2
call PUT $ROUTER/kv/user-1500 hello >/dev/null

say "1. Move user-1500 to shard-d, as before"
call PUT $ROUTER/directory/user-1500 shard-d

# user-1500: all 2000 writes are answered, within a few milliseconds.
# everybody else: all 500 answered, as in act 2.
#
# It is still scaling up, so demo 1's warning still holds: this works until
# the key outgrows the biggest machine there is. Beyond that, the key itself
# has to be split into several keys, which the application has to do.
say "2. Traffic: the same 400 + 100 writes/s"
hot_key_traffic

# shard-d did more than ten times the work of any other shard, and kept up.
say "   Status: who did the work"
call GET $ROUTER/status

# ---------- Act 4: the cost ----------

# With "modulo" the router worked out the owner of a key from the key itself.
# It had nothing to remember, so it could be restarted, replaced or run in
# several copies without a second thought.
#
# With a directory, WHERE A KEY IS has become data in its own right, and in
# this demo that data lives in the router's memory.

say "=== Act 4: the cost. The router now has something to lose ==="

# One entry: user-1500 -> shard-d. The read finds the key there.
say "1. Before the restart: the directory, and a read of the moved key"
call GET $ROUTER/directory
call GET $ROUTER/kv/user-1500

say "2. Restart the router (the shards keep running and keep their data)"
$COMPOSE restart router >/dev/null 2>&1
until curl -sf $ROUTER/status >/dev/null; do sleep 0.3; done

# The directory is empty, so the router falls back to the hash and looks for
# user-1500 on shard-a. It is not there: 404, "key not found".
#
# The value was not lost. It still sits on shard-d (the last line asks
# shard-d directly, on its own port). What was lost is the way to it. And a
# new write to user-1500 would now be stored on shard-a, leaving two
# different values for the same key on two shards.
say "3. After the restart: the directory, and the same read"
call GET $ROUTER/directory
call GET $ROUTER/kv/user-1500
call GET http://localhost:8004/kv/user-1500

# Stop and remove the containers and the network. Data lives in memory only,
# so nothing is left behind.
say "Cleaning up: docker compose down"
$COMPOSE down >/dev/null 2>&1

say "Done."
