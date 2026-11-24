# Sharding example

A minimal setup to see what sharding is for: a tiny key-value API in plain
Node.js (no libraries), run as Docker containers behind a router, and put
under load with [oha](https://github.com/hatoo/oha).

## The idea in one minute

A single database node has limits: it can hold only so much data and do only
so many writes per second. There are two ways to add nodes:

- **Replication**: every node stores **every** key. This gives you spare
  copies and more nodes to read from. It does not give you more room or
  faster writes, because every node still has to store and write everything.
- **Sharding**: every key lives on **one** node, and the keys are split
  between the nodes. Each node stores and writes only its share, so room and
  write speed grow with the number of nodes.

The demos build on each other:

- **Demo 1** shows the write limit: traffic outgrows a node, and neither a
  bigger machine nor replicas solve it for good.
- **Demo 2** shows the storage limit: data outgrows a node, and replicas add
  no room.
- **Demo 3** shards the data and lifts both limits.
- **Demo 4** shows that this only works if the keys let the work spread, and
  what each way of assigning keys costs.
- **Demo 5** takes hot spots apart with a directory, and shows where that
  stops working.
- **Demo 6** adds a fourth shard and counts how much stored data has to move.
- **Demo 7** looks at operations that span several shards, and at a shard
  that is slow or dead.
- **Demo 8** puts the two ideas together: every shard gets a replica.

## How it works

| Piece | What it does |
|---|---|
| `shard.js` | One storage node. Holds its data in memory and knows nothing about other nodes. |
| `router.js` | The front door. Holds no data: it decides which node(s) handle each request and passes it on. |
| oha | Load-testing CLI. Runs as a container, so there is nothing to install. |

A laptop cannot honestly show hardware limits, so every node has two
**artificial** ones, small and exact, so the numbers are the same on every
machine:

| Setting | Value | Stands in for | Effect |
|---|---|---|---|
| `MAX_KEYS` | 1000 | A full disk | New keys beyond it are refused with `507` |
| `WRITE_COST_MS` | 5 | A disk's write speed | Writes happen one at a time, so at most 200 per second |

Clients use the router at `localhost:8000`:

| Endpoint | What it does |
|---|---|
| `PUT /kv/{key}` | Store the request body as the value |
| `GET /kv/{key}` | Read a value |
| `GET /status` | Per node: keys stored, room, writes handled |
| `GET /scan?from={key}&to={key}` | Count the keys in a stretch, and say which nodes had to be asked |
| `PUT /directory/{key}` | Only with the `directory` strategy: move the key to the node named in the request body |
| `GET /directory` | Only with the `directory` strategy: list the moved keys and where they live |
| `POST /rebalance` | Move every key to the node that owns it now, and say how many had to move |
| `GET /count` | Count the keys in the whole cluster by asking every node. With `?partial=yes` it answers even if some nodes are down. |
| `PUT /kv-many` | Write several keys at once, given as a JSON object of keys and values |
| `POST /repair` | Only with the `replicated` strategy: copy to every replica the keys it is missing |

The router's `STRATEGY` decides how requests are spread:

| Strategy | Where a key lives | Used in |
|---|---|---|
| `mirror` | On every node. A write goes to all nodes, a read to one of them in turn. With a single node, that is simply one database. | Demos 1 and 2 |
| `modulo` | On one node, picked by `hash(key) % number of nodes`. Reads and writes for a key go to that node only. | Demos 3 and 4 |
| `range` | On one node, picked by the key's place in alphabetical order: each node owns one stretch of the keys. | Demo 4 |
| `directory` | On one node, picked by hash like `modulo`, unless the router's table says otherwise. Keys can be moved by hand. | Demo 5 |
| `ring` | On one node, picked by consistent hashing: nodes and keys are placed on a circle by their hashes, and a key belongs to the next node clockwise. | Demo 6 |
| `replicated` | On every node of one shard. The shard is picked by hash; a shard is a group of nodes that mirror each other. | Demo 8 |

The hash turns a key into a number that looks random but is always the same
for the same key, so keys spread **almost** evenly. With `modulo`, `range` and
`ring` the router needs no table of which key is where: it computes the
owner from the key. With `directory` it has to remember the keys that were
moved.

## Running the examples

You need Docker with Docker Compose, plus `bash` and `curl`.

### Demo 1: the write limit

```sh
./demo-1-write-limit.sh
```

Traffic outgrows a node. Writes are sent at a steady rate, the way real
traffic arrives, and that rate is raised step by step. Each step lasts 5
seconds, and a client gives up if it has no answer after 1 second. 

| Act | Setup | Limit | Traffic | Answered | Gave up | Typical wait |
|---|---|---|---|---|---|---|
| 1 | One node | 200 writes/s | 50, 100, 150 writes/s | all | 0 | 7 ms |
| 1 | One node | 200 writes/s | 300 writes/s | 597 of 1500 | 903 | 500 ms |
| 2 | One node, twice as fast | 400 writes/s | 300 writes/s | all | 0 | 3 ms |
| 2 | One node, twice as fast | 400 writes/s | 600 writes/s | 1197 of 3000 | 1803 | 500 ms |
| 3 | Three nodes, mirrored | 200 writes/s | 150 writes/s | all | 0 | 7 ms |
| 3 | Three nodes, mirrored | 200 writes/s | 300 writes/s | 596 of 1500 | 904 | 500 ms |

What each act shows:

1. **Going over the limit is a collapse, not a slowdown.** At 300 writes/s
   the node falls 100 writes further behind every second. After two seconds
   every new write has more than a second of queue ahead of it, so only the
   writes from those first two seconds are answered in time.
2. **Scaling up moves the limit, it does not remove it.** A machine twice as
   fast handles 300 writes/s easily, and breaks the same way once traffic
   doubles again. Bigger machines get expensive quickly and eventually there
   is no bigger one.
3. **Replicas do not help writes.** Three mirrored nodes break at the same
   300 writes/s as a single one, because each of them has to do every write.

This demo rewrites the same 100 keys, so storage never fills. Running out
of room is demo 2.

### Demo 2: the storage limit

```sh
./demo-2-storage-limit.sh
```

Data outgrows a node. The same load, 3000 writes with new keys, is sent
first to one node and then to three mirrored nodes. Here the writes are sent
as fast as the node answers them, so nothing times out. 

| | One node | Three nodes, mirrored |
|---|---|---|
| Writes accepted (`200`) | 999 | 999 |
| Writes refused (`507`) | 2001 | 2001 |
| Keys stored | 1000 | 1000, on each of the three |
| Writes handled | 3001 | 3001 by each node, 9003 in total |
| Writes per second | about 200 | about 200 |

(999 and not 1000 because the script stores one key by hand before the load.)

Three nodes hold no more than one: they did three times the work to store
the same 1000 keys. A bigger disk would help the way the faster machine did
in demo 1, by moving the limit.

After both demos, what is left is to stop making every node do everything:
split the keys so that each node stores and writes only its share. **That is
sharding**.

### Demo 3: sharding

```sh
./demo-3-sharding.sh
```

The same nodes with the same limits, but the router now gives each key to one
node (`modulo` strategy). Each half runs a load that failed earlier, first on
one node to see it fail again, then on three shards. 

**Part 1, write speed** (the load from demo 1):

| Setup | Traffic | Answered | Gave up | Typical wait |
|---|---|---|---|---|
| Before: one node | 300 writes/s | 596 of 1500 | 904 | 500 ms |
| After: three shards | 300 writes/s | all | 0 | 7 ms |
| After: three shards | 500 writes/s | all | 0 | 12 ms |

Afterwards the three shards had handled 1524, 1209 and 1267 writes: about a
third each, adding up to the 4000 that were sent. With mirroring, every node
handled every write.

**Part 2, storage** (the load from demo 2, 3000 new keys):

| | Before: one node | After: three shards |
|---|---|---|
| Writes accepted (`200`) | 1000 | 2970 |
| Writes refused (`507`) | 2000 | 30 |
| Writes per second | about 200 | about 580 |
| Keys stored | 1000 | 1000 + 991 + 980 |

The numbers of the "after" column change a little from run to run, because
the keys are random.

Both limits are roughly tripled, not exactly:

- **A few writes are still refused.** The keys do not split into exactly
  1000 per shard. One shard was offered 1030, kept 1000 and refused 30, while
  the other two still had 29 places free between them.
- **Write speed tops out a little under 600.** The busiest shard reaches its
  200 writes/s before the others do.

That is the price of spreading keys by hash: the usable capacity is somewhat
less than the sum of the nodes. Unlike a bigger machine, though, the next
step is always available: add another shard.

### Demo 4: the key choice matters

```sh
./demo-4-key-choice.sh
```

Three shards throughout, so the cluster can do close to 600 writes/s. The
keys are users, `user-0000` to `user-2999`, and every act sends 500 writes/s
for 5 seconds. Two ways of assigning keys to shards are compared: by `range`
(`shard-a` owns `user-0xxx`, `shard-b` owns `user-1xxx`, `shard-c` owns
`user-2xxx`) and by hash (`modulo`).

| Act | Shards by | Writes go to | Answered | Gave up | Scan of 100 neighbouring users |
|---|---|---|---|---|---|
| 1 | Range | The newest users (`user-2xxx`) | 331 of 2500 | 2169 | Asks 1 shard |
| 2 | Hash | The newest users (`user-2xxx`) | all | 0 | Asks 3 shards |
| 3 | Range | One user, 80% of the time | 690 of 2500 | 1810 | |
| 3 | Hash | One user, 80% of the time | 698 of 2500 | 1802 | |

What each act shows:

1. **Range sharding puts neighbours together, for better and worse.**
   Applications mostly write to their newest records, and with keys handed
   out in order those all belong to the same shard. `shard-c` got all 500
   writes/s and collapsed like the single node of demo 1, while the other two
   handled nothing. The upside is the scan: the users from `user-1000` to
   `user-1099` can only be on `shard-b`, so one request answers it.
2. **Hashing spreads the writes and scatters the neighbours.** The same
   traffic is divided into thirds and everything is answered. The same scan
   now needs all three shards (35, 32 and 33 keys came from each), because
   any shard may hold some of the keys in between. With 100 shards that
   would be 100 requests, and the scan would be as slow as the slowest one.
3. **A hot key defeats both.** A key lives on exactly one shard, so if most
   writes are for one key, most writes are for one shard. That shard handled
   about 1225 writes in both runs against about 165 on each of the others.
   The writes that were answered are mostly those to other users.

In every collapse two of the three shards were almost idle. Sharding
multiplies capacity only for work that the keys allow to be divided.

### Demo 5: directory-based sharding

```sh
./demo-5-directory.sh
```

Three shards share the keys by hash, as in demo 3. On top of that the router
keeps a **directory**: a table of exceptions saying "this key lives on that
node", which can be changed while everything runs. A fourth node, `shard-d`,
starts empty and only holds keys that are moved to it. 

**Act 1, a hot shard.** Two busy users get 150 writes/s each. Either is fine
for a shard on its own, but the hash puts both on `shard-b`.

| Setup | Traffic | Answered | Gave up | Typical wait |
|---|---|---|---|---|
| Before: both on `shard-b` | 300 writes/s | 596 of 1500 | 904 | 503 ms |
| After: one of them moved to `shard-a` | 300 writes/s | all | 0 | 8 ms |

One directory entry fixes it, with the same three shards and the same
traffic. The spare capacity was there all along.

**Act 2, a hot key.** `user-1500` alone gets 400 writes/s, twice what a
shard can do, while everybody else gets 100 writes/s between them.

| Setup | `user-1500` answered | Everybody else answered |
|---|---|---|
| Before: `user-1500` on `shard-a` with its neighbours | 338 of 2000 | 375 of 500 |
| After: `user-1500` alone on `shard-d` | 398 of 2000 | all 500 |

Before the move, a quarter of the other users' writes failed too: the users
who live on `shard-a` were stuck in the same queue as the hot key. Moving the
hot key to a shard of its own frees them. The hot key itself is no better
off: it still gets 400 writes/s and its shard can still do 200.

**Act 3, a faster machine for the hot key.** The same as act 2, but
`shard-d` is a machine that can do 500 writes/s instead of 200.

| Setup | `user-1500` answered | Everybody else answered |
|---|---|---|
| `user-1500` alone on a faster `shard-d` | all 2000 | all 500 |

The hot key cannot be spread over several shards, but its one shard can be
made faster. This is scaling up, as in demo 1, except that the directory
lets it be applied to the one node that needs it: the other three stay small.
Demo 1's warning still holds, though. It works until the key outgrows the
biggest machine available.

**Act 4, the cost.** The router is restarted.

| | Before the restart | After the restart |
|---|---|---|
| Directory | `user-1500` is on `shard-d` | empty |
| Read of `user-1500` | `200`, from `shard-d` | `404`, the router looks on `shard-a` |

The value is still on `shard-d`; what was lost is the way to it. A new write
to `user-1500` would now land on `shard-a`, leaving two different values for
one key.

What the demo shows:

- **A directory fixes hot shards.** It can put any key on any node, so keys
  that collide can be separated and a noisy key can be isolated.
- **It does not fix a hot key by itself.** A key still lives on one shard.
  What the directory adds is the choice of which shard, so the hot key can
  be given a faster one (act 3). Past the biggest machine, the application
  has to split the key into several.
- **Where a key lives becomes data.** With `modulo` the router could be
  restarted or run in several copies freely, because it computed everything.
  A directory has to be stored safely, kept in step with the data and
  available to every router. In real systems it lives in a replicated store
  of its own.

Beyond a faster machine (not part of the demo): act 3 solves the hot key by
scaling up its shard, so it holds only while the key's traffic fits on the
biggest machine available. Here that would show as soon as `user-1500` got
more than 500 writes/s. Past that point the way the key is written has to
change, because one key always lives on one shard:

- **Split the key into sub-keys.** The application writes to `user-1500#0`
  to `user-1500#3`, picking one at random per write. The sub-keys hash to
  different shards, so 400 writes/s become about 100 on each. Reading the
  full value then means reading every sub-key and combining them, which is a
  cross-shard operation with the problems of demo 7. It only works for data
  that can be combined, such as counters, likes or lists of events, and not
  for a single value that every write replaces.
- **Combine writes before they reach the shard.** Collect the writes to the
  hot key for a short while and send one write with the result: one "add
  400" per second instead of 400 times "add 1". The cost is a small delay,
  and losing the collected writes if the process holding them dies.
- **Combine writes behind a durable queue.** The usual way to do the above
  without that risk. Every write is appended to a queue that survives
  crashes, and is acknowledged at that point. A consumer reads the queue in
  batches, combines the writes for each key and sends one write per key per
  batch to the shard. For a counter that is "add 400"; for a value that every
  write replaces, only the last one in the batch is written. Appending to a
  queue is far cheaper than a database write, and if the consumer dies the
  queue still holds whatever was not applied yet. The costs:
  - A write is acknowledged when it is queued, not when it is stored, so a
    read right after a write may not see it.
  - The queue has to take the full 400 writes/s for that key, and has become
    a piece that must itself be durable and available.
  - Applying a batch twice must be harmless, because a consumer that crashes
    after writing but before marking the batch as done will apply it again.
    "Set to X" is safe; "add 400" needs protection, such as recording which
    batch was applied last.
  - For values that are replaced, a key's writes must be applied in the
    order they were queued.
- **Reduce the writes.** Rate-limit the key, or record only a sample and
  scale the result up. Fine for something like view counts, not for money.

Two things that do not help a key that is written too often: a queue on its
own, without the combining, only absorbs a burst and otherwise just grows,
as the node's own queue did in demo 1; and replicas or a cache help with
reads, while every replica still has to do every write (demo 8).

### Demo 6: adding a shard

```sh
./demo-6-adding-a-shard.sh
```

The cluster grows from three shards to four while holding 1800 keys. The
same steps are run with two ways of assigning keys by hash: `modulo`, and
`ring` (consistent hashing).

| Step | `modulo` | `ring` |
|---|---|---|
| 1. Three shards, 1800 keys stored | all 1800 can be read | all 1800 can be read |
| 2. Fourth shard added, nothing moved yet | 1356 keys not found | 422 keys not found |
| 3. Rebalance: keys moved | 1356 (75%) | 422 (23%) |
| 3. Rebalance: time | 6.3 s | 2.1 s |
| Afterwards | all 1800 can be read | all 1800 can be read |

What it shows:

- **Adding a shard does not move any data by itself.** The router starts
  computing owners over four shards, and for every key whose owner changed
  it looks in the wrong place. The data is all still there, but those keys
  answer `404` until they are moved.
- **`modulo` reshuffles almost everything.** `hash % 3` and `hash % 4` agree
  for only a quarter of the keys, so three quarters of everything stored is
  copied to gain a quarter more room. Most of those keys move from one old
  shard to another, and each old shard did 700 to 800 extra writes.
- **The ring moves only what the new shard takes over.** The fourth shard
  should hold about a quarter of the keys, so about a quarter is the least
  that can move. All 422 moved keys went to `shard-d`; no key moved between
  old shards.
- **Moving data is work.** Every copy and delete goes through the same write
  queues as the application's writes. That is why the amount moved matters:
  in a live system a rebalance competes with the traffic.

How the ring gets there: every shard is placed at 100 spots on a circle, at
the hashes of `shard-a#0`, `shard-a#1` and so on. A key is placed at its own
hash and belongs to the shard whose spot comes next, clockwise. A shard's
spots depend only on its name, not on how many shards there are, so adding a
shard leaves every existing spot in place. The new spots each take over the
piece of the circle just before them, and all other keys keep their owner.

The price is a less even split. With the ring the four shards ended up with
418, 460, 500 and 422 keys, against 416, 488, 445 and 451 with `modulo`.

### Demo 7: across shards, and when one fails

```sh
./demo-7-across-shards.sh
```

Everything that touches a single key involves a single shard. This demo is  
about the rest: a question for the whole cluster (how many keys are there?),  
a write to two keys on different shards (a transfer between two accounts),  
and what a slow or dead shard does to them. Three shards by hash hold 1800  
users and two accounts.

| Act | State of the cluster | A key on a healthy shard | A key on the affected shard | The count |
|---|---|---|---|---|
| 1 | All healthy | `200` in 4 ms | | 1802, in 5 ms |
| 2 | `shard-c` takes 1 second to answer | `200` in 4 ms | `200` in 1 s | 1802, in 1 s |
| 3 | `shard-b` is stopped | `200` in 2 ms | `502` | refused (`503`), or 1162 marked incomplete |
| 5 | `shard-b` is back, empty | `200` | `404` | 1162 keys are left, per the status |

Reading all 1800 users while `shard-b` was stopped: 1161 found, 639 failed.
After it came back: 1161 found, 639 not found.

Act 4 is the write across two shards with `shard-b` still stopped. Moving 10
from alice (`shard-b`) to bob (`shard-c`) set bob to 110 and failed to set
alice to 90: the request ended half done.

What it shows:

- **A failure is limited to one shard's share.** With `shard-b` slow or
  dead, the keys on the other two shards were served as fast as ever. A
  single database that goes down takes everything with it.
- **Whatever asks every shard is as slow as the slowest.** The count took a
  full second when one of three shards did, because the router cannot answer
  before the last shard has. The more shards, the more likely one of them is
  having a bad moment.
- **Whatever asks every shard needs every shard.** With one shard down the  
  router can refuse to count, or give a number it knows is too low and say  
  so. That is the consistency-or-availability choice of the CAP theorem.
- **Writes across shards are not all-or-nothing.** On one database the
  transfer could be a transaction. Across shards each write succeeds or fails
  alone, so 10 appeared out of nothing. Doing better needs a protocol such as
  two-phase commit, with extra round trips and failure cases of its own.
- **A shard is still the only home of its keys.** `shard-b` came back empty,
  since the nodes keep data in memory, and 639 users and alice's account
  were gone. A failed disk would do the same to a real shard. Protecting the
  data takes copies of every shard, which is replication.

This is why applications that shard try to pick keys so that the things used
together live on the same shard, and why sharding is put off for as long as
one database will do.

### Demo 8: replicated shards

```sh
./demo-8-replicated-shards.sh
```

Sharding and replication together. The keys are split over three shards by
hash, and every shard is two nodes that hold the same keys: six nodes in
all, `shard-a1` and `shard-a2`, `shard-b1` and `shard-b2`, `shard-c1` and
`shard-c2`. They hold 1800 users. The demo takes about 25 seconds and
removes the containers when it finishes.

**Act 1, all healthy.** Every key is stored twice: 558, 639 and 603 keys per
shard, the same on both replicas.

| Traffic | Answered | Gave up |
|---|---|---|
| 500 writes/s | all 2500 | 0 |
| 900 writes/s | 1867 of 4500 | 2633 |

Six nodes of 200 writes/s each do not add up to 1200. Both replicas of a
shard do every write of that shard, so the cluster is as fast as its three
shards: about 600, as in demo 3. The three extra machines add no capacity.

**Acts 2 and 3, a replica dies and comes back** (`shard-b1`).

| State | Read all 1800 users | Keys on `shard-b1` / `shard-b2` |
|---|---|---|
| `shard-b1` stopped | all found | down / 639 |
| `shard-b1` back, not repaired | 1161 found, 639 not found | 0 / 639 |
| After the repair (639 keys copied in 3.2 s) | all found | 639 / 639 |

While the replica was down, a write to its shard was still accepted: stored
on `shard-b2`, missed by `shard-b1`.

**Act 4, strict writes.** The router is told that a write only counts if
every replica of the shard stored it, and `shard-b1` is stopped again.

| Request | Result |
|---|---|
| Write to shard b (dead replica) | `503`, refused |
| Write to shard c | `200`, on both replicas |
| Read all 1800 users | all found |

What it shows:

- **Shards give capacity, replicas give safety.** The number of shards
  decides how much the cluster can hold and write. The replicas decide what
  happens when a node dies, and are paid for in machines.
- **A dead replica costs nothing visible.** The router skips it and asks the
  other one. In demo 7 the same failure, with one node per shard, made 639
  users unavailable and then lost them.
- **A replica that is back but behind is worse than a dead one.** It
  answers, and the answers are wrong: 639 users "not found" although their
  data was safe on the other replica. It has to be repaired before it is
  trusted, and the repair is write work that takes time.
- **Each shard faces the CAP choice.** With a replica down, a write can be  
  accepted on the remaining one (available, but the replicas now differ) or  
  refused (consistent, but a third of the keys become read-only). Other  
  shards are not affected either way. Three replicas per shard with a  
  majority rule give both for a single failure...

This is the shape of most real sharded databases: a router, shards for
scale, and a small replicated group behind every shard.

## Playing with it by hand

Each demo has its own compose file, so every command needs `-f`. Run one
demo's setup at a time: they all use the same service names and ports.

### Demo 1 setup: the write limit

```sh
COMPOSE="docker compose -f docker-compose.1-write-limit.yml"

# One node behind the router
NODES=http://shard-a:8000 $COMPOSE up -d --force-recreate

# Or one node twice as fast (half the time per write)
NODES=http://shard-a:8000 WRITE_COST_MS=2.5 $COMPOSE up -d --force-recreate

# Or three mirrored nodes
NODES=http://shard-a:8000,http://shard-b:8000,http://shard-c:8000 $COMPOSE up -d --force-recreate

# Steady traffic: 300 writes/s for 5 seconds, clients give up after 1 second
$COMPOSE run --rm oha --no-tui -q 300 -n 1500 -c 1000 -t 1s -m PUT -d hello \
  --rand-regex-url 'http://router:8000/kv/user-[0-9]{2}'

# Stop
$COMPOSE down
```

### Demo 2 setup: the storage limit

```sh
COMPOSE="docker compose -f docker-compose.2-storage-limit.yml"

# One node behind the router
NODES=http://shard-a:8000 $COMPOSE up -d --force-recreate

# Or three mirrored nodes
NODES=http://shard-a:8000,http://shard-b:8000,http://shard-c:8000 $COMPOSE up -d --force-recreate

# Load: 3000 writes with new keys, 50 at a time, full oha report
$COMPOSE run --rm oha --no-tui -n 3000 -c 50 -m PUT -d hello \
  --rand-regex-url 'http://router:8000/kv/[a-z]{12}'

# Stop
$COMPOSE down
```

### Demo 3 setup: sharding

```sh
COMPOSE="docker compose -f docker-compose.3-sharding.yml"

# Three shards behind the router
NODES=http://shard-a:8000,http://shard-b:8000,http://shard-c:8000 $COMPOSE up -d --force-recreate

# Or one node, for comparison
NODES=http://shard-a:8000 $COMPOSE up -d --force-recreate

# The response says which shard owns the key
curl -X PUT -d hello localhost:8000/kv/greeting

# Steady traffic: 500 writes/s for 5 seconds, clients give up after 1 second
$COMPOSE run --rm oha --no-tui -q 500 -n 2500 -c 1000 -t 1s -m PUT -d hello \
  --rand-regex-url 'http://router:8000/kv/user-[1-9][0-9]{2}'

# Fill it up: 3000 writes with new keys, 50 at a time
$COMPOSE run --rm oha --no-tui -n 3000 -c 50 -m PUT -d hello \
  --rand-regex-url 'http://router:8000/kv/[a-z]{12}'

# Stop
$COMPOSE down
```

### Demo 4 setup: the key choice

```sh
COMPOSE="docker compose -f docker-compose.4-key-choice.yml"

# Three shards, keys assigned by range (user-0xxx, user-1xxx, user-2xxx)
STRATEGY=range $COMPOSE up -d --force-recreate

# Or assigned by hash
STRATEGY=modulo $COMPOSE up -d --force-recreate

# 500 writes/s to the newest users
$COMPOSE run --rm oha --no-tui -q 500 -n 2500 -c 1000 -t 1s -m PUT -d hello \
  --rand-regex-url 'http://router:8000/kv/user-2[0-9]{3}'

# 500 writes/s, about 80% of them to user-1500
$COMPOSE run --rm oha --no-tui -q 500 -n 2500 -c 1000 -t 1s -m PUT -d hello \
  --rand-regex-url 'http://router:8000/kv/user-(1500|1500|1500|1500|[0-2][0-9]{3})'

# Store 100 neighbouring users, then count them with a scan
curl -s -o /dev/null -X PUT -d hello 'localhost:8000/kv/user-[1000-1099]'
curl 'localhost:8000/scan?from=user-1000&to=user-1099'

# Stop
$COMPOSE down
```

### Demo 5 setup: directory-based sharding

```sh
COMPOSE="docker compose -f docker-compose.5-directory.yml"

# Three shards that share the keys by hash, and an empty fourth one
$COMPOSE up -d --force-recreate

# Or the same with a faster fourth one (2 ms per write: 500 writes/s)
SHARD_D_WRITE_COST_MS=2 $COMPOSE up -d --force-recreate

# Store a key, move it to shard-d, read it back from its new home
curl -X PUT -d hello localhost:8000/kv/user-1500
curl -X PUT -d shard-d localhost:8000/directory/user-1500
curl localhost:8000/kv/user-1500

# List the moved keys
curl localhost:8000/directory

# 400 writes/s to that one key
$COMPOSE run --rm oha --no-tui -q 400 -n 2000 -c 1000 -t 1s -m PUT -d hello \
  --rand-regex-url 'http://router:8000/kv/user-1500'

# Restart the router: the directory is gone
$COMPOSE restart router
curl localhost:8000/directory

# Stop
$COMPOSE down
```

### Demo 6 setup: adding a shard

```sh
COMPOSE="docker compose -f docker-compose.6-adding-a-shard.yml"
THREE=http://shard-a:8000,http://shard-b:8000,http://shard-c:8000
FOUR=$THREE,http://shard-d:8000

# Three shards in use (STRATEGY=modulo for the other way of hashing)
STRATEGY=ring NODES=$THREE $COMPOSE up -d --force-recreate

# Store 1800 keys, 50 at a time
curl -Z --parallel-max 50 -s -o /dev/null -X PUT -d hello 'localhost:8000/kv/user-[0001-1800]'

# Add the fourth shard: only the router is replaced, the shards keep their data
STRATEGY=ring NODES=$FOUR $COMPOSE up -d router

# Some keys cannot be found any more
curl localhost:8000/kv/user-0001

# Move every key to its new owner
curl -X POST localhost:8000/rebalance

# Stop
$COMPOSE down
```

### Demo 7 setup: across shards

```sh
COMPOSE="docker compose -f docker-compose.7-across-shards.yml"

# Three shards, keys assigned by hash
$COMPOSE up -d --force-recreate

# Store 1800 users, 50 at a time
curl -Z --parallel-max 50 -s -o /dev/null -X PUT -d hello 'localhost:8000/kv/user-[0001-1800]'

# Count the keys in the whole cluster
curl localhost:8000/count

# Write two keys in one request
curl -X PUT -d '{"account-alice":"100","account-bob":"100"}' localhost:8000/kv-many

# Make shard-c slow (1 second per answer), then normal again
curl -X PUT -d 1000 localhost:8003/delay
curl -X PUT -d 0 localhost:8003/delay

# Stop shard-b, count anyway, start it again (it comes back empty)
$COMPOSE stop shard-b
curl 'localhost:8000/count?partial=yes'
$COMPOSE start shard-b

# Stop
$COMPOSE down
```

### Demo 8 setup: replicated shards

```sh
COMPOSE="docker compose -f docker-compose.8-replicated-shards.yml"

# Three shards of two replicas each; a write counts if one replica stored it
$COMPOSE up -d --force-recreate

# Or: a write counts only if every replica stored it (replaces only the router)
WRITES_NEED=all $COMPOSE up -d router

# Store 1800 users, 50 at a time
curl -Z --parallel-max 50 -s -o /dev/null -X PUT -d hello 'localhost:8000/kv/user-[0001-1800]'

# Kill one replica of shard b, write to that shard, bring the replica back
$COMPOSE stop shard-b1
curl -X PUT -d hello localhost:8000/kv/user-0002
$COMPOSE start shard-b1

# It came back empty: copy over what it is missing
curl -X POST localhost:8000/repair

# Stop
$COMPOSE down
```

### With any setup

```sh
# Write, read, look at the nodes
curl -X PUT -d hello localhost:8000/kv/greeting
curl localhost:8000/kv/greeting
curl localhost:8000/status
```

Inside the Compose network the router is `http://router:8000`, which is the
address oha uses. A single node can be inspected directly on
`localhost:8001` (`shard-a`), `localhost:8002` (`shard-b`),
`localhost:8003` (`shard-c`) and, in demos 5 and 6, `localhost:8004`
(`shard-d`). Demo 8 has six nodes, `shard-a1` to `shard-c2`, on
`localhost:8001` to `localhost:8006`. A node can be made slow for demo 7 with
`curl -X PUT -d 1000 localhost:8003/delay` (milliseconds; `0` to undo).

Drop `--no-tui` to watch oha's live dashboard instead of the text report.

## Files

| File | Purpose |
|---|---|
| `shard.js` | The storage node, with the two artificial limits |
| `router.js` | The router and its strategies |
| `docker-compose.1-write-limit.yml` | Setup for demo 1: the write speed of a node can be changed |
| `docker-compose.2-storage-limit.yml` | Setup for demo 2: fixed limits |
| `docker-compose.3-sharding.yml` | Setup for demo 3: the router shards with `modulo` |
| `docker-compose.4-key-choice.yml` | Setup for demo 4: three shards, strategy `range` or `modulo` |
| `docker-compose.5-directory.yml` | Setup for demo 5: three shards by hash, an empty fourth whose write speed can be changed, strategy `directory` |
| `docker-compose.6-adding-a-shard.yml` | Setup for demo 6: four nodes, of which the router uses three or four, strategy `modulo` or `ring` |
| `docker-compose.7-across-shards.yml` | Setup for demo 7: three shards, strategy `modulo` |
| `docker-compose.8-replicated-shards.yml` | Setup for demo 8: three shards of two replicas each, strategy `replicated` |
| `demo-1-write-limit.sh` | Scripted demo 1: growing traffic, scaling up, replicas |
| `demo-2-storage-limit.sh` | Scripted demo 2: a full node, replicas |
| `demo-3-sharding.sh` | Scripted demo 3: both loads again, on one node and on three shards |
| `demo-4-key-choice.sh` | Scripted demo 4: newest keys, scans and a hot key, by range and by hash |
| `demo-5-directory.sh` | Scripted demo 5: a hot shard fixed, a hot key isolated and then given a faster shard, the directory lost |
| `demo-6-adding-a-shard.sh` | Scripted demo 6: three shards become four, by modulo and by ring |
| `demo-7-across-shards.sh` | Scripted demo 7: a count and a two-key write with a slow shard, a dead shard, and after it returns |
| `demo-8-replicated-shards.sh` | Scripted demo 8: a replica dies, returns empty and is repaired; loose and strict writes |

## What this is not

This is a teaching toy, not a database. Its simplifications are deliberate:

- **Data lives in memory.** Restarting a container wipes it.
- **The limits are simulated.** A refused or accepted write "costs" a fixed
  5 ms of waiting, not real disk work.
- **Moving keys is not safe under traffic.** The directory demo and the
  rebalance move keys while nobody writes to them. A write arriving mid-move
  could be lost. Real systems keep serving during a rebalance, which is a
  large part of what makes it hard.
- **Writes across shards are not transactions.** `PUT /kv-many` can end up
  half done, and nothing cleans up afterwards. Real systems use a protocol
  such as two-phase commit, or avoid such writes by design.
- **Shards have no copies until demo 8, and those copies are basic.** The
  router does not know which replica is up to date, repair only copies
  missing keys, and there is no majority rule. 
- **The cluster can only grow.** The rebalance does not handle removing a
  shard.
- **Mirroring has no failure handling.** If one node refuses a write, the  
  others keep it. Keeping copies in agreement is part of CAP theorem.