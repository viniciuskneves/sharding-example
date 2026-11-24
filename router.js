// The front door of the cluster. Zero dependencies: only Node built-ins.
//
// Clients talk only to the router. The router holds NO data: for every
// request it decides which node(s) should handle it and passes it on.
// That decision is the STRATEGY, and it is the whole subject of these demos.
//
// Strategies so far:
//
//   mirror -> every node stores EVERY key (this is replication, as in the
//             CAP demo). A write goes to all nodes, a read can go to any.
//             With a single node in NODES this is simply "one database".
//
//   modulo -> each key lives on exactly ONE node (this is sharding). The
//             node is picked by hashing the key and taking the remainder of
//             dividing by the number of nodes. Reads and writes for a key
//             go to that node only.
//
//   range  -> also sharding, but the node is picked by the key's place in
//             alphabetical order: RANGE_SPLITS cuts the keys into one
//             stretch per node. Neighbouring keys live on the same node.
//
//   directory -> sharding by hash like "modulo", plus a table of
//             exceptions ("this key lives on that node") that can be edited
//             while the router runs. A key can be moved to any node.
//
//   replicated -> sharding AND replication together. The keys are split
//             over shards by hash, and every shard is a small group of
//             nodes that mirror each other. See demo 8.
//
//   ring   -> sharding by hash again, but arranged so that adding a node
//             changes the owner of as few keys as possible ("consistent
//             hashing"). See ringOwner() below.

const http = require('node:http');
const crypto = require('node:crypto');
const dns = require('node:dns');

// ---------- configuration ----------

const PORT = Number(process.env.PORT || 8000);
const STRATEGY = (process.env.STRATEGY || 'mirror').toLowerCase();

// Comma-separated list of the nodes behind this router,
// e.g. "http://shard-a:8000,http://shard-b:8000"
const NODES_SETTING = (process.env.NODES || '').split(',').map((n) => n.trim()).filter(Boolean);

// Only for the "replicated" strategy: the shards, each one a group of nodes
// that hold the same keys ("replicas"). Shards are separated by ";" and the
// replicas of a shard by ",", e.g. two shards of two replicas each:
//   "http://shard-a1:8000,http://shard-a2:8000;http://shard-b1:8000,http://shard-b2:8000"
const SHARD_GROUPS = (process.env.SHARD_GROUPS || '')
  .split(';')
  .map((group) => group.split(',').map((n) => n.trim()).filter(Boolean))
  .filter((group) => group.length > 0);

// Only for the "replicated" strategy: how many replicas of a shard must
// store a write for it to count as done.
//   "one" -> at least one. Writes keep working while a replica is down, and
//            that replica misses them.
//   "all" -> every one. The replicas never disagree, but a single replica
//            that is down blocks every write to its shard.
const WRITES_NEED = (process.env.WRITES_NEED || 'one').toLowerCase();

// With "replicated", the nodes are simply all the replicas of all shards.
const NODES = STRATEGY === 'replicated' ? SHARD_GROUPS.flat() : NODES_SETTING;

// Only for the "range" strategy: the keys at which one node's stretch ends
// and the next one's begins, in order. Three nodes need two of them, e.g.
// "user-1000,user-2000":
//   first node:   every key before user-1000
//   second node:  user-1000 up to (not including) user-2000
//   third node:   user-2000 and everything after it
const RANGE_SPLITS = (process.env.RANGE_SPLITS || '').split(',').map((s) => s.trim()).filter(Boolean);

// Only for the "directory" strategy: extra nodes that own no keys by hash.
// A key only lives on one of them if the directory says so, which makes
// them suitable as a dedicated home for a key that was moved.
const DIRECTORY_ONLY_NODES = (process.env.DIRECTORY_ONLY_NODES || '').split(',').map((n) => n.trim()).filter(Boolean);

// Every node the router knows about.
const ALL_NODES = [...NODES, ...DIRECTORY_ONLY_NODES];

// "http://shard-a:8000" -> "shard-a". Only used to make output readable.
const nameOf = (node) => new URL(node).hostname;

// ---------- talking to nodes ----------

// Keep connections to the nodes open and reuse them. Opening a new connection
// for every request would make the router itself the slow part.
const agent = new http.Agent({ keepAlive: true });

// Turn a node's hostname into an IP address by asking the DNS server directly.
//
// Why not Node's default lookup? The default runs on a pool of only 4 threads
// and blocks one of them until the system resolver gives up. The name of a
// node that is DOWN no longer resolves, and on some Docker setups (Linux, for
// one) each attempt hangs for seconds. A few of those fill the pool, lookups
// for HEALTHY nodes get stuck behind them, and one dead shard would make the
// whole cluster look dead. dns.resolve4 does not use that pool.
// (The CAP example's server.js has the same function for the same reason.)
function lookupNode(hostname, options, callback) {
  dns.resolve4(hostname, (err, addresses) => {
    if (err) return callback(err);
    // Depending on its settings, Node asks for one address or for all of them.
    if (options.all) return callback(null, addresses.map((address) => ({ address, family: 4 })));
    callback(null, addresses[0], 4);
  });
}

// How long the router waits for a node before giving up on it.
const NODE_TIMEOUT_MS = 3000;

// Pass one request on to one node. Resolves with { node, code, body }.
// A node that cannot be reached, or does not answer in time, is reported as
// a 502 instead of throwing.
function forward(node, method, path, body) {
  return new Promise((resolve) => {
    const req = http.request(node + path, { method, agent, lookup: lookupNode }, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => resolve({ node, code: res.statusCode, body: JSON.parse(data) }));
    });
    const timer = setTimeout(() => req.destroy(new Error('no answer in time')), NODE_TIMEOUT_MS);
    req.on('close', () => clearTimeout(timer));
    req.on('error', (err) =>
      resolve({ node, code: 502, body: { error: `${nameOf(node)} unreachable: ${err.message}` } }),
    );
    req.end(body);
  });
}

// ---------- strategy: mirror ----------

// Write to ALL nodes at the same time and wait for all of them.
//
// Two things follow, and they are the point of the first two demos:
//   - The write is only as fast as one node: every node must do every write,
//     so three nodes get through the same number of writes as one.
//   - The cluster only holds as much as one node: every node stores every
//     key, so they all fill up at the same moment.
async function mirrorPut(path, value) {
  const replies = await Promise.all(NODES.map((node) => forward(node, 'PUT', path, value)));

  // If any node refused, the write failed. Report the first refusal.
  // KNOWN SIMPLIFICATION: the nodes that did accept it keep it. Keeping
  // copies in agreement when some writes fail is the CAP demo's subject.
  const failed = replies.find((reply) => reply.code !== 200);
  if (failed) return [failed.code, failed.body];

  return [200, { ...replies[0].body, node: undefined, storedOn: NODES.map(nameOf) }];
}

// Read from ONE node, taking turns ("round-robin"). All nodes hold the same
// data, so any of them can answer, and the read load is shared between them.
let nextReader = 0;
async function mirrorGet(path) {
  const node = NODES[nextReader++ % NODES.length];
  const reply = await forward(node, 'GET', path);
  return [reply.code, { ...reply.body, node: undefined, readFrom: nameOf(node) }];
}

// ---------- strategies: modulo and range (sharding) ----------

// Turn a key into a number between 0 and about 4 billion.
//
// A hash function scrambles its input: the result looks random, but the same
// key always gives the same number. MD5 produces 16 bytes; the first 4, read
// as one unsigned integer, are plenty to pick a node with.
// (MD5 is broken for security purposes. Here it is only used to spread keys.)
function hash(key) {
  return crypto.createHash('md5').update(key).digest().readUInt32BE(0);
}

// modulo: the hash divided by the number of nodes leaves a remainder of
// 0, 1, ... up to (number of nodes - 1), and that remainder is the node's
// position in NODES.
//
// Because hashes look random, each node ends up owning close to an equal
// share of the keys. Close to, not exactly: see demo 3. The flip side is that
// keys which are neighbours in the alphabet end up on unrelated nodes.
function moduloPosition(key) {
  return hash(key) % NODES.length;
}

// range: the node's position is the number of split points the key has
// already passed. With splits at user-1000 and user-2000:
//   "user-0421" passed none   -> position 0
//   "user-1000" passed one    -> position 1
//   "user-2750" passed both   -> position 2
//
// Keys are compared as text, letter by letter, which is why the numbers in
// the demo keys are padded with zeros ("user-0421", not "user-421").
//
// Neighbouring keys stay together, which makes scans cheap (see scan()
// below). Whether the nodes get equal shares depends entirely on which keys
// the application actually uses: see demo 4.
function rangePosition(key) {
  return RANGE_SPLITS.filter((split) => key >= split).length;
}

// ring ("consistent hashing"): picture every possible hash value laid out on
// a circle, from 0 round to about 4 billion and back to 0.
//
//   - Each node is placed on the circle at RING_POINTS spots. A spot is the
//     hash of the node's name plus a counter: "shard-a#0", "shard-a#1", ...
//   - A key is placed on the circle at its own hash.
//   - The key belongs to the node whose spot comes next, going clockwise.
//
// What makes this different from modulo: a node's spots depend only on that
// node's NAME, never on how many nodes there are. So when a node is added,
// every existing spot stays exactly where it was. The new node's spots each
// take over the piece of the circle just before them, and every key outside
// those pieces keeps its owner. With modulo, changing the number of nodes
// changes the result of the division for almost every key.
//
// Why many spots per node rather than one: with a single spot each, the
// pieces of the circle would be of very unequal size, and so would the
// nodes' shares. Many small pieces average out.
const RING_POINTS = 100;

// Every spot on the circle, sorted by position: [{ at, node }, ...].
// Built once at startup, from NODES alone.
const ring = NODES.flatMap((node) =>
  Array.from({ length: RING_POINTS }, (_, i) => ({ at: hash(`${nameOf(node)}#${i}`), node })),
).sort((a, b) => a.at - b.at);

// The first spot at or after the key's position. If there is none, the key
// is past the last spot, and going clockwise wraps around to the first one.
// (Real systems find the spot with a binary search; a plain scan through a
// few hundred spots is fast enough here and easier to read.)
function ringOwner(key) {
  const position = hash(key);
  return (ring.find((spot) => spot.at >= position) ?? ring[0]).node;
}

// The node a key belongs to when nobody decided otherwise.
//
// The router needs no table of "which key is where" for this. The answer is
// computed from the key alone, the same way on every request.
function computedOwnerOf(key) {
  if (STRATEGY === 'range') return NODES[rangePosition(key)];
  if (STRATEGY === 'ring') return ringOwner(key);
  return NODES[moduloPosition(key)];
}

// The directory: key -> node, for the keys that were moved by hand.
//
// It holds only the EXCEPTIONS. Every key that is not in here still lives
// where its hash says, so the table stays small.
//
// This Map is the one piece of state the router has, and it lives in memory
// only. The price of being able to move keys is that the router can no
// longer work out where everything is from the keys alone: see demo 5.
const directory = new Map();

// The node that owns a key: the directory's answer if it has one, otherwise
// the computed one. (With any other strategy the directory is always empty.)
function ownerOf(key) {
  return directory.get(key) ?? computedOwnerOf(key);
}

// Pass the request on to the ONE node that owns the key, and return its
// answer unchanged (the answer already says which node produced it).
//
// This is where sharding gets its scaling from:
//   - Each node does only the writes for its own keys, so the nodes work at
//     the same time on different writes and their speeds add up.
//   - Each node stores only its own keys, so their room adds up too.
async function shardRequest(method, key, path, value) {
  const reply = await forward(ownerOf(key), method, path, value);
  return [reply.code, reply.body];
}

// ---------- strategy: directory ----------

// Move one key to the node with the given name and remember where it went.
//
// KNOWN SIMPLIFICATION: this is only safe while nobody is writing to the
// key. A write that arrives between the steps below can be lost or end up on
// the old node. Moving data under live traffic is a problem of its own.
async function moveKey(key, path, targetName) {
  const target = ALL_NODES.find((node) => nameOf(node) === targetName);
  if (!target) return [400, { error: `unknown node "${targetName}": use one of ${ALL_NODES.map(nameOf).join(', ')}` }];

  const from = ownerOf(key);
  if (from !== target) {
    // 1. Copy the value to its new home (if the key has one yet).
    const current = await forward(from, 'GET', path);
    if (current.code === 200) {
      const copied = await forward(target, 'PUT', path, current.body.value);
      if (copied.code !== 200) return [copied.code, copied.body];
    }

    // 2. Point the router at the new home. A key that moves back to where
    //    its hash says needs no entry at all.
    if (target === computedOwnerOf(key)) directory.delete(key);
    else directory.set(key, target);

    // 3. Remove the old copy.
    if (current.code === 200) await forward(from, 'DELETE', path);
  }

  return [200, { key, movedFrom: nameOf(from), movedTo: nameOf(target), directoryEntries: directory.size }];
}

// The whole directory: which keys were moved, and where to.
function listDirectory() {
  const entries = {};
  for (const [key, node] of directory) entries[key] = nameOf(node);
  return [200, { entries }];
}

// ---------- strategy: replicated (sharding + replication) ----------

// Two steps for every key:
//   1. WHICH SHARD? By hash, exactly like "modulo", but counting shards
//      instead of nodes. This is what spreads the data and the writes.
//   2. WHICH NODES? All the replicas of that shard. This is what keeps the
//      data alive when a node dies.
function replicasOf(key) {
  return SHARD_GROUPS[hash(key) % SHARD_GROUPS.length];
}

// Write to every replica of the key's shard at the same time.
//
// The replicas of ONE shard all do the same write, like the mirrored nodes
// of demos 1 and 2: a shard with two replicas is no faster and has no more
// room than a shard with one. The capacity of the cluster comes from the
// number of SHARDS; the replicas are paid for in machines.
async function replicatedPut(path, value) {
  const replicas = replicasOf(decodeURIComponent(path.slice(4)));
  const replies = await Promise.all(replicas.map((node) => forward(node, 'PUT', path, value)));

  const storedOn = replies.filter((reply) => reply.code === 200).map((reply) => nameOf(reply.node));
  const missed = replies.filter((reply) => reply.code !== 200).map((reply) => nameOf(reply.node));

  // The decision the CAP example is about, made for one shard: is a write
  // that could not reach every replica good enough?
  const enough = WRITES_NEED === 'all' ? missed.length === 0 : storedOn.length > 0;
  if (!enough) {
    // KNOWN SIMPLIFICATION: with "all", the replicas that did store the
    // write keep it although the write is reported as refused. The CAP
    // example's server.js has the same gap and explains it.
    return [503, { error: `write refused: not stored on ${missed.join(', ')}`, writesNeed: WRITES_NEED }];
  }
  return [200, { ...replies.find((reply) => reply.code === 200).body, node: undefined, storedOn, missed }];
}

// Read from the first replica of the key's shard that answers.
//
// A replica that is down (502) is skipped and the next one is asked, which
// is what makes a dead node harmless for reads. Whatever a LIVE replica
// says is taken as the answer, including "key not found".
//
// KNOWN SIMPLIFICATION: the router does not know whether a replica is up to
// date. A replica that was away and came back without its data answers
// "not found" for keys its shard does have, until repair() has run. Real
// systems keep track of which replicas are behind and do not read from them.
async function replicatedGet(path) {
  let reply;
  for (const node of replicasOf(decodeURIComponent(path.slice(4)))) {
    reply = await forward(node, 'GET', path);
    if (reply.code !== 502) break;
  }
  return [reply.code, reply.body];
}

// Bring every replica up to date with the others of its shard: whatever key
// one replica has and another lacks is copied over.
//
// This is what a replica needs after it was away. The answer says how many
// keys each node received.
//
// KNOWN SIMPLIFICATIONS: only MISSING keys are copied. A key that two
// replicas both have, with different values, is left alone: deciding which
// value wins is the conflict problem of the CAP example. And like every
// other data move in these demos, it is only safe while nobody is writing.
async function repair() {
  const startedAt = Date.now();
  const copies = [];

  for (const replicas of SHARD_GROUPS) {
    // What does each reachable replica of this shard hold?
    const listings = (await Promise.all(replicas.map((node) => forward(node, 'GET', '/scan')))).filter(
      (reply) => reply.code === 200,
    );
    // For each replica: every key that another replica has and this one
    // lacks goes on the list of copies to make.
    for (const target of listings) {
      const has = new Set(target.body.keys);
      for (const holder of listings) {
        for (const key of holder.body.keys) {
          if (has.has(key)) continue;
          has.add(key);
          copies.push({ key, from: holder.node, to: target.node });
        }
      }
    }
  }

  const copiedTo = {};
  await inParallel(copies, 50, async ({ key, from, to }) => {
    const path = `/kv/${encodeURIComponent(key)}`;
    const current = await forward(from, 'GET', path);
    if (current.code !== 200) return;
    const copied = await forward(to, 'PUT', path, current.body.value);
    if (copied.code === 200) copiedTo[nameOf(to)] = (copiedTo[nameOf(to)] ?? 0) + 1;
  });

  return [200, { copiedTo, tookSeconds: Number(((Date.now() - startedAt) / 1000).toFixed(1)) }];
}

// ---------- operations that span several shards ----------

// Everything above this point deals with ONE key, which lives on ONE shard.
// The two operations below involve several shards at once, and that is where
// sharding gets awkward: see demo 7.

// How many keys are stored in the whole cluster?
//
// No single shard knows. The router has to ask every one of them and add up
// the answers, which has two consequences:
//   - The count is only as fast as the SLOWEST shard, because the router
//     cannot answer before the last one has.
//   - If any shard is down, the true total cannot be known. The router can
//     refuse to answer (the default), or, with allowPartial, answer with
//     what the reachable shards hold and say that it is incomplete. That is
//     the consistency-or-availability choice of the CAP example again.
async function count(allowPartial) {
  const startedAt = Date.now();
  const replies = await Promise.all(ALL_NODES.map((node) => forward(node, 'GET', '/status')));
  const tookMs = Date.now() - startedAt;

  const answered = replies.filter((reply) => reply.code === 200);
  const missing = replies.filter((reply) => reply.code !== 200).map((reply) => nameOf(reply.node));
  const total = answered.reduce((sum, reply) => sum + reply.body.keys, 0);

  if (missing.length && !allowPartial) {
    return [503, { error: `cannot count: no answer from ${missing.join(', ')}`, nodesAsked: replies.length, tookMs }];
  }
  return [200, { count: total, complete: missing.length === 0, missing, nodesAsked: replies.length, tookMs }];
}

// Write several keys in one request, e.g. {"account-alice":"90","account-bob":"110"}.
//
// On a single database this could be a transaction: either every write
// happens or none does. Here each key goes to its own shard, the shards know
// nothing of each other, and each write succeeds or fails on its own. If one
// shard is down, the request ends up HALF done, and the router can only
// report it.
//
// Making this all-or-nothing across shards takes a protocol of its own
// (two-phase commit is the classic one), which costs extra round trips and
// has its own failure cases. This router does not attempt it.
async function putMany(entries) {
  const results = await Promise.all(
    Object.entries(entries).map(async ([key, value]) => {
      const node = ownerOf(key);
      const reply = await forward(node, 'PUT', `/kv/${encodeURIComponent(key)}`, String(value));
      return { key, node: nameOf(node), ok: reply.code === 200 };
    }),
  );

  const written = {};
  const failed = {};
  for (const { key, node, ok } of results) (ok ? written : failed)[key] = node;

  const allOk = Object.keys(failed).length === 0;
  return [allOk ? 200 : 502, { written, failed, outcome: allOk ? 'all written' : 'HALF DONE: some keys written, some not' }];
}

// ---------- rebalancing ----------

// Run `job` for every item, at most `limit` of them at the same time.
// Each of the `limit` workers keeps taking the next item until none is left.
async function inParallel(items, limit, job) {
  let next = 0;
  const worker = async () => {
    while (next < items.length) await job(items[next++]);
  };
  await Promise.all(Array.from({ length: limit }, worker));
}

// Put every key on the node that owns it NOW.
//
// After the set of nodes changes, some keys sit on a node that is no longer
// their owner, and the router will not look for them there. This walks
// through everything every node holds and moves what is in the wrong place.
// The answer says how many keys had to move: that number is the cost of
// changing the cluster, and it depends heavily on the strategy.
//
// KNOWN SIMPLIFICATIONS:
//   - Only safe while nobody is writing, like moveKey() above.
//   - Only handles a cluster that GROWS. A node that was removed from NODES
//     is unknown to the router, so its keys would be left behind.
async function rebalance() {
  const startedAt = Date.now();

  // 1. Ask every node for all the keys it holds, and keep the misplaced ones.
  const listings = await Promise.all(ALL_NODES.map((node) => forward(node, 'GET', '/scan')));
  let keys = 0;
  const misplaced = [];
  for (const { node, body } of listings) {
    for (const key of body.keys) {
      keys++;
      const owner = ownerOf(key);
      if (owner !== node) misplaced.push({ key, from: node, to: owner });
    }
  }

  // 2. Move them: read the value, write it to the owner, delete the old copy.
  //    Every one of these is a write that a node has to work on, in the same
  //    queue as the application's own writes. Moving data is not free.
  let moved = 0;
  let failed = 0;
  await inParallel(misplaced, 50, async ({ key, from, to }) => {
    const path = `/kv/${encodeURIComponent(key)}`;
    const current = await forward(from, 'GET', path);
    const copied = current.code === 200 ? await forward(to, 'PUT', path, current.body.value) : current;
    if (copied.code !== 200) return failed++;
    await forward(from, 'DELETE', path);
    moved++;
  });

  return [
    200,
    {
      keys,
      moved,
      movedPercent: keys ? Math.round((100 * moved) / keys) : 0,
      failed,
      tookSeconds: Number(((Date.now() - startedAt) / 1000).toFixed(1)),
    },
  ];
}

// ---------- scans ----------

// Which nodes might hold keys between `from` and `to`?
//
//   range:  only the nodes whose stretch overlaps the scan. Usually that is
//           a single node, because neighbouring keys live together.
//   modulo: ALL of them. Hashing scattered the keys, so any node may hold
//           some of the keys in between, and the only way to find out is to
//           ask every one ("scatter-gather").
//   directory: all of them too, including the directory-only nodes.
//   mirror: any single node, since each one has everything.
function nodesForScan(from, to) {
  if (STRATEGY === 'range') return NODES.slice(rangePosition(from), rangePosition(to) + 1);
  if (STRATEGY === 'mirror') return [NODES[0]];
  return ALL_NODES;
}

// Count the keys between `from` and `to` (both included).
// The answer also says which nodes had to be asked and how many keys each
// one contributed: that list is the cost of the scan.
async function scan(from, to) {
  const query = `/scan?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`;
  const replies = await Promise.all(nodesForScan(from, to).map((node) => forward(node, 'GET', query)));

  const failed = replies.find((reply) => reply.code !== 200);
  if (failed) return [failed.code, failed.body];

  const asked = {};
  for (const { node, body } of replies) asked[nameOf(node)] = body.keys.length;
  const count = Object.values(asked).reduce((sum, n) => sum + n, 0);
  return [200, { from, to, count, nodesAsked: replies.length, keysFromEachNode: asked }];
}

// ---------- public API ----------

// One line per node: how full and how busy it is.
async function status() {
  const replies = await Promise.all(ALL_NODES.map((node) => forward(node, 'GET', '/status')));
  const nodes = {};
  for (const { node, code, body } of replies) {
    nodes[nameOf(node)] =
      code === 200
        ? { keys: body.keys, maxKeys: body.maxKeys, writesHandled: body.writesHandled }
        : { error: body.error };
  }
  return [200, { strategy: STRATEGY, nodes }];
}

// ---------- HTTP plumbing ----------

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

// Maps a request to a handler. Every handler returns [httpStatus, jsonBody].
async function route(req) {
  const { pathname, searchParams } = new URL(req.url, 'http://localhost');
  const isKey = pathname.startsWith('/kv/');

  if (pathname === '/status' && req.method === 'GET') return status();

  if (STRATEGY === 'replicated') {
    if (isKey && req.method === 'PUT') return replicatedPut(pathname, await readBody(req));
    if (isKey && req.method === 'GET') return replicatedGet(pathname);

    // POST /repair -> copy to every replica the keys it is missing
    if (pathname === '/repair' && req.method === 'POST') return repair();

    // Scans, counts, multi-key writes and rebalancing are left out for this
    // strategy, to keep it small.
    return [404, { error: 'not found' }];
  }

  // e.g. GET /scan?from=user-1000&to=user-1099
  if (pathname === '/scan' && req.method === 'GET') {
    return scan(searchParams.get('from') ?? '', searchParams.get('to') ?? '');
  }

  if (isKey && STRATEGY === 'mirror') {
    if (req.method === 'PUT') return mirrorPut(pathname, await readBody(req));
    if (req.method === 'GET') return mirrorGet(pathname);
  }

  if (STRATEGY !== 'mirror') {
    // GET /count -> the number of keys in the whole cluster.
    // GET /count?partial=yes -> answer even if some shards are down.
    if (pathname === '/count' && req.method === 'GET') return count(searchParams.get('partial') === 'yes');

    // PUT /kv-many with a JSON object of keys and values -> write them all
    if (pathname === '/kv-many' && req.method === 'PUT') return putMany(JSON.parse(await readBody(req)));
  }

  // POST /rebalance -> move every key to the node that owns it now
  if (pathname === '/rebalance' && req.method === 'POST' && STRATEGY !== 'mirror') return rebalance();

  if (isKey && ['modulo', 'range', 'ring', 'directory'].includes(STRATEGY)) {
    const key = decodeURIComponent(pathname.slice(4));
    if (req.method === 'PUT') return shardRequest('PUT', key, pathname, await readBody(req));
    if (req.method === 'GET') return shardRequest('GET', key, pathname);
  }

  if (STRATEGY === 'directory') {
    // GET /directory -> list the moved keys
    if (pathname === '/directory' && req.method === 'GET') return listDirectory();

    // PUT /directory/{key} with a node name as the body -> move that key
    if (pathname.startsWith('/directory/') && req.method === 'PUT') {
      const key = decodeURIComponent(pathname.slice('/directory/'.length));
      return moveKey(key, `/kv/${encodeURIComponent(key)}`, (await readBody(req)).trim());
    }
  }

  return [404, { error: 'not found' }];
}

if (!['mirror', 'modulo', 'range', 'ring', 'directory', 'replicated'].includes(STRATEGY)) {
  console.error(`unknown STRATEGY "${STRATEGY}": use "mirror", "modulo", "range", "ring", "directory" or "replicated"`);
  process.exit(1);
}

if (STRATEGY === 'range' && RANGE_SPLITS.length !== NODES.length - 1) {
  console.error(`RANGE_SPLITS needs ${NODES.length - 1} keys to cut the keys into ${NODES.length} stretches, got ${RANGE_SPLITS.length}`);
  process.exit(1);
}

if (NODES.length === 0) {
  console.error('NODES (or SHARD_GROUPS, for "replicated") is empty: the router needs at least one node to send requests to');
  process.exit(1);
}

http
  .createServer(async (req, res) => {
    let code, body;
    try {
      [code, body] = await route(req);
    } catch (err) {
      [code, body] = [500, { error: String(err.message || err) }];
    }
    res.writeHead(code, { 'content-type': 'application/json' });
    res.end(JSON.stringify(body) + '\n');
  })
  .listen(PORT, () =>
    console.log(`router listening on :${PORT}, strategy "${STRATEGY}" over ${NODES.map(nameOf).join(', ')}`),
  );

// Node runs as PID 1 inside the container and would otherwise ignore SIGTERM,
// making "docker compose down" wait 10 seconds before killing it.
process.on('SIGTERM', () => process.exit(0));
