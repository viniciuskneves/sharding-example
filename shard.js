// A tiny single-node key-value store. Zero dependencies: only Node built-ins.
//
// This is one "database server". It knows nothing about other nodes: no
// peers, no replication, no sharding. Spreading data over several of these is
// entirely the router's job (see router.js).
//
// A laptop cannot honestly show hardware limits, so this node has two
// ARTIFICIAL ones. They are small and exact on purpose, so the demos give the
// same numbers on every machine:
//
//   MAX_KEYS      -> storage limit. Stands in for "the disk is full".
//                    Once the node holds this many keys, new keys get a 507.
//
//   WRITE_COST_MS -> write throughput limit. Stands in for "the disk can only
//                    do so many writes per second". Writes are done one at a
//                    time and each takes this long, so the node cannot go
//                    faster than 1000 / WRITE_COST_MS writes per second.

const http = require('node:http');

// ---------- configuration ----------

const NAME = process.env.NODE_NAME || 'shard';
const PORT = Number(process.env.PORT || 8000);

// How many keys fit on this node.
const MAX_KEYS = Number(process.env.MAX_KEYS || 1000);

// How long one write takes. 5 ms -> at most 200 writes per second.
const WRITE_COST_MS = Number(process.env.WRITE_COST_MS || 5);

// ---------- a switch for the demos ----------

// Extra time this node waits before answering ANY request, in ms. Normally 0.
// A demo can turn this node into a slow machine while it runs, with
// "PUT /delay" and a number of milliseconds as the body. It stands in for a
// node that is overloaded, has a failing disk, or sits behind a bad network.
let delayMs = 0;

// ---------- state ----------

// The whole "database": key -> value.
const store = new Map();

// How many writes this node has been asked to do, including refused ones.
// Shown in /status so you can see how the work was spread across nodes.
let writesHandled = 0;

// ---------- the write queue ----------

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// The tail of the queue: a promise that settles when the last write that was
// queued so far has finished. It starts out already settled (empty queue).
let lastWrite = Promise.resolve();

// How many writes are in the queue right now, counting the one in progress.
let writesInQueue = 0;

// Run `job` after every write queued before it, and resolve with its result.
// Chaining each job onto the previous one is what makes writes happen one at
// a time, in arrival order, no matter how many requests arrive together.
function enqueueWrite(job) {
  // Did this write arrive while the node was already busy? If so it will
  // start the instant the previous write ends ("back to back").
  const backToBack = writesInQueue > 0;
  writesInQueue++;

  const result = lastWrite.then(() => job(backToBack)).finally(() => writesInQueue--);
  // The next write must wait for this one even if this one throws.
  lastWrite = result.catch(() => {});
  return result;
}

// The moment (in ms since this process started) at which the most recent
// write was DUE to finish.
let dueAt = 0;

// Pretend to work on one write for WRITE_COST_MS.
//
// Why not simply sleep(WRITE_COST_MS)? Timers always fire a little late, so
// each 5 ms sleep really takes 6 ms or more and the node would do about 155
// writes per second instead of 200. To keep the limit exact, a back-to-back
// write is due WRITE_COST_MS after the previous write was DUE, not after it
// actually finished. A late timer then makes the next sleep shorter, and the
// lateness never adds up.
async function payWriteCost(backToBack) {
  const now = performance.now();
  // A write that found the node idle starts its clock now.
  const startsAt = backToBack ? dueAt : now;
  dueAt = startsAt + WRITE_COST_MS;
  // Already late? Then do not wait at all: even the shortest possible sleep
  // takes over a millisecond, which would put the node further behind.
  if (dueAt > now) await sleep(dueAt - now);
}

// ---------- public API ----------

function putKey(key, value) {
  return enqueueWrite(async (backToBack) => {
    writesHandled++;

    // Pay the price of one write. While this node "works" on it, every other
    // write waits in the queue. This line IS the throughput limit.
    //
    // A refused write pays it too: the node has to do the work of looking at
    // the request before it can tell that there is no room.
    await payWriteCost(backToBack);

    // Storage limit. Overwriting a key that is already here needs no new
    // room, so only NEW keys are refused.
    if (!store.has(key) && store.size >= MAX_KEYS) {
      return [507, { error: `storage full (${store.size}/${MAX_KEYS} keys): refusing new key` }];
    }

    store.set(key, value);
    return [200, { key, value }];
  });
}

// Removing a key is a write like any other: it waits for its turn in the
// queue and costs the same.
function deleteKey(key) {
  return enqueueWrite(async (backToBack) => {
    writesHandled++;
    await payWriteCost(backToBack);
    const existed = store.delete(key);
    return [200, { key, deleted: existed }];
  });
}

// Reads skip the queue: they do not change anything, so they do not need to
// wait for their turn. (That is why adding copies of the data helps reads.)
function getKey(key) {
  if (!store.has(key)) return [404, { error: 'key not found' }];
  return [200, { key, value: store.get(key) }];
}

// The keys this node holds between `from` and `to` (both included), in order.
// Without a `to` there is no upper end, so "/scan" alone lists every key.
// The node simply looks through everything it has. It cannot know about keys
// on other nodes: putting together a complete answer is the router's job.
function scanKeys(from, to) {
  const keys = [...store.keys()].filter((key) => key >= from && (to === null || key <= to)).sort();
  return [200, { keys }];
}

// How full and how busy this node is.
function status() {
  return [200, { keys: store.size, maxKeys: MAX_KEYS, writesHandled, writeCostMs: WRITE_COST_MS }];
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
  const key = pathname.startsWith('/kv/') ? decodeURIComponent(pathname.slice(4)) : null;

  if (key && req.method === 'PUT') return putKey(key, await readBody(req));
  if (key && req.method === 'GET') return getKey(key);
  if (key && req.method === 'DELETE') return deleteKey(key);
  if (pathname === '/status' && req.method === 'GET') return status();

  // The demo switch: make this node slow (or normal again, with 0).
  if (pathname === '/delay' && req.method === 'PUT') {
    delayMs = Number(await readBody(req)) || 0;
    return [200, { delayMs }];
  }

  if (pathname === '/scan' && req.method === 'GET') {
    return scanKeys(searchParams.get('from') ?? '', searchParams.get('to'));
  }

  return [404, { error: 'not found' }];
}

http
  .createServer(async (req, res) => {
    let code, body;
    try {
      [code, body] = await route(req);
      // A slow node (see delayMs above) holds every answer back for a while.
      if (delayMs && !req.url.startsWith('/delay')) await sleep(delayMs);
    } catch (err) {
      [code, body] = [500, { error: String(err.message || err) }];
    }
    res.writeHead(code, { 'content-type': 'application/json' });
    // Every response says which node produced it.
    res.end(JSON.stringify({ node: NAME, ...body }) + '\n');
  })
  .listen(PORT, () =>
    console.log(`${NAME} listening on :${PORT}, room for ${MAX_KEYS} keys, ${WRITE_COST_MS} ms per write`),
  );

// Node runs as PID 1 inside the container and would otherwise ignore SIGTERM,
// making "docker compose down" wait 10 seconds before killing it.
process.on('SIGTERM', () => process.exit(0));
