// Per-request latency of a libSQL HTTP server, as an app sees it: the
// requests @libsql/client sends (POST /v2/pipeline: execute + close) over
// Node's fetch with keep-alive, one at a time, then 8 clients for 10 s.
//
//   DB_URL=https://db.example.com DB_TOKEN=... node bench/probe/pipeline-latency.mjs
//
// Needs a database with tables kv (k INTEGER PRIMARY KEY, v TEXT, 10,000
// rows) and log (id INTEGER PRIMARY KEY, n INTEGER, body TEXT): run
// `node bench/probe/pipeline-latency.mjs --setup` once. Prints one JSON line;
// never prints the token. ROUNDS sets the sequential rounds (default 600).
const url = process.env.DB_URL;
const token = process.env.DB_TOKEN;
delete process.env.DB_TOKEN;
const n = Number(process.env.ROUNDS || 600);
const pct = (xs, p) => xs[Math.min(xs.length - 1, Math.floor(xs.length * p))];
const stats = (name, xs) => {
  xs.sort((a, b) => a - b);
  const r = (v) => Math.round(v * 1000) / 1000;
  return { name, n: xs.length, p50: r(pct(xs, 0.5)), p90: r(pct(xs, 0.9)), p99: r(pct(xs, 0.99)), max: r(xs[xs.length - 1]) };
};
const int = (v) => ({ type: "integer", value: String(v) });
const pipeline = (requests) => fetch(`${url}/v2/pipeline`, {
  method: "POST",
  headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
  body: JSON.stringify({ baton: null, requests: [...requests, { type: "close" }] }),
});
async function check(name, res) {
  const body = await res.text();
  if (!res.ok || body.includes('"type":"error"')) throw new Error(`${name}: HTTP ${res.status} ${body.slice(0, 120)}`);
}
async function timed(name, rounds, make) {
  const xs = [];
  for (let i = 0; i < rounds; i++) {
    const t = performance.now();
    await check(name, await make(i));
    xs.push(performance.now() - t);
  }
  return stats(name, xs);
}

if (process.argv.includes("--setup")) {
  const x = (sql, args = []) => ({ type: "execute", stmt: { sql, args } });
  await check("tables", await pipeline([x("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)"), x("CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")]));
  await check("rows", await pipeline([x("BEGIN"), ...Array.from({ length: 10_000 }, (_, k) => x("INSERT INTO kv VALUES (?, ?)", [int(k), { type: "text", value: `value-${k}` }])), x("COMMIT")]));
  console.log(JSON.stringify({ setup: "done" }));
  process.exit(0);
}

const out = { at: new Date().toISOString(), node: process.versions.node, arch: process.arch, results: [] };
await timed("warmup", 30, () => fetch(`${url}/health`));
out.results.push(await timed("health", n, () => fetch(`${url}/health`)));
out.results.push(await timed("point read", n, (i) => pipeline([{ type: "execute", stmt: { sql: "SELECT v FROM kv WHERE k = ?", args: [int((i * 7919) % 10000)] } }])));
out.results.push(await timed("single-row insert", Math.floor(n / 2), (i) => pipeline([{ type: "execute", stmt: { sql: "INSERT INTO log (n, body) VALUES (?, 'probe')", args: [int(i)] } }])));
out.results.push(await timed("10-row transaction", Math.floor(n / 6), (i) => pipeline([
  { type: "execute", stmt: { sql: "BEGIN" } },
  ...Array.from({ length: 10 }, (_, j) => ({ type: "execute", stmt: { sql: "INSERT INTO log (n, body) VALUES (?, 'txn')", args: [int(i * 10 + j)] } })),
  { type: "execute", stmt: { sql: "COMMIT" } },
])));
const stopAt = performance.now() + 10_000;
const all = [];
await Promise.all(Array.from({ length: 8 }, async (_, c) => {
  let i = c;
  while (performance.now() < stopAt) {
    const t = performance.now();
    await check("concurrent", await pipeline([{ type: "execute", stmt: { sql: "SELECT v FROM kv WHERE k = ?", args: [int((i++ * 104729) % 10000)] } }]));
    all.push(performance.now() - t);
  }
}));
out.results.push({ ...stats("point read, 8 clients, 10 s", all), opsPerSecond: Math.round(all.length / 10) });
console.log(JSON.stringify({ probe: "pipeline-latency", ...out }));
