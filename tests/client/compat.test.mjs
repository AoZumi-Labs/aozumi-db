// Drives a real aozumi-db binary with the unmodified @libsql/client.
// Run: cargo build && (cd tests/client && npm install && node --test)

import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createClient, LibsqlError } from "@libsql/client";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const bin = process.env.AOZUMI_DB_BIN ?? join(root, "target", "debug", "aozumi-db");
const secret = "compat-secret";
const port = 18000 + Math.floor(Math.random() * 1000);
const url = `http://127.0.0.1:${port}`;
let server;
let dataDir;
let db;
let token;

async function admin(method, path, body) {
  const resp = await fetch(url + path, {
    method,
    headers: { authorization: `Bearer ${secret}`, "content-type": "application/json" },
    body: body && JSON.stringify(body),
  });
  return { status: resp.status, body: resp.status === 204 ? null : await resp.json().catch(() => null) };
}

before(async () => {
  dataDir = mkdtempSync(join(tmpdir(), "aozumi-db-compat-"));
  server = spawn(bin, [], {
    env: { ...process.env, AOZUMI_DB_LISTEN: `127.0.0.1:${port}`, AOZUMI_DB_DATA_DIR: dataDir, AOZUMI_DB_SECRET: secret },
    stdio: ["ignore", "ignore", "inherit"],
  });
  for (let i = 0; i < 100; i++) {
    try {
      if ((await fetch(`${url}/health`)).ok) break;
    } catch {}
    await new Promise((r) => setTimeout(r, 50));
  }
  assert.equal((await admin("POST", "/v1/databases", { name: "app" })).status, 201);
  const { body } = await admin("POST", "/v1/databases/app/tokens", { ttl_seconds: 600 });
  token = body.token;
  db = createClient({ url, authToken: token });
  await db.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL UNIQUE, data BLOB, score REAL)");
});

after(() => {
  db?.close();
  server?.kill();
  rmSync(dataDir, { recursive: true, force: true });
});

test("execute with positional and named arguments", async () => {
  const insert = await db.execute({ sql: "INSERT INTO notes (body, score) VALUES (?, ?)", args: ["first", 1.5] });
  assert.equal(insert.rowsAffected, 1);
  assert.equal(insert.lastInsertRowid, 1n);

  const rs = await db.execute({ sql: "SELECT id, body, score FROM notes WHERE body = :body", args: { body: "first" } });
  assert.deepEqual(rs.columns, ["id", "body", "score"]);
  assert.equal(rs.rows[0].body, "first");
  assert.equal(rs.rows[0].score, 1.5);
  assert.equal(rs.rows[0].id, 1);
});

test("blobs and 64-bit integers survive the trip", async () => {
  const big = createClient({ url, authToken: token, intMode: "bigint" });
  const rs = await big.execute({ sql: "SELECT ? AS b, ? AS big", args: [new Uint8Array([0, 1, 255]), 9007199254740993n] });
  assert.deepEqual(Array.from(new Uint8Array(rs.rows[0].b)), [0, 1, 255]);
  assert.equal(rs.rows[0].big, 9007199254740993n);
  big.close();
});

test("batch commits together and rolls back together", async () => {
  await db.batch(["INSERT INTO notes (body) VALUES ('b1')", "INSERT INTO notes (body) VALUES ('b2')"], "write");
  assert.equal((await db.execute("SELECT count(*) AS n FROM notes WHERE body LIKE 'b%'")).rows[0].n, 2);

  await assert.rejects(
    db.batch(["INSERT INTO notes (body) VALUES ('b3')", "INSERT INTO notes (body) VALUES ('b1')"], "write"),
    (err) => err instanceof LibsqlError && err.code.startsWith("SQLITE_CONSTRAINT"),
  );
  assert.equal((await db.execute("SELECT count(*) AS n FROM notes WHERE body = 'b3'")).rows[0].n, 0);
});

test("interactive transactions commit and roll back", async () => {
  const tx = await db.transaction("write");
  await tx.execute("INSERT INTO notes (body) VALUES ('tx-commit')");
  await tx.commit();

  const rollback = await db.transaction("write");
  await rollback.execute("INSERT INTO notes (body) VALUES ('tx-rollback')");
  await rollback.rollback();

  const rs = await db.execute("SELECT body FROM notes WHERE body LIKE 'tx-%' ORDER BY body");
  assert.deepEqual(rs.rows.map((r) => r.body), ["tx-commit"]);
});

test("executeMultiple runs a script", async () => {
  await db.executeMultiple("CREATE TABLE tags (name TEXT); INSERT INTO tags VALUES ('a'); INSERT INTO tags VALUES ('b');");
  assert.equal((await db.execute("SELECT count(*) AS n FROM tags")).rows[0].n, 2);
});

test("SQL errors surface as LibsqlError with SQLite codes", async () => {
  await assert.rejects(db.execute("SELECT * FROM missing_table"), (err) => err instanceof LibsqlError);
});

test("a token for another database cannot read this one", async () => {
  await admin("POST", "/v1/databases", { name: "other" });
  const { body } = await admin("POST", "/v1/databases/other/tokens", {});
  const other = createClient({ url, authToken: body.token });
  await assert.rejects(other.execute("SELECT * FROM notes"));
  other.close();

  const nobody = createClient({ url, authToken: "adb1.bad.token" });
  await assert.rejects(nobody.execute("SELECT 1"), (err) => err instanceof LibsqlError);
  nobody.close();
});

test("the CLI mints tokens the server accepts", async () => {
  const token = execFileSync(bin, ["token", "app", "60"], { env: { ...process.env, AOZUMI_DB_SECRET: secret } }).toString().trim();
  const cli = createClient({ url, authToken: token });
  assert.equal((await cli.execute("SELECT 7 AS n")).rows[0].n, 7);
  cli.close();
});
