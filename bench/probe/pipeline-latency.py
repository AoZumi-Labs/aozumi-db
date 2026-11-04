"""Per-request latency of a libSQL HTTP server over one kept-alive
connection, with only the Python standard library: for the server's own
machine, where Node may not be installed, to see the server without the
network in front of it. Same requests as pipeline-latency.mjs (sequential
part), same tables (run that one with --setup first, or pass --setup here).

    DB_URL=http://127.0.0.1:8080 DB_TOKEN=... python3 bench/probe/pipeline-latency.py
"""
import http.client, json, os, sys, time, urllib.parse

url = urllib.parse.urlsplit(os.environ["DB_URL"])
token = os.environ.pop("DB_TOKEN")
conn = (http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection)(url.hostname, url.port)
headers = {"authorization": f"Bearer {token}", "content-type": "application/json"}


def call(method, path, body=None):
    conn.request(method, path, body=None if body is None else json.dumps(body), headers=headers)
    response = conn.getresponse()
    data = response.read()
    if response.status != 200 or b'"type":"error"' in data:
        raise SystemExit(f"{path}: HTTP {response.status} {data[:200]!r}")
    return data


def pipe(requests):
    return call("POST", "/v2/pipeline", {"baton": None, "requests": requests + [{"type": "close"}]})


def integer(v):
    return {"type": "integer", "value": str(v)}


def execute(sql, args=()):
    return {"type": "execute", "stmt": {"sql": sql, "args": list(args)}}


if "--setup" in sys.argv:
    pipe([execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)"), execute("CREATE TABLE log (id INTEGER PRIMARY KEY, n INTEGER, body TEXT)")])
    pipe([execute("BEGIN")] + [execute("INSERT INTO kv VALUES (?, ?)", [integer(k), {"type": "text", "value": f"value-{k}"}]) for k in range(10000)] + [execute("COMMIT")])


def stats(name, xs):
    xs.sort()
    p = lambda q: round(xs[min(len(xs) - 1, int(len(xs) * q))], 3)
    return {"name": name, "n": len(xs), "p50": p(0.5), "p90": p(0.9), "p99": p(0.99), "max": round(xs[-1], 3)}


def timed(name, n, f):
    xs = []
    for i in range(n):
        t = time.perf_counter()
        f(i)
        xs.append((time.perf_counter() - t) * 1000)
    return stats(name, xs)


timed("warmup", 30, lambda i: call("GET", "/health"))
results = [
    timed("health", 600, lambda i: call("GET", "/health")),
    timed("point read", 600, lambda i: pipe([execute("SELECT v FROM kv WHERE k = ?", [integer((i * 7919) % 10000)])])),
    timed("single-row insert", 300, lambda i: pipe([execute("INSERT INTO log (n, body) VALUES (?, 'probe')", [integer(i)])])),
    timed("10-row transaction", 100, lambda i: pipe([execute("BEGIN")] + [execute("INSERT INTO log (n, body) VALUES (?, 'txn')", [integer(i * 10 + j)]) for j in range(10)] + [execute("COMMIT")])),
]
print(json.dumps({"probe": "pipeline-latency.py", "results": results}))
