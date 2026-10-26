#!/usr/bin/env python3
"""Turn head-to-head output (bench/head-to-head.sh) into results.json and
one SVG bar chart per metric: the median over rounds, one bar per contender.

    python3 bench/chart.py <raw.md> <output dir>
"""
import json
import re
import statistics
import sys
from pathlib import Path

NAMES = {
    "aozumi-full": "aozumi-db",
    "aozumi-normal": "aozumi-db (sync normal)",
    "libsql": "libSQL server",
}
COLORS = {"aozumi-full": "#3b5bdb", "aozumi-normal": "#748ffc", "libsql": "#2f9e44"}
WORKLOADS = {
    "A": "YCSB A: 50% read, 50% update",
    "B": "YCSB B: 95% read, 5% update",
    "C": "YCSB C: 100% read",
    "D": "YCSB D: read latest, 5% insert",
    "E": "YCSB E: short scans, 5% insert",
    "F": "YCSB F: read-modify-write",
}


def ms(cell):
    return float(cell.strip().removesuffix("ms").strip())


def parse(raw):
    runs = {}  # (contender, metric) -> [values]
    contender = None
    for line in raw.splitlines():
        head = re.match(r"### round \d+ (\S+) (\S+)", line)
        if head:
            contender = head.group(1)
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if contender and len(cells) >= 5 and cells[0] in WORKLOADS and cells[1].replace(".", "").isdigit():
            runs.setdefault((contender, f"ycsb_{cells[0]}_ops"), []).append(float(cells[1]))
            runs.setdefault((contender, f"ycsb_{cells[0]}_p99_ms"), []).append(ms(cells[3]))
            failed = cells[6].split()[0] if len(cells) > 6 else "0"
            runs.setdefault((contender, f"ycsb_{cells[0]}_refused"), []).append(float(failed))
        if contender and len(cells) >= 7 and cells[0].endswith("/s") and cells[1].endswith("/s"):
            runs.setdefault((contender, "write_p50_ms"), []).append(ms(cells[2]))
            runs.setdefault((contender, "write_p99_ms"), []).append(ms(cells[4]))
            runs.setdefault((contender, "write_p999_ms"), []).append(ms(cells[5]))
            runs.setdefault((contender, "write_refused"), []).append(float(cells[7].split()[0]) if len(cells) > 7 else 0.0)
    return runs


def chart(title, subtitle, bars, unit, lower_is_better, path):
    bars = sorted(bars, key=lambda b: b[1], reverse=not lower_is_better)
    width, height, left, top, bottom = 760, 420, 40, 90, 110
    plot_h = height - top - bottom
    top_value = max(v for _, v, _ in bars) * 1.1 or 1
    slot = (width - 2 * left) / len(bars)
    bar_w = min(120, slot * 0.6)
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" font-family="Inter, Helvetica, Arial, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        f'<text x="{left}" y="40" font-size="24" font-weight="700" fill="#111">{title}</text>',
        f'<text x="{left}" y="66" font-size="14" fill="#555">{subtitle}</text>',
    ]
    for i in range(1, 5):
        y = top + plot_h - plot_h * i / 4
        out.append(f'<line x1="{left}" x2="{width - left}" y1="{y:.1f}" y2="{y:.1f}" stroke="#e9ecef"/>')
    for n, (label, value, color) in enumerate(bars):
        h = plot_h * value / top_value
        x = left + slot * n + (slot - bar_w) / 2
        y = top + plot_h - h
        shown = f"{value:,.0f}" if value >= 100 else f"{value:.2f}"
        out.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" rx="4" fill="{color}"/>')
        out.append(f'<text x="{x + bar_w / 2:.1f}" y="{y - 8:.1f}" font-size="15" font-weight="600" text-anchor="middle" fill="#111">{shown}</text>')
        out.append(f'<text x="{x + bar_w / 2:.1f}" y="{top + plot_h + 24}" font-size="14" text-anchor="middle" fill="#333">{label}</text>')
    note = "lower is better" if lower_is_better else "higher is better"
    out.append(f'<text x="{left}" y="{height - 20}" font-size="12" fill="#868e96">{unit}, {note}. Median of rounds; same machine, same client. aozumi-db/bench</text>')
    out.append("</svg>")
    path.write_text("\n".join(out))


def main():
    raw, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    runs = parse(raw.read_text())
    medians = {f"{c}/{m}": statistics.median(v) for (c, m), v in runs.items()}
    (out / "results.json").write_text(json.dumps({"runs": {f"{c}/{m}": v for (c, m), v in runs.items()}, "medians": medians}, indent=2))
    contenders = sorted({c for c, _ in runs}, key=list(NAMES).index)

    def bars(metric):
        return [(NAMES[c], medians[f"{c}/{metric}"], COLORS[c]) for c in contenders if f"{c}/{metric}" in medians]

    for w, title in WORKLOADS.items():
        chart(f"YCSB {w}", title + ", 32 clients, 100k records", bars(f"ycsb_{w}_ops"), "successful operations per second", False, out / f"ycsb-{w.lower()}.svg")
    chart("Write latency (p99)", "single-row writes arriving at 1,000/s over 32 connections, open loop", bars("write_p99_ms"), "milliseconds", True, out / "write-p99.svg")

    rows = ["| metric | " + " | ".join(NAMES[c] for c in contenders) + " |", "|---|" + "---:|" * len(contenders)]
    metrics = (
        [f"ycsb_{w}_ops" for w in WORKLOADS]
        + [f"ycsb_{w}_p99_ms" for w in WORKLOADS]
        + [f"ycsb_{w}_refused" for w in WORKLOADS]
        + ["write_p50_ms", "write_p99_ms", "write_p999_ms", "write_refused"]
    )
    for m in metrics:
        cells = []
        for c in contenders:
            v = medians.get(f"{c}/{m}")
            if v is None:
                cells.append("")
            elif m.endswith("_ops") or m.endswith("_refused"):
                cells.append(f"{v:,.0f}")
            else:
                cells.append(f"{v:.2f} ms")
        rows.append(f"| {m} | " + " | ".join(cells) + " |")
    (out / "table.md").write_text("\n".join(rows) + "\n")
    print("\n".join(rows))


if __name__ == "__main__":
    main()
