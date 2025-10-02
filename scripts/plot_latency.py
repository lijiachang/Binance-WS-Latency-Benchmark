#!/usr/bin/env python3
import argparse
import csv
from collections import defaultdict
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt


def load_csv(path: str):
    times: List[int] = []
    per_conn: Dict[int, List[Tuple[int, float, float, float, float, int]]] = defaultdict(list)
    # Each row: ts_epoch_ms,conn_id,count,drops,p50_ms,p90_ms,p99_ms,max_ms
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            ts = int(row["ts_epoch_ms"])  # epoch ms (server-adjusted)
            cid = int(row["conn_id"])
            p50 = float(row["p50_ms"]) if row["p50_ms"] else 0.0
            p90 = float(row["p90_ms"]) if row["p90_ms"] else 0.0
            p99 = float(row["p99_ms"]) if row["p99_ms"] else 0.0
            max_ms = float(row["max_ms"]) if row["max_ms"] else 0.0
            count = int(row["count"]) if row["count"] else 0
            per_conn[cid].append((ts, p50, p90, p99, max_ms, count))
            times.append(ts)
    if times:
        t0 = min(times)
    else:
        t0 = 0
    return t0, per_conn


def plot(per_conn, t0, title_prefix: str, out: str = None, show: bool = True):
    # Prepare figure with two subplots: p50 and p99
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax1, ax2 = axes

    for cid, rows in sorted(per_conn.items()):
        rows.sort(key=lambda x: x[0])
        xs = [(ts - t0) / 1000.0 for (ts, *_rest) in rows]
        p50 = [r[1] for r in rows]
        p99 = [r[3] for r in rows]
        ax1.plot(xs, p50, label=f"conn {cid:02}")
        ax2.plot(xs, p99, label=f"conn {cid:02}")

    ax1.set_ylabel("p50 latency (ms)")
    ax2.set_ylabel("p99 latency (ms)")
    ax2.set_xlabel("time since start (s)")
    ax1.grid(True, alpha=0.3)
    ax2.grid(True, alpha=0.3)
    ax1.legend(ncol=5, fontsize=8)
    ax2.legend(ncol=5, fontsize=8)
    fig.suptitle(f"{title_prefix} bookTicker latency per connection")
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    if out:
        fig.savefig(out, dpi=150)
        print(f"Saved figure to {out}")
    if show:
        plt.show()


def main():
    parser = argparse.ArgumentParser(description="Plot per-connection latency from CSV")
    parser.add_argument("csv", help="CSV path from --window-csv output")
    parser.add_argument("--out", help="Save figure to this path (e.g. out.png)")
    parser.add_argument("--no-show", action="store_true", help="Do not display on screen")
    parser.add_argument("--title", default="Binance Futures", help="Title prefix")
    args = parser.parse_args()

    t0, per_conn = load_csv(args.csv)
    if not per_conn:
        print("No data found in CSV.")
        return
    plot(per_conn, t0, args.title, out=args.out, show=not args.no_show)


if __name__ == "__main__":
    main()

