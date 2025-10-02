#!/usr/bin/env python3
import argparse
import csv
from collections import defaultdict
from typing import Dict, List, Tuple

import os
import matplotlib.pyplot as plt


def load_csv(path: str):
    times: List[int] = []
    per_conn: Dict[int, List[Tuple[int, float, float, float, float, int]]] = defaultdict(list)
    # Each row: ts_epoch_ms,conn_id,count,drops,p50_ms,p90_ms,p99_ms,max_ms
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        # Detect latency unit columns dynamically: p50_ms|us|ns
        unit_suffix = None
        for s in ("ms", "us", "ns"):
            if f"p50_{s}" in r.fieldnames or f"p90_{s}" in r.fieldnames:
                unit_suffix = s
                break
        if unit_suffix is None:
            raise ValueError("CSV missing p50_[ms|us|ns] columns")
        for row in r:
            ts = int(row["ts_epoch_ms"])  # epoch ms (server-adjusted)
            cid = int(row["conn_id"])
            p50 = float(row.get(f"p50_{unit_suffix}") or 0.0)
            p90 = float(row.get(f"p90_{unit_suffix}") or 0.0)
            p99 = float(row.get(f"p99_{unit_suffix}") or 0.0)
            max_ms = float(row.get(f"max_{unit_suffix}") or 0.0)
            count = int(row["count"]) if row["count"] else 0
            per_conn[cid].append((ts, p50, p90, p99, max_ms, count))
            times.append(ts)
    if times:
        t0 = min(times)
    else:
        t0 = 0
    return t0, per_conn, unit_suffix


def _series(rows: List[Tuple[int, float, float, float, float, int]], metric: str):
    # Extract (ts, value) for given metric name with count>0 and value>0
    idx = {"p50": 1, "p99": 3}[metric]
    out = []
    for r in rows:
        ts = r[0]
        val = r[idx]
        count = r[5]
        if count > 0 and val > 0:
            out.append((ts, val))
    return out


def plot_raw(per_conn, unit: str, title_prefix: str, out: str = None, show: bool = True):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    for cid, rows in sorted(per_conn.items()):
        rows.sort(key=lambda x: x[0])
        s50 = _series(rows, "p50")
        s99 = _series(rows, "p99")
        if s50:
            ax1.plot([t for t, _ in s50], [v for _, v in s50], label=f"conn {cid:02}")
        if s99:
            ax2.plot([t for t, _ in s99], [v for _, v in s99], label=f"conn {cid:02}")

    ax1.set_ylabel(f"p50 latency ({unit})")
    ax2.set_ylabel(f"p99 latency ({unit})")
    ax2.set_xlabel("ts_epoch_ms")
    for ax in (ax1, ax2):
        ax.grid(True, alpha=0.3)
        ax.legend(ncol=5, fontsize=8)
    fig.suptitle(f"{title_prefix} latency (raw)")
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    if out:
        fig.savefig(out, dpi=150)
        print(f"Saved figure to {out}")
    if show:
        plt.show()


def plot_ema(per_conn, unit: str, alpha: float, title_prefix: str, out: str = None, show: bool = True):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    for cid, rows in sorted(per_conn.items()):
        rows.sort(key=lambda x: x[0])
        s50 = _series(rows, "p50")
        s99 = _series(rows, "p99")
        if s50:
            ema = None
            xs = []
            ys = []
            for ts, v in s50:
                ema = v if ema is None else alpha * v + (1 - alpha) * ema
                xs.append(ts)
                ys.append(ema)
            ax1.plot(xs, ys, label=f"conn {cid:02}")
        if s99:
            ema = None
            xs = []
            ys = []
            for ts, v in s99:
                ema = v if ema is None else alpha * v + (1 - alpha) * ema
                xs.append(ts)
                ys.append(ema)
            ax2.plot(xs, ys, label=f"conn {cid:02}")

    ax1.set_ylabel(f"p50 EMA ({unit})")
    ax2.set_ylabel(f"p99 EMA ({unit})")
    ax2.set_xlabel("ts_epoch_ms")
    for ax in (ax1, ax2):
        ax.grid(True, alpha=0.3)
        ax.legend(ncol=5, fontsize=8)
    fig.suptitle(f"{title_prefix} latency (EMA α={alpha})")
    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    if out:
        fig.savefig(out, dpi=150)
        print(f"Saved figure to {out}")
    if show:
        plt.show()


def main():
    parser = argparse.ArgumentParser(description="Plot per-connection latency from CSV")
    parser.add_argument("csv", help="CSV path from --window-csv output")
    parser.add_argument("--out", help="Save figures with this prefix (e.g. out)")
    parser.add_argument("--no-show", action="store_true", help="Do not display on screen")
    parser.add_argument("--title", default="Binance Futures", help="Title prefix")
    parser.add_argument("--ema-alpha", type=float, default=0.2, help="EMA smoothing factor alpha (0-1]")
    args = parser.parse_args()

    t0, per_conn, unit = load_csv(args.csv)
    if not per_conn:
        print("No data found in CSV.")
        return
    # Determine outputs
    def derive_out_paths(out):
        if not out:
            return None, None
        root, ext = os.path.splitext(out)
        if ext:
            return f"{root}_raw{ext}", f"{root}_ema{ext}"
        else:
            return f"{out}_raw.png", f"{out}_ema.png"

    raw_out, ema_out = derive_out_paths(args.out)

    # Plot raw and EMA
    plot_raw(per_conn, unit, args.title, out=raw_out, show=not args.no_show)
    plot_ema(per_conn, unit, args.ema_alpha, args.title, out=ema_out, show=not args.no_show)


if __name__ == "__main__":
    main()
