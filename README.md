# Binance WS Latency Benchmark

Measure per-connection latency for Binance Futures bookTicker using N parallel WebSockets. Records histograms, prints periodic summaries, and optionally exports 1-second window stats to CSV for plotting.

## Build

```
cargo build --release
```

## Run examples

- 10 connections, BTCUSDT only, run 2 minutes, print every 5s, export per-second windows:

```
cargo run --release -- \
  --connections 10 \
  --symbol BTCUSDT \
  --duration-secs 120 \
  --print-interval-secs 5 \
  --latency-unit us \
  --window-csv out.csv
```

- High volume (all symbols):

```
cargo run --release -- --connections 10 --all --duration-secs 60 --latency-unit ns --window-csv out.csv
```

Notes:
- `--latency-unit` controls display and CSV units for latency percentiles: `ms|us|ns` (default: `us`).
- `--all` is very high volume; start with a single symbol first.
- App-level ping is enabled by default (`--ping-interval-secs 15`). You can disable with `--ping-interval-secs 0`.

## Output

- Periodic summary lines show cumulative p50/p90/p99 and max per connection.
- If `--window-csv` is specified, a CSV with per-second window stats is written at exit.

CSV columns:
```
ts_epoch_ms,conn_id,count,drops,p50_<unit>,p90_<unit>,p99_<unit>,max_<unit>
```
Where `<unit>` is `ms`, `us`, or `ns` depending on `--latency-unit` (time reference column `ts_epoch_ms` remains milliseconds).

## Plot

Requires Python 3 and `matplotlib`.

- Save two figures (raw + EMA), auto-detect unit (ms/us/ns) from CSV header, filter out zero values:

```
python3 scripts/plot_latency.py out.csv --out latency
# or specify extension
python3 scripts/plot_latency.py out.csv --out latency.png
```

Outputs:
- `latency_raw.png`: p50/p99 vs `ts_epoch_ms` (x-axis is epoch ms; y-axis is latency in the CSV unit). Zero/count=0 rows are excluded.
- `latency_ema.png`: per-connection EMA of p50/p99 vs `ts_epoch_ms`. Tune smoothing with `--ema-alpha` (default 0.2).


How to run

- Build:
    - cd binance_ws_latency_benchmark
    - cargo build --release
- Example run (10 conns, BTCUSDT, 2 mins, print every 5s, export windows):
    - cargo run --release -- --connections 10 --symbol BTCUSDT --duration-secs 120 --print-interval-secs 5 --latency-unit us --window-csv out.csv
- All symbols (very high volume — verify capacity first):
    - cargo run --release -- --connections 10 --all --duration-secs 60 --latency-unit ns --window-csv out.csv

Plotting

- Install deps (once):
    - `pip install matplotlib`
- Examples:
    - `python3 scripts/plot_latency.py out.csv --out latency` (generates `latency_raw.png` and `latency_ema.png`)
    - `python3 scripts/plot_latency.py out.csv --out latency.png --ema-alpha 0.3`

Notes

- The code uses server-time sync with midpoint RTT correction to compute one-way latency: recv_time(server_clock) − event_time.
- CSV contains per-second window stats for each connection: ts_epoch_ms, conn_id, count, drops, p50_<unit>, p90_<unit>, p99_<unit>, max_<unit> (unit = ms/us/ns).
- You can visualize whether a specific connection consistently leads by comparing p50/p99 across connections over time.
- If you want raw per-message latency logging for scatter plots, say the word — I can add an optional raw CSV emitter with backpressure control.
