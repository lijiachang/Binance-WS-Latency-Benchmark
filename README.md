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

```
python3 scripts/plot_latency.py out.csv --out latency.png
```
The plotting script autodetects `ms/us/ns` in the CSV header.

This produces a figure with p50 and p99 latency over time per connection, letting you visually inspect whether a specific connection is systematically faster or just transiently leading.


How to run

- Build:
    - cd binance_ws_latency_benchmark
    - cargo build --release
- Example run (10 conns, BTCUSDT, 2 mins, print every 5s, export windows):
    - cargo run --release -- --connections 10 --symbol BTCUSDT --duration-secs 120 --print-interval-secs 5 --latency-unit us --window-csv out.csv
- All symbols (very high volume — verify capacity first):
    - cargo run --release -- --connections 10 --all --duration-secs 60 --latency-unit ns --window-csv out.csv

Plotting

- Requires Python 3 and matplotlib:
    - pip install matplotlib
    - python3 binance_ws_latency_benchmark/scripts/plot_latency.py binance_ws_latency_benchmark/out.csv --out binance_ws_latency_benchmark/latency.png

Notes

- The code uses server-time sync with midpoint RTT correction to compute one-way latency: recv_time(server_clock) − event_time.
- CSV contains per-second window stats for each connection: ts_epoch_ms, conn_id, count, drops, p50_<unit>, p90_<unit>, p99_<unit>, max_<unit> (unit = ms/us/ns).
- You can visualize whether a specific connection consistently leads by comparing p50/p99 across connections over time.
- If you want raw per-message latency logging for scatter plots, say the word — I can add an optional raw CSV emitter with backpressure control.
