use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use reqwest::Client;
use serde::Deserialize;
use tokio::{
    select,
    sync::mpsc,
    time::{interval, Instant, MissedTickBehavior},
};
use tokio_tungstenite::{connect_async_with_config, tungstenite};
use tracing::{error, info, warn};

#[derive(Parser, Debug, Clone)]
#[command(name = "binance_ws_latency_benchmark")]
struct Opts {
    /// Number of concurrent websocket connections
    #[arg(long, default_value_t = 10)]
    connections: usize,

    /// Subscription target: a single symbol (e.g. BTCUSDT) or use --all to subscribe to !bookTicker
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// Subscribe to all symbols via !bookTicker (high volume!)
    #[arg(long, default_value_t = false)]
    all: bool,

    /// Binance Futures WS endpoint base
    #[arg(long, default_value = "wss://fstream.binance.com")]
    ws_base: String,

    /// Run duration in seconds (0 = run until Ctrl-C)
    #[arg(long, default_value_t = 120u64)]
    duration_secs: u64,

    /// How often to print summary (seconds)
    #[arg(long, default_value_t = 5u64)]
    print_interval_secs: u64,

    /// Time sync refresh interval (seconds)
    #[arg(long, default_value_t = 60u64)]
    time_sync_interval_secs: u64,

    /// Per-second window stats CSV output file (optional)
    #[arg(long)]
    window_csv: Option<String>,

    /// Max message size in bytes
    #[arg(long, default_value_t = 64usize << 20)] // 64 MiB
    max_message_size: usize,

    /// Max frame size in bytes
    #[arg(long, default_value_t = 16usize << 20)] // 16 MiB
    max_frame_size: usize,

    /// Ping interval seconds (0 = disable app-level pings)
    #[arg(long, default_value_t = 15u64)]
    ping_interval_secs: u64,

    /// Display unit for latency in summaries: ms | us | ns
    #[arg(long, value_enum, default_value_t = LatencyUnit::Us)]
    latency_unit: LatencyUnit,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum LatencyUnit {
    Ms,
    Us,
    Ns,
}

impl LatencyUnit {
    fn scale_factor(self) -> u64 {
        match self {
            LatencyUnit::Ms => 1,
            LatencyUnit::Us => 1_000,
            LatencyUnit::Ns => 1_000_000,
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            LatencyUnit::Ms => "ms",
            LatencyUnit::Us => "µs",
            LatencyUnit::Ns => "ns",
        }
    }
}

#[derive(Debug)]
struct Sample {
    conn_id: usize,
    recv_instant: Instant,
    event_time_ms: i64,
    symbol: Option<String>,
}

#[derive(Deserialize, Debug)]
struct BookTickerTop {
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "T")]
    transact_time: Option<i64>,
    #[serde(rename = "s")]
    symbol: Option<String>,
}

#[derive(Deserialize, Debug)]
struct Envelope {
    stream: Option<String>,
    data: Option<BookTickerTop>,
}

fn lower_symbol(sym: &str) -> String {
    sym.to_ascii_lowercase()
}

fn stream_url(ws_base: &str, symbol: &str, all: bool) -> String {
    if all {
        format!("{}/ws/!bookTicker", ws_base.trim_end_matches('/'))
    } else {
        // single symbol bookTicker
        let s = lower_symbol(symbol);
        format!("{}/ws/{}@bookTicker", ws_base.trim_end_matches('/'), s)
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .compact()
        .init();

    let opts = Opts::parse();
    validate_opts(&opts)?;

    info!("Starting with options: {:?}", opts);

    // anchor mapping: Instant -> epoch ms
    let anchor_instant = Instant::now();
    let anchor_epoch_ms = epoch_ms_now();

    // shared server time offset (ms). server_epoch ~= local_epoch + offset
    let offset_ms = Arc::new(AtomicI64::new(0));

    // spawn time sync task
    let http = Client::builder()
        .user_agent("binance-ws-latency-bench/0.1")
        .timeout(Duration::from_secs(5))
        .build()?;
    let offset_clone = offset_ms.clone();
    let time_sync_handle = tokio::spawn(time_sync_task(
        http.clone(),
        opts.time_sync_interval_secs,
        offset_clone,
        anchor_instant,
        anchor_epoch_ms,
    ));

    let ws_url = stream_url(&opts.ws_base, &opts.symbol, opts.all);
    info!("Will connect to stream: {}", ws_url);

    // channel from WS readers to aggregator
    let (tx, mut rx) = mpsc::channel::<Sample>(200_000);

    // spawn N websocket readers
    for conn_id in 0..opts.connections {
        let tx_clone = tx.clone();
        let url = ws_url.clone();
        let offset = offset_ms.clone();
        let opts_ws = opts.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = ws_reader_task(conn_id, &url, opts_ws.clone(), offset.clone(), tx_clone.clone()).await {
                    warn!(conn_id, error = ?e, "ws_reader_task exited with error; will reconnect");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                } else {
                    // normal close: reconnect after short delay
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
    }
    drop(tx); // main no longer sends

    // Aggregator state
    let mut conns_total: Vec<ConnAgg> = (0..opts.connections)
        .map(|_| ConnAgg::new())
        .collect();

    // per-second windows we record for CSV
    let mut windows: Vec<WindowRow> = Vec::new();

    // timers
    let mut print_int = interval(Duration::from_secs(opts.print_interval_secs));
    print_int.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut window_int = interval(Duration::from_secs(1));
    window_int.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // run duration
    let deadline = if opts.duration_secs > 0 {
        Some(Instant::now() + Duration::from_secs(opts.duration_secs))
    } else {
        None
    };

    info!("Aggregator running...");
    loop {
        select! {
            biased;

            _ = tokio::signal::ctrl_c() => {
                warn!("Ctrl-C received, stopping...");
                break;
            }

            _ = async {
                if let Some(dl) = deadline {
                    tokio::time::sleep_until(dl).await;
                } else {
                    futures_util::future::pending::<()>().await;
                }
            }, if deadline.is_some() => {
                info!("Reached duration deadline, stopping...");
                break;
            }

            _ = print_int.tick() => {
                print_summary(&conns_total, opts.latency_unit);
            }

            _ = window_int.tick() => {
                let now_epoch_ms = instant_to_epoch_ms(Instant::now(), anchor_instant, anchor_epoch_ms) + offset_ms.load(Ordering::Relaxed);
                // finalize one-second window per connection
                for (id, agg) in conns_total.iter_mut().enumerate() {
                    let row = agg.finalize_window_row(id, now_epoch_ms);
                    windows.push(row);
                }
            }

            maybe = rx.recv() => {
                match maybe {
                    Some(sample) => {
                        let conn_id = sample.conn_id;
                        if let Some(agg) = conns_total.get_mut(conn_id) {
                            // Convert recv instant to estimated server epoch ms
                            let local_server_epoch_ms = instant_to_epoch_ms(sample.recv_instant, anchor_instant, anchor_epoch_ms)
                                + offset_ms.load(Ordering::Relaxed);
                            let evt_ms = sample.event_time_ms;
                            let mut latency_ms = local_server_epoch_ms - evt_ms;
                            if latency_ms < 0 {
                                // due to clock noise or asymmetry; clamp
                                latency_ms = 0;
                            }
                            agg.record(latency_ms as u64);
                        }
                    }
                    None => {
                        // all senders dropped; we still might want to drain timers and exit
                        warn!("All senders dropped; exiting aggregator loop");
                        break;
                    }
                }
            }
        }
    }

    // final one more window flush
    let now_epoch_ms = instant_to_epoch_ms(Instant::now(), anchor_instant, anchor_epoch_ms) + offset_ms.load(Ordering::Relaxed);
    for (id, agg) in conns_total.iter_mut().enumerate() {
        let row = agg.finalize_window_row(id, now_epoch_ms);
        windows.push(row);
    }

    // final summary
    println!("\n========== FINAL SUMMARY ==========");
    print_summary(&conns_total, opts.latency_unit);

    // write CSV if requested
    if let Some(path) = &opts.window_csv {
        if let Err(e) = write_csv(path, &windows, opts.latency_unit) {
            error!(error=?e, "Failed to write CSV");
        } else {
            info!("Wrote CSV window stats to {}", path);
        }
    }

    // stop time sync
    time_sync_handle.abort();

    Ok(())
}

fn validate_opts(opts: &Opts) -> Result<()> {
    if opts.connections == 0 {
        return Err(anyhow!("connections must be >= 1"));
    }
    if opts.max_frame_size == 0 || opts.max_message_size == 0 {
        return Err(anyhow!("max sizes must be > 0"));
    }
    Ok(())
}

// epoch ms at now()
fn epoch_ms_now() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch");
    (now.as_secs() as i64) * 1000 + (now.subsec_millis() as i64)
}

// map an Instant to epoch ms using anchor
fn instant_to_epoch_ms(now: Instant, anchor_instant: Instant, anchor_epoch_ms: i64) -> i64 {
    let delta = now.saturating_duration_since(anchor_instant);
    anchor_epoch_ms + delta.as_millis() as i64
}

async fn time_sync_task(
    http: Client,
    interval_secs: u64,
    offset_ms: Arc<AtomicI64>,
    anchor_instant: Instant,
    anchor_epoch_ms: i64,
) {
    let url = "https://fapi.binance.com/fapi/v1/time";
    let mut intv = interval(Duration::from_secs(interval_secs.max(5)));
    intv.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        intv.tick().await;
        let t0 = Instant::now();
        let local0_ms = instant_to_epoch_ms(t0, anchor_instant, anchor_epoch_ms);
        match http.get(url).send().await {
            Ok(resp) => match resp.json::<ServerTime>().await {
                Ok(res) => {
                    let t1 = Instant::now();
                    let local1_ms = instant_to_epoch_ms(t1, anchor_instant, anchor_epoch_ms);
                    let rtt_ms = (local1_ms - local0_ms).max(1);
                    let midpoint_local_ms = local0_ms + (rtt_ms / 2);
                    let new_offset = (res.serverTime as i64) - midpoint_local_ms;
                    offset_ms.store(new_offset, Ordering::Relaxed);
                    info!(
                        server_time = res.serverTime,
                        offset_ms = new_offset,
                        rtt_ms = rtt_ms,
                        "time sync updated"
                    );
                }
                Err(e) => {
                    warn!(error=?e, "time sync parse failed");
                }
            },
            Err(e) => {
                warn!(error=?e, "time sync request failed");
            },
        }
    }
}

#[derive(Deserialize)]
struct ServerTime {
    serverTime: u64,
}

async fn ws_reader_task(
    conn_id: usize,
    url: &str,
    opts: Opts,
    _offset_ms: Arc<AtomicI64>,
    tx: mpsc::Sender<Sample>,
) -> Result<()> {
    let request = url.to_string();
    let ws_config = tungstenite::protocol::WebSocketConfig {
        max_message_size: Some(opts.max_message_size),
        max_frame_size: Some(opts.max_frame_size),
        accept_unmasked_frames: false,
        ..Default::default()
    };

    info!(conn_id, %request, "connecting...");
    let (ws_stream, _resp) = connect_async_with_config(request, Some(ws_config), true)
        .await
        .context("ws connect failed")?;
    info!(conn_id, "connected");

    let (mut ws_sink, mut ws_stream) = ws_stream.split();

    // Optional app-level keepalive using select! with a timer.
    let mut ping_int = if opts.ping_interval_secs > 0 {
        let mut i = interval(Duration::from_secs(opts.ping_interval_secs));
        i.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Some(i)
    } else {
        None
    };

    loop {
        select! {
            // Reader branch
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(tungstenite::Message::Text(s))) => {
                        if let Some((evt_ms, symbol)) = parse_event_time_symbol(&s) {
                            let sample = Sample {
                                conn_id,
                                recv_instant: Instant::now(),
                                event_time_ms: evt_ms,
                                symbol,
                            };
                            if let Err(_e) = tx.try_send(sample) {
                                // drop on backpressure
                            }
                        }
                    }
                    Some(Ok(tungstenite::Message::Binary(_))) => { /* ignore */ }
                    Some(Ok(tungstenite::Message::Ping(p))) => {
                        let _ = ws_sink.send(tungstenite::Message::Pong(p)).await;
                    }
                    Some(Ok(tungstenite::Message::Pong(_))) => { /* ignore */ }
                    Some(Ok(tungstenite::Message::Close(frame))) => {
                        info!(conn_id, ?frame, "ws closed by server");
                        break;
                    }
                    Some(Ok(tungstenite::Message::Frame(_))) => { /* ignore */ }
                    Some(Err(e)) => {
                        warn!(conn_id, error=?e, "ws read error");
                        break;
                    }
                    None => {
                        warn!(conn_id, "ws stream ended");
                        break;
                    }
                }
            }

            // Periodic ping branch
            _ = async {
                if let Some(ref mut i) = ping_int { i.tick().await; }
                else { futures_util::future::pending::<()>().await; }
            }, if ping_int.is_some() => {
                if let Err(e) = ws_sink.send(tungstenite::Message::Ping(Vec::new())).await {
                    warn!(conn_id, error=?e, "ping send failed");
                    break;
                }
            }
        }
    }

    info!(conn_id, "reader exiting");
    Ok(())
}

fn parse_event_time_symbol(s: &str) -> Option<(i64, Option<String>)> {
    // Try parse as either envelope or bare bookTicker
    // Event time precedence: E (event time), fallback T (transact time)
    if let Ok(env) = serde_json::from_str::<Envelope>(s) {
        if let Some(data) = env.data {
            let evt = data.event_time.or(data.transact_time)?;
            return Some((evt, data.symbol));
        }
    }
    if let Ok(top) = serde_json::from_str::<BookTickerTop>(s) {
        let evt = top.event_time.or(top.transact_time)?;
        return Some((evt, top.symbol));
    }
    None
}

struct ConnAgg {
    // cumulative stats
    total_hist: Histogram<u64>,
    total_count: u64,
    total_drops: u64,
    // per-second window
    window_hist: Histogram<u64>,
    window_count: u64,
}

impl ConnAgg {
    fn new() -> Self {
        let total_hist =
            Histogram::new_with_bounds(1, 120_000, 3).expect("histogram bounds"); // 1ms .. 120s
        let window_hist =
            Histogram::new_with_bounds(1, 120_000, 3).expect("histogram bounds");
        Self {
            total_hist,
            total_count: 0,
            total_drops: 0,
            window_hist,
            window_count: 0,
        }
    }

    fn record(&mut self, latency_ms: u64) {
        let _ = self.total_hist.record(latency_ms.min(120_000));
        let _ = self.window_hist.record(latency_ms.min(120_000));
        self.total_count += 1;
        self.window_count += 1;
    }

    fn finalize_window_row(&mut self, conn_id: usize, ts_epoch_ms: i64) -> WindowRow {
        let row = WindowRow {
            ts_epoch_ms,
            conn_id,
            count: self.window_count,
            drops: 0, // not tracked per-window currently
            p50_ms: percentile(&self.window_hist, 50.0),
            p90_ms: percentile(&self.window_hist, 90.0),
            p99_ms: percentile(&self.window_hist, 99.0),
            max_ms: self.window_hist.max(),
        };
        self.window_hist.reset();
        self.window_count = 0;
        row
    }
}

fn percentile(h: &Histogram<u64>, p: f64) -> f64 {
    if h.len() == 0 {
        0.0
    } else {
        h.value_at_quantile((p / 100.0).clamp(0.0, 1.0)) as f64
    }
}

fn print_summary(conns: &[ConnAgg], unit: LatencyUnit) {
    println!("----- Summary ({} conns) -----", conns.len());
    for (i, agg) in conns.iter().enumerate() {
        let p50_ms = percentile(&agg.total_hist, 50.0);
        let p90_ms = percentile(&agg.total_hist, 90.0);
        let p99_ms = percentile(&agg.total_hist, 99.0);
        let max_ms = agg.total_hist.max();

        let p50 = format_latency_scaled_int(p50_ms, unit.clone());
        let p90 = format_latency_scaled_int(p90_ms, unit.clone());
        let p99 = format_latency_scaled_int(p99_ms, unit.clone());
        let max = format_int_with_commas((max_ms as u128) * (unit.scale_factor() as u128));

        let count_fmt = format_int_with_commas(agg.total_count as u128);
        println!(
            "conn {:02}: count={} p50={} {} p90={} {} p99={} {} max={} {}",
            i,
            count_fmt,
            p50,
            unit.suffix(),
            p90,
            unit.suffix(),
            p99,
            unit.suffix(),
            max,
            unit.suffix()
        );
    }
}

fn format_latency_scaled_int(value_ms: f64, unit: LatencyUnit) -> String {
    let scaled: u128 = ((value_ms * unit.scale_factor() as f64).round() as i128).max(0) as u128;
    format_int_with_commas(scaled)
}

fn format_int_with_commas(mut n: u128) -> String {
    // Handles only non-negative integers; suitable for counts/latencies here
    let mut parts: Vec<String> = Vec::new();
    if n == 0 {
        return "0".to_string();
    }
    while n > 0 {
        let chunk = (n % 1000) as u16;
        n /= 1000;
        if n > 0 {
            parts.push(format!("{:03}", chunk));
        } else {
            parts.push(format!("{}", chunk));
        }
    }
    parts.reverse();
    parts.join(",")
}

struct WindowRow {
    ts_epoch_ms: i64,
    conn_id: usize,
    count: u64,
    drops: u64,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    max_ms: u64,
}

fn write_csv(path: &str, rows: &[WindowRow], unit: LatencyUnit) -> Result<()> {
    use std::fs::File;
    use std::io::{BufWriter, Write};
    let f = File::create(path)?;
    let mut w = BufWriter::new(f);
    let suf = match unit { LatencyUnit::Ms => "ms", LatencyUnit::Us => "us", LatencyUnit::Ns => "ns" };
    writeln!(
        w,
        "ts_epoch_ms,conn_id,count,drops,p50_{s},p90_{s},p99_{s},max_{s}",
        s = suf
    )?;
    for r in rows {
        let scale = unit.scale_factor() as f64;
        let p50 = (r.p50_ms * scale).round();
        let p90 = (r.p90_ms * scale).round();
        let p99 = (r.p99_ms * scale).round();
        let max = (r.max_ms as f64 * scale).round() as u128;
        writeln!(
            w,
            "{},{},{},{},{},{},{},{}",
            r.ts_epoch_ms,
            r.conn_id,
            r.count,
            r.drops,
            p50 as u128,
            p90 as u128,
            p99 as u128,
            max
        )?;
    }
    w.flush()?;
    Ok(())
}
