//! gRPC client for streaming telemetry batches to the cloud gateway.
//!
//! Uses tonic for the gRPC transport. Reads batches from the WAL,
//! streams them to the gateway, and ACKs them on successful receipt.
//!
//! Only available with the `cloud-transport` feature.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::proto::purplesector::telemetry_ingress_client::TelemetryIngressClient;
use crate::proto::purplesector::TelemetryBatch;
use crate::wal::TelemetryWal;

/// Configuration for the gRPC transport.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GrpcConfig {
    /// Gateway endpoint URL (e.g., "https://gateway.purplesector.io:443").
    pub gateway_url: String,

    /// Path to the WAL database file.
    pub wal_path: String,

    /// How many batches to drain from the WAL per send cycle.
    pub drain_batch_size: usize,

    /// Retry delay when the gateway is unreachable.
    #[serde(with = "humantime_serde", default = "default_retry_delay")]
    pub retry_delay: Duration,

    /// Optional bearer token for authentication (set by OIDC flow).
    pub auth_token: Option<String>,

    /// If set, `run_transport` writes the current WAL depth into this atomic
    /// each poll cycle so external code (e.g. the tray-app stats view) can
    /// read it without modifying the WAL internals.
    #[serde(skip)]
    pub wal_depth_reporter: Option<Arc<AtomicU64>>,
}

fn default_retry_delay() -> Duration {
    Duration::from_secs(5)
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            gateway_url: "http://localhost:50051".into(),
            wal_path: "telemetry-wal.db".into(),
            drain_batch_size: 32,
            retry_delay: default_retry_delay(),
            auth_token: None,
            wal_depth_reporter: None,
        }
    }
}

/// Commands sent to the WAL worker thread.
enum WalCmd {
    Push(TelemetryBatch),
    Peek(usize, tokio::sync::oneshot::Sender<Result<Vec<(i64, TelemetryBatch)>>>),
    AckUpTo(i64),
    Depth(tokio::sync::oneshot::Sender<Result<usize>>),
    Shutdown,
}

/// Spawn a dedicated blocking thread for WAL operations (rusqlite is !Send).
fn spawn_wal_worker(
    wal_path: String,
) -> Result<mpsc::Sender<WalCmd>> {
    let (tx, mut rx) = mpsc::channel::<WalCmd>(256);

    std::thread::Builder::new()
        .name("wal-worker".into())
        .spawn(move || {
            let wal = match TelemetryWal::open(Path::new(&wal_path)) {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to open WAL: {e}");
                    return;
                }
            };

            // Block on the channel using a simple loop with try_recv + sleep
            // since we're on a dedicated OS thread, not the tokio runtime.
            loop {
                match rx.blocking_recv() {
                    Some(WalCmd::Push(batch)) => {
                        if let Err(e) = wal.push(&batch) {
                            tracing::error!("WAL push failed: {e}");
                        }
                    }
                    Some(WalCmd::Peek(count, reply)) => {
                        let _ = reply.send(wal.peek(count));
                    }
                    Some(WalCmd::AckUpTo(id)) => {
                        if let Err(e) = wal.ack_up_to(id) {
                            tracing::error!("WAL ack failed: {e}");
                        }
                    }
                    Some(WalCmd::Depth(reply)) => {
                        let _ = reply.send(wal.depth());
                    }
                    Some(WalCmd::Shutdown) | None => {
                        debug!("WAL worker shutting down");
                        break;
                    }
                }
            }
        })
        .context("Failed to spawn WAL worker thread")?;

    Ok(tx)
}

/// The gRPC transport sends batches to the cloud gateway.
///
/// Architecture:
/// - Happy path: batches are sent directly via a persistent gRPC streaming
///   connection as they arrive, keeping latency near-zero.
/// - On failure: batches are buffered in the WAL (SQLite). On reconnect the
///   WAL is drained before live streaming resumes, preserving order.
/// - The WAL is also used on startup to replay any batches from a previous
///   session that did not get ACKed.
pub async fn run_transport(
    config: GrpcConfig,
    mut batch_rx: mpsc::Receiver<TelemetryBatch>,
) -> Result<()> {
    let wal_tx = spawn_wal_worker(config.wal_path.clone())?;

    info!(
        "gRPC transport started, gateway={}, wal={}",
        config.gateway_url, config.wal_path
    );

    // Log initial WAL depth
    let (depth_tx, depth_rx) = tokio::sync::oneshot::channel();
    wal_tx.send(WalCmd::Depth(depth_tx)).await.ok();
    if let Ok(Ok(depth)) = depth_rx.await {
        if depth > 0 {
            info!("WAL has {depth} pending batches from previous session");
        }
    }

    'outer: loop {
        // ── Connect to gateway ────────────────────────────────────────
        let channel = loop {
            match Channel::from_shared(config.gateway_url.clone())
                .context("Invalid gateway URL")?
                .connect()
                .await
            {
                Ok(ch) => break ch,
                Err(e) => {
                    warn!("Gateway connect failed: {e}, retrying in {:?}", config.retry_delay);
                    // Drain incoming batches into WAL while offline so we
                    // don't block the capture pipeline.
                    loop {
                        match batch_rx.try_recv() {
                            Ok(batch) => { wal_tx.send(WalCmd::Push(batch)).await.ok(); }
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                wal_tx.send(WalCmd::Shutdown).await.ok();
                                return Ok(());
                            }
                        }
                    }
                    tokio::time::sleep(config.retry_delay).await;
                }
            }
        };

        let mut client = TelemetryIngressClient::new(channel);
        info!("Connected to gateway at {}", config.gateway_url);

        // ── Drain any WAL backlog before going live ───────────────────
        loop {
            let (peek_tx, peek_rx) = tokio::sync::oneshot::channel();
            wal_tx.send(WalCmd::Peek(config.drain_batch_size, peek_tx)).await.ok();
            let backlog = peek_rx.await.unwrap_or(Ok(vec![])).unwrap_or_default();
            if backlog.is_empty() { break; }

            let max_id = backlog.last().map(|(id, _)| *id).unwrap_or(0);
            let count = backlog.len();
            let stream = tokio_stream::iter(backlog.into_iter().map(|(_, b)| b));
            match client.stream_telemetry(stream).await {
                Ok(resp) => {
                    let ack = resp.into_inner();
                    info!("WAL drain ACK: {} batches, {} samples", ack.batches_received, ack.samples_received);
                    wal_tx.send(WalCmd::AckUpTo(max_id)).await.ok();
                    if let Some(r) = &config.wal_depth_reporter {
                        let (d_tx, d_rx) = tokio::sync::oneshot::channel();
                        wal_tx.send(WalCmd::Depth(d_tx)).await.ok();
                        let depth = d_rx.await.unwrap_or(Ok(0)).unwrap_or(0);
                        r.store(depth as u64, Ordering::Relaxed);
                    }
                    if count < config.drain_batch_size { break; }
                }
                Err(e) => {
                    warn!("WAL drain failed: {e}, reconnecting...");
                    continue 'outer;
                }
            }
        }

        // ── Live streaming loop ───────────────────────────────────────
        // One persistent HTTP/2 client-streaming RPC for the lifetime of
        // the connection. Batches are forwarded into the stream as they
        // arrive — no per-batch connection overhead.
        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<TelemetryBatch>(256);
        let batch_stream = tokio_stream::wrappers::ReceiverStream::new(stream_rx);

        // Spawn the RPC call — it reads from batch_stream until stream_tx is dropped.
        let rpc_handle = tokio::spawn({
            let mut c = client.clone();
            async move {
                let result = c.stream_telemetry(batch_stream).await;
                info!("Persistent stream RPC ended: {:?}", result.as_ref().map(|_| "ok").unwrap_or("err"));
                result
            }
        });

        info!("Entering live streaming loop");
        let mut batch_count: u64 = 0;
        loop {
            let batch = match batch_rx.recv().await {
                Some(b) => b,
                None => {
                    info!("Batch channel closed, shutting down transport...");
                    drop(stream_tx);
                    let _ = rpc_handle.await;
                    wal_tx.send(WalCmd::Shutdown).await.ok();
                    return Ok(());
                }
            };

            batch_count += 1;
            if batch_count <= 3 || batch_count % 50 == 0 {
                info!("Forwarding batch #{batch_count} to gateway stream");
            }

            // Forward batch into the persistent stream
            if stream_tx.send(batch).await.is_err() {
                // stream_rx dropped — RPC ended (gateway closed or error)
                warn!("Gateway stream closed after {batch_count} batches, reconnecting...");
                continue 'outer;
            }

            if let Some(r) = &config.wal_depth_reporter {
                r.store(0, Ordering::Relaxed);
            }
        }
    }
}
