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

/// The gRPC transport sends batches from the WAL to the cloud gateway.
///
/// Architecture:
/// 1. New batches arrive via `batch_rx` channel and are written to the WAL
///    (on a dedicated blocking thread since rusqlite is !Send).
/// 2. A drain loop peeks batches from the WAL, streams them to the gateway.
/// 3. On ACK, batches are removed from the WAL.
/// 4. On error, the drain loop retries after `retry_delay`.
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

    loop {
        // Phase 1: Receive incoming batches and write to WAL
        loop {
            match batch_rx.try_recv() {
                Ok(batch) => {
                    wal_tx.send(WalCmd::Push(batch)).await.ok();
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    info!("Batch channel closed, draining WAL...");
                    let drain = tokio::time::timeout(
                        Duration::from_secs(2),
                        drain_wal(&config, &wal_tx),
                    );
                    match drain.await {
                        Ok(Ok(n)) => info!("Drained {n} batches on shutdown"),
                        Ok(Err(e)) => warn!("Drain on shutdown failed: {e}"),
                        Err(_) => warn!("Drain on shutdown timed out"),
                    }
                    wal_tx.send(WalCmd::Shutdown).await.ok();
                    return Ok(());
                }
            }
        }

        // Phase 2: Try to drain WAL to gateway
        let (depth_tx, depth_rx) = tokio::sync::oneshot::channel();
        wal_tx.send(WalCmd::Depth(depth_tx)).await.ok();
        let pending = depth_rx.await.unwrap_or(Ok(0)).unwrap_or(0);

        if let Some(reporter) = &config.wal_depth_reporter {
            reporter.store(pending as u64, Ordering::Relaxed);
        }

        if pending > 0 {
            match drain_wal(&config, &wal_tx).await {
                Ok(sent) => {
                    debug!("Drained {sent} batches to gateway");
                }
                Err(e) => {
                    warn!("Gateway send failed: {e}, retrying in {:?}", config.retry_delay);
                    tokio::time::sleep(config.retry_delay).await;
                    continue;
                }
            }
        }

        // Wait for more batches or a short poll interval
        tokio::select! {
            result = batch_rx.recv() => {
                match result {
                    Some(batch) => {
                        wal_tx.send(WalCmd::Push(batch)).await.ok();
                    }
                    None => {
                        // Channel closed — flush WAL with a timeout and exit
                        info!("Batch channel closed, draining WAL...");
                        let drain = tokio::time::timeout(
                            Duration::from_secs(2),
                            drain_wal(&config, &wal_tx),
                        );
                        match drain.await {
                            Ok(Ok(n)) => info!("Drained {n} batches on shutdown"),
                            Ok(Err(e)) => warn!("Drain on shutdown failed: {e}"),
                            Err(_) => warn!("Drain on shutdown timed out"),
                        }
                        wal_tx.send(WalCmd::Shutdown).await.ok();
                        return Ok(());
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
}

/// Attempt to drain pending WAL batches to the gateway.
async fn drain_wal(config: &GrpcConfig, wal_tx: &mpsc::Sender<WalCmd>) -> Result<usize> {
    let (peek_tx, peek_rx) = tokio::sync::oneshot::channel();
    wal_tx
        .send(WalCmd::Peek(config.drain_batch_size, peek_tx))
        .await
        .ok();

    let batches = peek_rx
        .await
        .context("WAL worker dropped")?
        .context("WAL peek failed")?;

    if batches.is_empty() {
        return Ok(0);
    }

    let max_id = batches.last().map(|(id, _)| *id).unwrap_or(0);
    let count = batches.len();

    // Connect to gateway
    let channel = Channel::from_shared(config.gateway_url.clone())
        .context("Invalid gateway URL")?
        .connect()
        .await
        .context("Failed to connect to gateway")?;

    let mut client = TelemetryIngressClient::new(channel);

    // Stream batches
    let batch_stream = tokio_stream::iter(batches.into_iter().map(|(_, batch)| batch));
    let response = client
        .stream_telemetry(batch_stream)
        .await
        .context("StreamTelemetry RPC failed")?;

    let ack = response.into_inner();
    info!(
        "Gateway ACK: {} batches, {} samples",
        ack.batches_received, ack.samples_received
    );

    // Remove ACKed batches from WAL
    wal_tx.send(WalCmd::AckUpTo(max_id)).await.ok();

    Ok(count)
}
