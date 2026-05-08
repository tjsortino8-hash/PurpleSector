//! Telemetry capture pipeline — manages the capture → batch → gRPC lifecycle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

use prost::Message as _;
use ps_telemetry_core::batch::{self, BatchConfig};
use ps_telemetry_core::capture::ac::AcSource;
use ps_telemetry_core::capture::acc::AccSource;
use ps_telemetry_core::capture::demo::{DemoConfig, DemoSource};
use ps_telemetry_core::capture::{CaptureConfig, TelemetrySource};
use ps_telemetry_core::grpc::{self, GrpcConfig};

use eframe::egui;
use crate::config::{AppConfig, SimType};
use crate::stats::PipelineStats;

/// Commands sent from the tray menu / GUI to the pipeline manager.
#[derive(Debug)]
pub enum PipelineCommand {
    Start,
    Stop,
    ReloadConfig,
    Quit,
}

/// Runs the pipeline manager loop on the tokio runtime.
/// Listens for commands and starts/stops the capture pipeline accordingly.
pub async fn run_pipeline_manager(
    config: Arc<Mutex<AppConfig>>,
    stats: PipelineStats,
    running: Arc<AtomicBool>,
    mut cmd_rx: mpsc::Receiver<PipelineCommand>,
    egui_ctx: Arc<std::sync::OnceLock<egui::Context>>,
) {
    // Handle to the currently running pipeline task
    let mut pipeline_handle: Option<tokio::task::JoinHandle<()>> = None;
    // Shutdown signal for the running pipeline
    let mut shutdown_tx: Option<mpsc::Sender<()>> = None;

    loop {
        match cmd_rx.recv().await {
            Some(PipelineCommand::Start) => {
                if running.load(Ordering::Relaxed) {
                    warn!("Pipeline already running, ignoring Start command");
                    continue;
                }
                info!("Starting capture pipeline...");

                let (stop_tx, stop_rx) = mpsc::channel(1);
                shutdown_tx = Some(stop_tx);

                let cfg = config.lock().unwrap().clone();
                let stats_clone = stats.clone();
                let running_clone = running.clone();
                let egui_ctx_clone = egui_ctx.clone();

                pipeline_handle = Some(tokio::spawn(async move {
                    running_clone.store(true, Ordering::Relaxed);
                    if let Err(e) = run_pipeline(cfg, stats_clone, stop_rx).await {
                        error!("Pipeline error: {e:#}");
                    }
                    running_clone.store(false, Ordering::Relaxed);
                    if let Some(ctx) = egui_ctx_clone.get() {
                        ctx.request_repaint();
                    }
                    info!("Pipeline stopped");
                }));
            }
            Some(PipelineCommand::Stop) => {
                info!("Stopping capture pipeline...");
                if let Some(tx) = shutdown_tx.take() {
                    let _ = tx.send(()).await;
                }
                // Await the handle so running_clone.store(false) executes
                // before we return, ensuring the GUI sees the stopped state.
                // The shutdown signal above unblocks capture_loop immediately
                // so this await completes quickly.
                if let Some(handle) = pipeline_handle.take() {
                    let _ = handle.await;
                }
                running.store(false, Ordering::Relaxed);
                if let Some(ctx) = egui_ctx.get() {
                    ctx.request_repaint();
                }
            }
            Some(PipelineCommand::ReloadConfig) => {
                info!("Config reloaded — restart pipeline to apply changes");
                // If running, stop the old pipeline and restart with new config
                if running.load(Ordering::Relaxed) {
                    if let Some(tx) = shutdown_tx.take() {
                        let _ = tx.send(()).await;
                    }
                    // Wait for old task to finish before spawning a new one
                    if let Some(handle) = pipeline_handle.take() {
                        let _ = handle.await;
                    }
                    running.store(false, Ordering::Relaxed);

                    // Restart with updated config
                    let (stop_tx, stop_rx) = mpsc::channel(1);
                    shutdown_tx = Some(stop_tx);

                    let cfg = config.lock().unwrap().clone();
                    let stats_clone = stats.clone();
                    let running_clone = running.clone();

                    pipeline_handle = Some(tokio::spawn(async move {
                        running_clone.store(true, Ordering::Relaxed);
                        if let Err(e) = run_pipeline(cfg, stats_clone, stop_rx).await {
                            error!("Pipeline error: {e:#}");
                        }
                        running_clone.store(false, Ordering::Relaxed);
                    }));
                }
            }
            Some(PipelineCommand::Quit) | None => {
                info!("Pipeline manager shutting down");
                if let Some(tx) = shutdown_tx.take() {
                    let _ = tx.send(()).await;
                }
                if let Some(handle) = pipeline_handle.take() {
                    let _ = handle.await;
                }
                break;
            }
        }
    }
}

/// Run the actual capture → batch → gRPC pipeline until shutdown signal.
async fn run_pipeline(
    config: AppConfig,
    stats: PipelineStats,
    mut shutdown_rx: mpsc::Receiver<()>,
) -> anyhow::Result<()> {
    let batch_config = BatchConfig {
        flush_interval: Duration::from_millis(config.batch_interval_ms),
        user_id: if config.user_id.is_empty() {
            "anonymous".into()
        } else {
            config.user_id.clone()
        },
        source: format!("session-{}", chrono_session_id()),
        source_rate_hz: match config.sim_type {
            SimType::AssettoCorsaSS => 60,
            SimType::AssettoCorsa => 10,
            SimType::Demo => 60,
        },
    };

    let grpc_config = GrpcConfig {
        gateway_url: config.grpc_endpoint.clone(),
        wal_depth_reporter: Some(stats.wal_depth_arc()),
        ..Default::default()
    };

    // Channels
    let (sample_tx, sample_rx) = mpsc::channel(4096);
    let (batch_tx, batch_rx) = mpsc::channel(256);
    let (transport_tx, transport_rx) = mpsc::channel(256);

    // Spawn batch assembler
    let _batch_handle = batch::spawn_batch_assembler(batch_config, sample_rx, batch_tx);

    // Stats interceptor: count every batch that leaves the assembler before
    // it enters the WAL / gRPC transport. Tracks batches_sent and bytes_sent.
    let stats_intercept = stats.clone();
    tokio::spawn(async move {
        let mut rx = batch_rx;
        while let Some(batch) = rx.recv().await {
            let bytes = batch.encoded_len() as u64;
            stats_intercept.inc_batches_sent();
            stats_intercept.add_bytes_sent(bytes);
            if transport_tx.send(batch).await.is_err() {
                break;
            }
        }
    });

    // Spawn gRPC transport
    let stats_grpc = stats.clone();
    let grpc_handle = tokio::spawn(async move {
        if let Err(e) = grpc::run_transport(grpc_config, transport_rx).await {
            error!("gRPC transport error: {e}");
            stats_grpc.inc_batches_failed();
        }
    });

    // Start capture source
    let capture_result: anyhow::Result<()> = match config.sim_type {
        SimType::AssettoCorsaSS => {
            let capture_config = CaptureConfig::default();
            let mut source = AcSource::new(capture_config);
            source.start().await?;
            info!("Capture started: {}", source.name());
            capture_loop(&mut source, &sample_tx, &stats, &mut shutdown_rx).await;
            source.stop().await?;
            Ok(())
        }
        SimType::AssettoCorsa => {
            let mut acc_config = ps_telemetry_core::capture::acc::AccCaptureConfig::default();
            acc_config.connection_password = config.acc_connection_password.clone();
            acc_config.update_interval_ms = config.acc_update_interval_ms;
            let mut source = AccSource::new(acc_config);
            source.start().await?;
            info!("Capture started: {}", source.name());
            capture_loop(&mut source, &sample_tx, &stats, &mut shutdown_rx).await;
            source.stop().await?;
            Ok(())
        }
        SimType::Demo => {
            let demo_config = DemoConfig {
                loop_playback: true,
                ..Default::default()
            };
            let mut source = DemoSource::new(demo_config);
            source.start().await?;
            info!("Capture started: {}", source.name());
            capture_loop(&mut source, &sample_tx, &stats, &mut shutdown_rx).await;
            source.stop().await?;
            Ok(())
        }
    };

    drop(sample_tx);
    let _ = grpc_handle.await;
    capture_result
}

/// Main capture loop — reads samples until shutdown signal.
async fn capture_loop(
    source: &mut dyn TelemetrySource,
    sample_tx: &mpsc::Sender<ps_telemetry_core::proto::purplesector::TelemetryFrame>,
    stats: &PipelineStats,
    shutdown_rx: &mut mpsc::Receiver<()>,
) {
    let mut sample_count: u64 = 0;
    let mut last_log = std::time::Instant::now();
    loop {
        tokio::select! {
            result = source.next_sample() => {
                match result {
                    Ok(sample) => {
                        stats.inc_samples();
                        sample_count += 1;
                        if last_log.elapsed() >= std::time::Duration::from_secs(5) {
                            info!("Capture: {} samples received so far", sample_count);
                            last_log = std::time::Instant::now();
                        }
                        if sample_tx.send(sample).await.is_err() {
                            info!("Batch channel closed, stopping capture");
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Capture error: {e}");
                        break;
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received, {} total samples captured", sample_count);
                break;
            }
        }
    }
}

/// Generate a session ID based on current time.
fn chrono_session_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{ts:x}")
}
