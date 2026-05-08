//! Batch assembly module — accumulates individual `TelemetryFrame` values
//! into `TelemetryBatch` messages at a configurable interval.
//!
//! Only available with the `cloud-transport` feature.

use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::debug;

use crate::proto::purplesector::{TelemetryBatch, TelemetryFrame};

/// Configuration for the batch assembler.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchConfig {
    /// How often to flush accumulated samples into a batch.
    /// Default: 500ms. Configurable by the user.
    #[serde(with = "humantime_serde", default = "default_interval")]
    pub flush_interval: Duration,

    /// User ID to stamp on every batch.
    pub user_id: String,

    /// Source capture rate in Hz (informational, stamped on each batch).
    pub source_rate_hz: u32,

    /// Data source identifier: "demo", "ac-rig-1", "acc-rig-2", etc.
    /// Sessions are assigned in RisingWave based on this source.
    #[serde(default = "default_source")]
    pub source: String,
}

fn default_source() -> String {
    "live".to_string()
}

fn default_interval() -> Duration {
    Duration::from_millis(500)
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            flush_interval: default_interval(),
            user_id: String::new(),
            source_rate_hz: 60,
            source: default_source(),
        }
    }
}

/// Assembles individual samples into batches at a configurable interval.
///
/// Receives `TelemetryFrame` via a channel, accumulates them, and emits
/// `TelemetryBatch` messages on the output channel when either the flush
/// interval elapses or the assembler is explicitly flushed.
pub struct BatchAssembler {
    config: BatchConfig,
    buffer: Vec<TelemetryFrame>,
    last_flush: Instant,
}

impl BatchAssembler {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            buffer: Vec::with_capacity(512),
            last_flush: Instant::now(),
        }
    }

    /// Add a sample to the current batch buffer.
    /// Returns `Some(batch)` if the flush interval has elapsed.
    pub fn push(&mut self, sample: TelemetryFrame) -> Option<TelemetryBatch> {
        self.buffer.push(sample);

        if self.last_flush.elapsed() >= self.config.flush_interval {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Force-flush the current buffer into a batch, even if the interval
    /// hasn't elapsed. Returns an empty batch if no samples are buffered.
    pub fn flush(&mut self) -> TelemetryBatch {
        let samples = std::mem::take(&mut self.buffer);
        self.buffer = Vec::with_capacity(512);
        self.last_flush = Instant::now();

        let batch_size = samples.len() as u32;
        let batch_start_ts = samples.first().map(|s| s.timestamp).unwrap_or(0);
        let batch_end_ts = samples.last().map(|s| s.timestamp).unwrap_or(0);

        let source_value = self.config.source.clone();
        debug!(
            "Batch flushed: {} samples, ts range {}..{}, source='{}'",
            batch_size, batch_start_ts, batch_end_ts, source_value
        );

        TelemetryBatch {
            user_id: self.config.user_id.clone(),
            source: source_value,
            batch_start_ts,
            batch_end_ts,
            batch_size,
            source_rate_hz: self.config.source_rate_hz,
            samples,
            compressed_samples: None,
        }
    }

    /// Returns the number of samples currently buffered.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }
}

/// Spawn a background task that receives samples and emits batches.
///
/// This is a convenience wrapper for use in the tray app main loop.
pub fn spawn_batch_assembler(
    config: BatchConfig,
    mut sample_rx: mpsc::Receiver<TelemetryFrame>,
    batch_tx: mpsc::Sender<TelemetryBatch>,
) -> tokio::task::JoinHandle<()> {
    let flush_interval = config.flush_interval;

    tokio::spawn(async move {
        let mut assembler = BatchAssembler::new(config);

        loop {
            tokio::select! {
                result = sample_rx.recv() => {
                    match result {
                        Some(sample) => {
                            if let Some(batch) = assembler.push(sample) {
                                if batch_tx.send(batch).await.is_err() {
                                    break; // Receiver dropped
                                }
                            }
                        }
                        None => break, // sample_tx dropped — shutdown chain
                    }
                }
                _ = tokio::time::sleep(flush_interval) => {
                    if assembler.buffered_count() > 0 {
                        let batch = assembler.flush();
                        if batch_tx.send(batch).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }

        // Final flush
        if assembler.buffered_count() > 0 {
            let batch = assembler.flush();
            let _ = batch_tx.send(batch).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(ts: i64) -> TelemetryFrame {
        TelemetryFrame {
            timestamp: ts,
            speed: 100.0,
            throttle: 0.5,
            brake: 0.0,
            steering: 0.1,
            gear: 3,
            rpm: 5000,
            normalized_position: 0.5,
            lap_number: 1,
            lap_time: 30000,
            session_time: None,
            session_type: None,
            track_position: None,
            delta: None,
        }
    }

    #[test]
    fn test_manual_flush() {
        let config = BatchConfig {
            flush_interval: Duration::from_secs(60), // Long interval so push won't auto-flush
            user_id: "test-user".into(),
            source_rate_hz: 60,
            source: "test".into(),
        };
        let mut assembler = BatchAssembler::new(config);

        assert!(assembler.push(sample(1000)).is_none());
        assert!(assembler.push(sample(1016)).is_none());
        assert_eq!(assembler.buffered_count(), 2);

        let batch = assembler.flush();
        assert_eq!(batch.batch_size, 2);
        assert_eq!(batch.batch_start_ts, 1000);
        assert_eq!(batch.batch_end_ts, 1016);
        assert_eq!(batch.user_id, "test-user");
        assert_eq!(batch.source_rate_hz, 60);
        assert_eq!(assembler.buffered_count(), 0);
    }
}
