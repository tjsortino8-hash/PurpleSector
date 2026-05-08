//! Telemetry capture module — UDP and SharedMemory readers for sim data.
//!
//! This module is shared by both the System Tray app and the Tauri Desktop app.

pub mod ac;
pub mod acc;
pub mod demo;

use crate::proto::purplesector::TelemetryFrame;

/// Errors that can occur during telemetry capture.
#[derive(Debug, thiserror::Error)]
pub enum CaptureError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Packet too small: got {got} bytes, need at least {min}")]
    PacketTooSmall { got: usize, min: usize },

    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("Shared memory not available: {0}")]
    SharedMemoryUnavailable(String),

    #[error("Capture source stopped")]
    Stopped,
}

/// Configuration for a telemetry capture source.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CaptureConfig {
    /// Local UDP port to bind and receive on.
    pub udp_port: u16,
    /// UDP bind address.
    pub udp_host: String,
    /// Target host for games that require a handshake (e.g., AC).
    pub target_host: Option<String>,
    /// Target port on the game machine. Defaults to the game's standard port
    /// (9996 for AC, 9000 for ACC). Kept separate from udp_port so the local
    /// receive socket does not conflict with the game's own listener.
    pub target_port: Option<u16>,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            udp_port: 9997,
            udp_host: "0.0.0.0".into(),
            target_host: None,
            target_port: Some(9996),
        }
    }
}

/// Trait for telemetry capture sources.
///
/// Both AC and ACC collectors implement this trait. The Tauri desktop app
/// uses `TelemetrySource` to capture frames and writes them directly to
/// the local database. The tray app uses it to feed into the batch/gRPC
/// transport pipeline.
#[async_trait::async_trait]
pub trait TelemetrySource: Send + Sync {
    /// Start the capture source. Returns when the source is ready to produce samples.
    async fn start(&mut self) -> Result<(), CaptureError>;

    /// Receive the next telemetry sample. Blocks until a sample is available.
    async fn next_sample(&mut self) -> Result<TelemetryFrame, CaptureError>;

    /// Stop the capture source and release resources.
    async fn stop(&mut self) -> Result<(), CaptureError>;

    /// Human-readable name for this source (e.g., "Assetto Corsa", "ACC").
    fn name(&self) -> &str;

    /// The native capture rate of this source in Hz.
    fn source_rate_hz(&self) -> u32;
}

/// Parse a raw Assetto Corsa UDP telemetry packet into a `TelemetryFrame`.
///
/// Packet layout (from existing JS collector):
/// - Offset 8:   speed (f32 LE)
/// - Offset 40:  lap_time (i32 LE, ms)
/// - Offset 52:  lap_number (i32 LE)
/// - Offset 56:  throttle (f32 LE, 0-1)
/// - Offset 60:  brake (f32 LE, 0-1)
/// - Offset 68:  rpm (i32 LE)
/// - Offset 72:  steering (f32 LE, -1 to 1)
/// - Offset 76:  gear (i32 LE)
/// - Offset 308: normalized_position (f32 LE, 0-1)
pub fn parse_ac_packet(buf: &[u8], timestamp: i64) -> Result<TelemetryFrame, CaptureError> {
    const MIN_SIZE: usize = 312; // Need at least through offset 308 + 4 bytes
    if buf.len() < MIN_SIZE {
        return Err(CaptureError::PacketTooSmall {
            got: buf.len(),
            min: MIN_SIZE,
        });
    }

    let speed = f32::from_le_bytes(buf[8..12].try_into().unwrap());
    let lap_time = i32::from_le_bytes(buf[40..44].try_into().unwrap());
    let lap_number = i32::from_le_bytes(buf[52..56].try_into().unwrap());
    let throttle = f32::from_le_bytes(buf[56..60].try_into().unwrap()).clamp(0.0, 1.0);
    let brake = f32::from_le_bytes(buf[60..64].try_into().unwrap()).clamp(0.0, 1.0);
    let rpm = i32::from_le_bytes(buf[68..72].try_into().unwrap());
    let steering = f32::from_le_bytes(buf[72..76].try_into().unwrap()).clamp(-1.0, 1.0);
    let gear = i32::from_le_bytes(buf[76..80].try_into().unwrap());
    let normalized_position = f32::from_le_bytes(buf[308..312].try_into().unwrap()).clamp(0.0, 1.0);

    Ok(TelemetryFrame {
        timestamp,
        speed,
        throttle,
        brake,
        steering,
        gear,
        rpm,
        normalized_position,
        lap_number,
        lap_time,
        session_time: None,
        session_type: None,
        track_position: None,
        delta: None,
    })
}
