//! Assetto Corsa Competizione telemetry capture via UDP Broadcasting Protocol.
//!
//! ACC uses a registration-based UDP protocol at ~10Hz for session/car data,
//! combined with shared memory for physics (throttle, brake, steering, RPM).
//! On non-Windows platforms, shared memory is unavailable and those fields
//! default to zero.
//!
//! The Broadcasting Protocol flow:
//! 1. Send REGISTER_COMMAND_APPLICATION to ACC
//! 2. Receive REGISTRATION_RESULT
//! 3. Receive REALTIME_UPDATE (session state) and REALTIME_CAR_UPDATE (per-car data)
//! 4. On shutdown, send UNREGISTER_COMMAND_APPLICATION

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use super::{CaptureConfig, CaptureError, TelemetrySource};
use crate::proto::purplesector::TelemetryFrame;

// ACC Broadcasting Protocol message types (outbound)
const REGISTER_COMMAND_APPLICATION: u8 = 1;
const UNREGISTER_COMMAND_APPLICATION: u8 = 9;

// ACC Broadcasting Protocol message types (inbound)
const REGISTRATION_RESULT: u8 = 1;
const REALTIME_UPDATE: u8 = 2;
const REALTIME_CAR_UPDATE: u8 = 3;

/// ACC-specific capture configuration.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AccCaptureConfig {
    pub base: CaptureConfig,
    /// ACC broadcast port (default 9000).
    pub broadcast_port: u16,
    /// Display name for ACC registration.
    pub display_name: String,
    /// Connection password (from broadcasting.json).
    pub connection_password: String,
    /// Update interval in ms requested from ACC (default 100 = 10Hz).
    pub update_interval_ms: u32,
}

impl Default for AccCaptureConfig {
    fn default() -> Self {
        Self {
            base: CaptureConfig {
                udp_port: 20582,
                udp_host: "0.0.0.0".into(),
                target_host: Some("127.0.0.1".into()),
                target_port: None,
            },
            broadcast_port: 9000,
            display_name: "PurpleSector".into(),
            connection_password: String::new(),
            update_interval_ms: 100,
        }
    }
}

/// State parsed from an ACC REALTIME_UPDATE packet.
#[derive(Debug, Clone, Default)]
struct RealtimeUpdate {
    session_type: i32,
    session_time: f32,
    focused_car_index: i32,
}

/// State parsed from an ACC REALTIME_CAR_UPDATE packet.
#[derive(Debug, Clone)]
struct CarUpdate {
    car_index: u16,
    gear: i32,
    speed: f32,
    current_lap: i32,
    delta: i32,
    current_lap_time: i32,
    track_position: i32,
    spline_position: f32,
}

/// Assetto Corsa Competizione telemetry source.
pub struct AccSource {
    config: AccCaptureConfig,
    socket: Option<UdpSocket>,
    connection_id: i32,
    is_registered: bool,
    focused_car_index: i32,
    latest_realtime: RealtimeUpdate,
    buf: Vec<u8>,
}

impl AccSource {
    pub fn new(config: AccCaptureConfig) -> Self {
        Self {
            config,
            socket: None,
            connection_id: -1,
            is_registered: false,
            focused_car_index: -1,
            latest_realtime: RealtimeUpdate::default(),
            buf: vec![0u8; 4096],
        }
    }

    /// Build and send the ACC registration packet.
    async fn send_registration(&self) -> Result<(), CaptureError> {
        let socket = self.socket.as_ref().ok_or(CaptureError::Stopped)?;
        let target = self
            .config
            .base
            .target_host
            .as_deref()
            .unwrap_or("127.0.0.1");
        let addr: SocketAddr =
            format!("{}:{}", target, self.config.broadcast_port)
                .parse()
                .map_err(|e| CaptureError::HandshakeFailed(format!("Invalid target: {e}")))?;

        // ACC registration packet layout:
        // [u8 msg_type][u8 protocol_version]
        // [u16 display_name_len][utf16le display_name]
        // [u16 password_len][utf16le password]
        // [i32 update_interval]
        // [u16 command_password_len (0)]
        let display_utf16: Vec<u16> = self.config.display_name.encode_utf16().collect();
        let password_utf16: Vec<u16> = self.config.connection_password.encode_utf16().collect();

        let buf_size = 1 + 1
            + 2 + display_utf16.len() * 2
            + 2 + password_utf16.len() * 2
            + 4
            + 2;
        let mut buf = vec![0u8; buf_size];
        let mut offset = 0;

        buf[offset] = REGISTER_COMMAND_APPLICATION;
        offset += 1;
        buf[offset] = 4; // Protocol version 4
        offset += 1;

        // Display name
        buf[offset..offset + 2].copy_from_slice(&(display_utf16.len() as u16).to_le_bytes());
        offset += 2;
        for ch in &display_utf16 {
            buf[offset..offset + 2].copy_from_slice(&ch.to_le_bytes());
            offset += 2;
        }

        // Connection password
        buf[offset..offset + 2].copy_from_slice(&(password_utf16.len() as u16).to_le_bytes());
        offset += 2;
        for ch in &password_utf16 {
            buf[offset..offset + 2].copy_from_slice(&ch.to_le_bytes());
            offset += 2;
        }

        // Update interval
        buf[offset..offset + 4]
            .copy_from_slice(&(self.config.update_interval_ms as i32).to_le_bytes());
        offset += 4;

        // Empty command password
        buf[offset..offset + 2].copy_from_slice(&0u16.to_le_bytes());

        socket.send_to(&buf, addr).await?;
        info!("ACC registration sent to {addr}");
        Ok(())
    }

    /// Send unregistration request.
    async fn send_unregistration(&self) -> Result<(), CaptureError> {
        if self.connection_id < 0 {
            return Ok(());
        }
        if let Some(socket) = &self.socket {
            let target = self
                .config
                .base
                .target_host
                .as_deref()
                .unwrap_or("127.0.0.1");
            if let Ok(addr) =
                format!("{}:{}", target, self.config.broadcast_port).parse::<SocketAddr>()
            {
                let mut buf = vec![0u8; 5];
                buf[0] = UNREGISTER_COMMAND_APPLICATION;
                buf[1..5].copy_from_slice(&self.connection_id.to_le_bytes());
                let _ = socket.send_to(&buf, addr).await;
                debug!("ACC unregistration sent");
            }
        }
        Ok(())
    }

    /// Parse a REGISTRATION_RESULT packet.
    fn parse_registration_result(buf: &[u8]) -> Option<(i32, bool)> {
        if buf.len() < 7 {
            return None;
        }
        let connection_id = i32::from_le_bytes(buf[1..5].try_into().ok()?);
        let success = buf[5] == 1;
        Some((connection_id, success))
    }

    /// Parse a REALTIME_UPDATE packet.
    fn parse_realtime_update(buf: &[u8]) -> Option<RealtimeUpdate> {
        if buf.len() < 18 {
            return None;
        }
        let mut offset = 1;
        // event_index (u16)
        offset += 2;
        // session_index (u16)
        offset += 2;
        let session_type = buf[offset] as i32;
        offset += 1;
        // phase (u8)
        offset += 1;
        let session_time = f32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
        offset += 4;
        // session_end_time (f32)
        offset += 4;
        let focused_car_index = i32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);

        Some(RealtimeUpdate {
            session_type,
            session_time,
            focused_car_index,
        })
    }

    /// Parse a REALTIME_CAR_UPDATE packet.
    fn parse_car_update(buf: &[u8]) -> Option<CarUpdate> {
        if buf.len() < 70 {
            return None;
        }
        let mut offset = 1;

        let car_index = u16::from_le_bytes(buf[offset..offset + 2].try_into().ok()?);
        offset += 2;
        // driver_index (u16)
        offset += 2;
        // driver_count (u8)
        offset += 1;
        let gear = buf[offset] as i32;
        offset += 1;
        // world position (3 x f32)
        offset += 12;
        // velocity (3 x f32)
        let vx = f32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
        offset += 4;
        let vy = f32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
        offset += 4;
        let vz = f32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
        offset += 4;
        let speed = (vx * vx + vy * vy + vz * vz).sqrt() * 3.6;

        // rotation (3 x f32)
        offset += 12;
        // car damage (5 x f32)
        offset += 20;

        let current_lap = u16::from_le_bytes(buf[offset..offset + 2].try_into().ok()?) as i32;
        offset += 2;
        let delta = i32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
        offset += 4;
        // best_session_lap (i32)
        offset += 4;
        // last_lap (i32)
        offset += 4;
        let current_lap_time = i32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);
        offset += 4;
        // laps (u16)
        offset += 2;
        // cup_position (u16)
        offset += 2;
        let track_position = u16::from_le_bytes(buf[offset..offset + 2].try_into().ok()?) as i32;
        offset += 2;
        let spline_position = f32::from_le_bytes(buf[offset..offset + 4].try_into().ok()?);

        Some(CarUpdate {
            car_index,
            gear,
            speed,
            current_lap,
            delta,
            current_lap_time,
            track_position,
            spline_position,
        })
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}

#[async_trait::async_trait]
impl TelemetrySource for AccSource {
    async fn start(&mut self) -> Result<(), CaptureError> {
        let bind_addr = format!("{}:{}", self.config.base.udp_host, self.config.base.udp_port);
        let socket = UdpSocket::bind(&bind_addr).await?;
        info!("ACC UDP listener bound to {bind_addr}");
        self.socket = Some(socket);
        self.send_registration().await?;
        Ok(())
    }

    async fn next_sample(&mut self) -> Result<TelemetryFrame, CaptureError> {
        let socket = self.socket.as_ref().ok_or(CaptureError::Stopped)?;

        loop {
            let (len, _addr) = socket.recv_from(&mut self.buf).await?;
            if len == 0 {
                continue;
            }

            let msg_type = self.buf[0];
            let packet = &self.buf[..len];

            match msg_type {
                REGISTRATION_RESULT => {
                    if let Some((conn_id, success)) = Self::parse_registration_result(packet) {
                        if success {
                            self.connection_id = conn_id;
                            self.is_registered = true;
                            info!("ACC registered, connection_id={conn_id}");
                        } else {
                            warn!("ACC registration failed");
                        }
                    }
                    continue;
                }
                REALTIME_UPDATE => {
                    if let Some(update) = Self::parse_realtime_update(packet) {
                        self.focused_car_index = update.focused_car_index;
                        self.latest_realtime = update;
                    }
                    continue;
                }
                REALTIME_CAR_UPDATE => {
                    if let Some(car) = Self::parse_car_update(packet) {
                        if car.car_index as i32 == self.focused_car_index {
                            let timestamp = Self::now_ms();
                            return Ok(TelemetryFrame {
                                timestamp,
                                speed: car.speed,
                                gear: car.gear,
                                // Shared memory fields — zero until SHM integration
                                // is added (Windows-only via windows-rs).
                                throttle: 0.0,
                                brake: 0.0,
                                steering: 0.0,
                                rpm: 0,
                                normalized_position: car.spline_position,
                                lap_number: car.current_lap,
                                lap_time: car.current_lap_time,
                                session_time: Some(self.latest_realtime.session_time),
                                session_type: Some(self.latest_realtime.session_type),
                                track_position: Some(car.track_position),
                                delta: Some(car.delta),
                            });
                        }
                    }
                    continue;
                }
                _ => {
                    continue;
                }
            }
        }
    }

    async fn stop(&mut self) -> Result<(), CaptureError> {
        self.send_unregistration().await?;
        self.socket = None;
        self.is_registered = false;
        self.connection_id = -1;
        info!("ACC source stopped");
        Ok(())
    }

    fn name(&self) -> &str {
        "Assetto Corsa Competizione"
    }

    fn source_rate_hz(&self) -> u32 {
        // ACC broadcasting sends at 10Hz (100ms update interval)
        10
    }
}
