//! Assetto Corsa telemetry capture via UDP.
//!
//! AC sends binary UDP packets at ~60Hz. This module handles the handshake
//! protocol and parses raw packets into `TelemetryFrame` values.

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

use super::{CaptureConfig, CaptureError, TelemetrySource, parse_ac_packet};
use crate::proto::purplesector::TelemetryFrame;

/// Assetto Corsa handshake operation IDs.
const OP_HANDSHAKE: i32 = 0;
const OP_SUBSCRIBE_UPDATE: i32 = 1;
const OP_DISMISS: i32 = 3;

/// Assetto Corsa telemetry source.
pub struct AcSource {
    config: CaptureConfig,
    socket: Option<UdpSocket>,
    handshake_completed: bool,
    buf: Vec<u8>,
}

impl AcSource {
    pub fn new(config: CaptureConfig) -> Self {
        Self {
            config,
            socket: None,
            handshake_completed: false,
            buf: vec![0u8; 4096],
        }
    }

    /// Build and send an AC handshake packet.
    fn build_handshake_packet(identifier: i32, version: i32, operation: i32) -> Vec<u8> {
        let mut pkt = vec![0u8; 12];
        pkt[0..4].copy_from_slice(&identifier.to_le_bytes());
        pkt[4..8].copy_from_slice(&version.to_le_bytes());
        pkt[8..12].copy_from_slice(&operation.to_le_bytes());
        pkt
    }

    async fn send_handshake(&self) -> Result<(), CaptureError> {
        let socket = self.socket.as_ref().ok_or_else(|| {
            CaptureError::HandshakeFailed("Socket not initialized".into())
        })?;
        let target = self
            .config
            .target_host
            .as_deref()
            .unwrap_or("127.0.0.1");
        let game_port = self.config.target_port.unwrap_or(9996);
        let addr: SocketAddr = format!("{}:{}", target, game_port).parse().map_err(
            |e| CaptureError::HandshakeFailed(format!("Invalid target address: {e}")),
        )?;

        let pkt = Self::build_handshake_packet(0, 1, OP_HANDSHAKE);
        socket.send_to(&pkt, addr).await?;
        info!("AC handshake sent to {addr}");
        Ok(())
    }

    async fn send_subscribe(&self, addr: SocketAddr) -> Result<(), CaptureError> {
        let socket = self.socket.as_ref().ok_or_else(|| {
            CaptureError::HandshakeFailed("Socket not initialized".into())
        })?;
        let pkt = Self::build_handshake_packet(0, 1, OP_SUBSCRIBE_UPDATE);
        socket.send_to(&pkt, addr).await?;
        info!("AC subscribe sent to {addr}");
        Ok(())
    }

    async fn send_dismiss(&self) -> Result<(), CaptureError> {
        if let Some(socket) = &self.socket {
            let target = self
                .config
                .target_host
                .as_deref()
                .unwrap_or("127.0.0.1");
            let game_port = self.config.target_port.unwrap_or(9996);
            if let Ok(addr) = format!("{}:{}", target, game_port).parse::<SocketAddr>() {
                let pkt = Self::build_handshake_packet(0, 1, OP_DISMISS);
                let _ = socket.send_to(&pkt, addr).await;
                debug!("AC dismiss sent");
            }
        }
        Ok(())
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}

#[async_trait::async_trait]
impl TelemetrySource for AcSource {
    async fn start(&mut self) -> Result<(), CaptureError> {
        let bind_addr = format!("{}:{}", self.config.udp_host, self.config.udp_port);
        let socket = UdpSocket::bind(&bind_addr).await?;
        info!("AC UDP listener bound to {bind_addr}");
        self.socket = Some(socket);
        self.send_handshake().await?;
        Ok(())
    }

    async fn next_sample(&mut self) -> Result<TelemetryFrame, CaptureError> {
        let socket = self
            .socket
            .as_ref()
            .ok_or(CaptureError::Stopped)?;

        loop {
            let (len, addr) = socket.recv_from(&mut self.buf).await?;

            if !self.handshake_completed {
                // First response from AC is the handshake reply — send subscribe
                self.send_subscribe(addr).await?;
                self.handshake_completed = true;
                info!("AC handshake completed with {addr}");
                continue;
            }

            let timestamp = Self::now_ms();
            match parse_ac_packet(&self.buf[..len], timestamp) {
                Ok(sample) => return Ok(sample),
                Err(CaptureError::PacketTooSmall { got, min }) => {
                    warn!("AC packet too small ({got} < {min}), skipping");
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn stop(&mut self) -> Result<(), CaptureError> {
        self.send_dismiss().await?;
        self.socket = None;
        self.handshake_completed = false;
        info!("AC source stopped");
        Ok(())
    }

    fn name(&self) -> &str {
        "Assetto Corsa"
    }

    fn source_rate_hz(&self) -> u32 {
        60
    }
}
