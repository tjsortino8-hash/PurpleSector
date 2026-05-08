//! gRPC gateway service implementation.
//!
//! Receives `TelemetryBatch` streams from sim rigs, validates auth,
//! unnests batches into individual frames, and publishes to Redpanda
//! at the source rate (source_rate_hz) for smooth playback.

use anyhow::Result;
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use crate::auth::{AuthConfig, JwtValidator};
use crate::proto::purplesector::telemetry_ingress_server::TelemetryIngress;
use crate::proto::purplesector::{IngressAck, TelemetryBatch, TelemetryEnvelope};

/// The gRPC gateway service.
pub struct TelemetryGatewayService {
    producer: FutureProducer,
    topic: String,
    validator: Arc<JwtValidator>,
}

impl TelemetryGatewayService {
    pub async fn new(
        kafka_brokers: &str,
        kafka_topic: &str,
        auth_config: AuthConfig,
    ) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", kafka_brokers)
            .set("message.timeout.ms", "5000")
            .set("compression.type", "snappy")
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka producer: {e}"))?;

        let validator = Arc::new(JwtValidator::new(auth_config.clone()));

        // Pre-fetch JWKS keys if auth is enabled
        if !auth_config.disabled {
            if let Err(e) = validator.refresh_keys().await {
                tracing::warn!("Initial JWKS fetch failed (will retry on first request): {e}");
            }
        }

        Ok(Self {
            producer,
            topic: kafka_topic.to_string(),
            validator,
        })
    }

    /// Validate a bearer token and return the user ID.
    async fn validate_token(&self, token: &str) -> Result<String, Status> {
        match self.validator.validate(token).await {
            Ok(claims) => Ok(claims.sub.unwrap_or_else(|| "anonymous".into())),
            Err(e) => {
                error!("Auth failed: {e}");
                Err(Status::unauthenticated(format!("Invalid token: {e}")))
            }
        }
    }

    /// Unnest a batch and publish all frames concurrently to Kafka.
    /// Key is user_id:source - RisingWave will assign sessions.
    async fn publish_frames_at_rate(&self, batch: TelemetryBatch) -> Result<(), anyhow::Error> {
        let key = format!("{}:{}", batch.user_id, batch.source);
        let frame_count = batch.samples.len();

        debug!(
            "Publishing {} frames (source={}, rate={}Hz)",
            frame_count, batch.source, batch.source_rate_hz
        );

        // Encode all frames first (synchronous)
        let payloads: Vec<Vec<u8>> = batch.samples.into_iter().map(|frame| {
            TelemetryEnvelope {
                user_id: batch.user_id.clone(),
                source: batch.source.clone(),
                source_rate_hz: batch.source_rate_hz,
                frame: Some(frame),
            }.encode_to_vec()
        }).collect();

        // Publish all frames concurrently — one Kafka round-trip instead of N serial
        let futures: Vec<_> = payloads.iter().map(|payload| {
            let record = FutureRecord::to(&self.topic)
                .key(key.as_str())
                .payload(payload.as_slice());
            self.producer.send(record, Duration::from_secs(5))
        }).collect();

        for result in futures::future::join_all(futures).await {
            if let Err((e, _)) = result {
                return Err(anyhow::anyhow!("Kafka publish failed: {e}"));
            }
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl TelemetryIngress for TelemetryGatewayService {
    async fn stream_telemetry(
        &self,
        request: Request<Streaming<TelemetryBatch>>,
    ) -> Result<Response<IngressAck>, Status> {
        // Extract token synchronously before entering async streaming
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("")
            .to_string();

        if token.is_empty() && !self.validator.config.disabled {
            return Err(Status::unauthenticated("Missing authorization header"));
        }

        let user_id = self.validate_token(&token).await?;
        debug!("Authenticated stream from user={user_id}");

        let mut stream = request.into_inner();
        let mut batches_received: u64 = 0;
        let mut frames_published: u64 = 0;

        while let Some(mut batch) = stream.message().await.map_err(|e| {
            Status::internal(format!("Stream error: {e}"))
        })? {
            let batch_size = batch.batch_size;
            let actual_samples = batch.samples.len();

            // Override user_id with the authenticated identity so it matches
            // what Django registered in active_sessions (UUID, not username).
            batch.user_id = user_id.clone();

            // Normalize source: live collector sends "session-<hex>" but
            // active_sessions has "live" — rewrite any non-demo source to "live".
            if batch.source != "demo" {
                batch.source = "live".to_string();
            }

            debug!(
                "Received batch: user={}, source={}, rate={}Hz, samples={}",
                batch.user_id, batch.source, batch.source_rate_hz, actual_samples
            );

            // Validate batch_size matches actual sample count
            if batch_size as usize != actual_samples {
                warn!(
                    "Batch size mismatch: declared={batch_size}, actual={actual_samples}"
                );
            }

            batches_received += 1;

            // Unnest batch and publish frames at source_rate_hz
            if let Err(e) = self.publish_frames_at_rate(batch).await {
                error!("Failed to publish frames: {e}");
                return Err(Status::internal(format!("Failed to publish frames: {e}")));
            }

            frames_published += actual_samples as u64;
        }

        info!(
            "Stream complete: user={user_id}, batches={batches_received}, frames={frames_published}"
        );

        Ok(Response::new(IngressAck {
            batches_received,
            samples_received: frames_published,
        }))
    }
}
