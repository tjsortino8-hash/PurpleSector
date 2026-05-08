//! OIDC/JWKS JWT authentication for the gRPC gateway.
//!
//! Compatible with third-party IAM providers: Auth0, Clerk, Keycloak,
//! AWS Cognito, Google Identity Platform, etc.
//!
//! Environment variables:
//! - `AUTH_DISABLED=true` — bypass auth entirely (dev mode)
//! - `AUTH_ISSUER_URL` — OIDC issuer URL (e.g., `https://purplesector.us.auth0.com/`)
//! - `AUTH_AUDIENCE` — expected JWT audience claim
//! - `AUTH_JWKS_URL` — (optional) explicit JWKS endpoint; defaults to `{issuer}/.well-known/jwks.json`

use anyhow::{Context, Result};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Auth configuration loaded from environment.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub disabled: bool,
    pub dev_user_id: Option<String>,
    pub issuer_url: Option<String>,
    pub audience: Option<String>,
    pub jwks_url: Option<String>,
}

impl AuthConfig {
    pub fn from_env() -> Self {
        let disabled = std::env::var("AUTH_DISABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        Self {
            disabled,
            dev_user_id: std::env::var("DEV_USER_ID").ok(),
            issuer_url: std::env::var("AUTH_ISSUER_URL").ok(),
            audience: std::env::var("AUTH_AUDIENCE").ok(),
            jwks_url: std::env::var("AUTH_JWKS_URL").ok(),
        }
    }

    /// Resolve the JWKS URL from explicit config or OIDC discovery.
    pub fn resolved_jwks_url(&self) -> Option<String> {
        if let Some(url) = &self.jwks_url {
            return Some(url.clone());
        }
        self.issuer_url
            .as_ref()
            .map(|issuer| format!("{}/.well-known/jwks.json", issuer.trim_end_matches('/')))
    }
}

/// JWKS key set response from the identity provider.
#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<JwkKey>,
}

#[derive(Debug, Deserialize)]
struct JwkKey {
    kid: Option<String>,
    kty: String,
    n: Option<String>,
    e: Option<String>,
    alg: Option<String>,
}

/// Claims extracted from a validated JWT.
#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    pub sub: Option<String>,
    pub aud: Option<serde_json::Value>,
    pub iss: Option<String>,
    pub exp: Option<u64>,
}

/// JWT validator with cached JWKS keys.
pub struct JwtValidator {
    pub config: AuthConfig,
    // kid → DecodingKey cache
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl JwtValidator {
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Fetch JWKS keys from the identity provider and cache them.
    pub async fn refresh_keys(&self) -> Result<()> {
        let jwks_url = self
            .config
            .resolved_jwks_url()
            .context("No JWKS URL configured")?;

        debug!("Fetching JWKS from {jwks_url}");
        let resp: JwksResponse = reqwest::get(&jwks_url)
            .await
            .context("Failed to fetch JWKS")?
            .json()
            .await
            .context("Failed to parse JWKS response")?;

        let mut cache = self.keys.write().await;
        cache.clear();

        for key in &resp.keys {
            if key.kty == "RSA" {
                if let (Some(kid), Some(n), Some(e)) = (&key.kid, &key.n, &key.e) {
                    match DecodingKey::from_rsa_components(n, e) {
                        Ok(dk) => {
                            cache.insert(kid.clone(), dk);
                        }
                        Err(err) => {
                            warn!("Failed to parse RSA key kid={kid}: {err}");
                        }
                    }
                }
            }
        }

        info!("JWKS refreshed: {} keys cached", cache.len());
        Ok(())
    }

    /// Validate a JWT token and return the claims.
    pub async fn validate(&self, token: &str) -> Result<TokenClaims> {
        if self.config.disabled {
            let sub = self.config.dev_user_id.clone().unwrap_or_else(|| "dev-user".into());
            return Ok(TokenClaims {
                sub: Some(sub),
                aud: None,
                iss: None,
                exp: None,
            });
        }

        let header = decode_header(token).context("Invalid JWT header")?;
        let kid = header.kid.context("JWT missing kid header")?;

        // Try cached key first
        let keys = self.keys.read().await;
        let decoding_key = match keys.get(&kid) {
            Some(key) => key.clone(),
            None => {
                drop(keys);
                // Refresh and retry
                self.refresh_keys().await?;
                let keys = self.keys.read().await;
                keys.get(&kid)
                    .cloned()
                    .context(format!("Unknown key kid={kid} after JWKS refresh"))?
            }
        };

        let mut validation = Validation::new(Algorithm::RS256);
        if let Some(aud) = &self.config.audience {
            validation.set_audience(&[aud]);
        }
        if let Some(iss) = &self.config.issuer_url {
            validation.set_issuer(&[iss]);
        }

        let token_data = decode::<TokenClaims>(token, &decoding_key, &validation)
            .context("JWT validation failed")?;

        Ok(token_data.claims)
    }
}
