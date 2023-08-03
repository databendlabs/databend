// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use base64::engine::general_purpose;
use base64::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use jwt_simple::prelude::ES256PublicKey;
use jwt_simple::prelude::RS256PublicKey;
use p256::EncodedPoint;
use p256::FieldBytes;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;

use super::PubKey;

const JWK_REFRESH_INTERVAL: u64 = 15;

#[derive(Debug, Serialize, Deserialize)]
pub struct JwkKey {
    pub kid: String,
    pub kty: String,
    pub alg: Option<String>,

    /// (Modulus) Parameter for kty `RSA`.
    #[serde(default)]
    pub n: String,
    /// (Exponent) Parameter for kty `RSA`.
    #[serde(default)]
    pub e: String,

    /// (X Coordinate) Parameter for kty `EC`
    #[serde(default)]
    pub x: String,
    /// (Y Coordinate) Parameter for kty `EC`
    #[serde(default)]
    pub y: String,
}

fn decode(v: &str) -> Result<Vec<u8>> {
    general_purpose::URL_SAFE_NO_PAD
        .decode(v.as_bytes())
        .map_err(|e| ErrorCode::InvalidConfig(e.to_string()))
}

impl JwkKey {
    fn get_public_key(&self) -> Result<PubKey> {
        match self.kty.as_str() {
            // Todo(youngsofun): the "alg" field is optional, maybe we need a config for it
            "RSA" => {
                let k = RS256PublicKey::from_components(&decode(&self.n)?, &decode(&self.e)?)?;
                Ok(PubKey::RSA256(k))
            }
            "EC" => {
                // borrowed from https://github.com/RustCrypto/traits/blob/master/elliptic-curve/src/jwk.rs#L68
                let xs = decode(&self.x)?;
                let x = FieldBytes::from_slice(&xs);
                let ys = decode(&self.y)?;
                let y = FieldBytes::from_slice(&ys);
                let ep = EncodedPoint::from_affine_coordinates(x, y, false);

                let k = ES256PublicKey::from_bytes(ep.as_bytes())?;
                Ok(PubKey::ES256(k))
            }
            _ => Err(ErrorCode::InvalidConfig(format!(
                " current not support jwk with typ={:?}",
                self.kty
            ))),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct JwkKeys {
    pub keys: Vec<JwkKey>,
}

pub struct JwkKeyStore {
    pub(crate) url: String,
    keys: Arc<RwLock<HashMap<String, PubKey>>>,
    pub(crate) last_refreshed_at: RwLock<Option<Instant>>,
    pub(crate) refresh_interval: Duration,
}

impl JwkKeyStore {
    pub fn new(url: String) -> Self {
        let refresh_interval = Duration::from_secs(JWK_REFRESH_INTERVAL * 60);
        let keys = Arc::new(RwLock::new(HashMap::new()));
        Self {
            url,
            keys,
            refresh_interval,
            last_refreshed_at: RwLock::new(None),
        }
    }
    pub fn url(&self) -> String {
        self.url.clone()
    }
}

impl JwkKeyStore {
    #[async_backtrace::framed]
    async fn load_keys(&self) -> Result<()> {
        let response = reqwest::get(&self.url).await.map_err(|e| {
            ErrorCode::AuthenticateFailure(format!("Could not download JWKS: {}", e))
        })?;
        let body = response.text().await.unwrap();
        let jwk_keys = serde_json::from_str::<JwkKeys>(&body)
            .map_err(|e| ErrorCode::InvalidConfig(format!("Failed to parse keys: {}", e)))?;
        let mut new_keys: HashMap<String, PubKey> = HashMap::new();
        for k in &jwk_keys.keys {
            new_keys.insert(k.kid.to_string(), k.get_public_key()?);
        }
        let mut keys = self.keys.write();
        *keys = new_keys;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn maybe_reload_keys(&self) -> Result<()> {
        let need_reload = {
            let last_refreshed_at = *self.last_refreshed_at.read();
            last_refreshed_at.is_none()
                || last_refreshed_at.unwrap().elapsed() > self.refresh_interval
        };
        if need_reload {
            self.load_keys().await?;
            self.last_refreshed_at.write().replace(Instant::now());
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub(super) async fn get_key(&self, key_id: Option<String>) -> Result<PubKey> {
        self.maybe_reload_keys().await?;
        let keys = self.keys.read();
        match key_id {
            Some(kid) => keys
                .get(&kid)
                .cloned()
                .ok_or(ErrorCode::AuthenticateFailure(format!(
                    "key id {} not found",
                    &kid
                ))),
            None => {
                if keys.len() != 1 {
                    Err(ErrorCode::AuthenticateFailure(
                        "must specify key_id for jwt when multi keys exists ",
                    ))
                } else {
                    Ok((*keys.iter().next().unwrap().1).clone())
                }
            }
        }
    }
}
