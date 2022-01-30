// Copyright 2022 Datafuse Labs.
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

use base64::decode_config;
use base64::URL_SAFE_NO_PAD;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use jwt_simple::prelude::RS256PublicKey;
use serde::Deserialize;
use serde::Serialize;

use crate::users::auth::jwt::PubKey;

const JWK_REFRESH_INTERVAL: u64 = 15;

#[derive(Debug, Serialize, Deserialize)]
pub struct JwkKey {
    pub kid: String,
    pub kty: String,
    pub alg: Option<String>,
    #[serde(default)]
    pub n: String,
    #[serde(default)]
    pub e: String,
}

fn decode(v: &str) -> Result<Vec<u8>> {
    decode_config(v.as_bytes(), URL_SAFE_NO_PAD)
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
            _ => Err(ErrorCode::InvalidConfig(format!(
                " current not support jwk with typ={:?}",
                self.kty
            ))),
        }
    }
}

#[derive(Deserialize)]
pub struct JwkKeys {
    pub keys: Vec<JwkKey>,
}

pub struct JwkKeyStore {
    url: String,
    keys: Arc<RwLock<HashMap<String, PubKey>>>,
    _refresh_interval: Duration,
}

impl JwkKeyStore {
    pub async fn new(url: String) -> Result<Self> {
        let _refresh_interval = Duration::from_secs(JWK_REFRESH_INTERVAL * 60);
        let keys = Arc::new(RwLock::new(HashMap::new()));
        let mut s = JwkKeyStore {
            url,
            keys,
            _refresh_interval,
        };
        s.load_keys().await?;
        Ok(s)
    }
}

impl JwkKeyStore {
    pub async fn load_keys(&mut self) -> Result<()> {
        let response = reqwest::get(&self.url).await.map_err(|e| {
            ErrorCode::NetworkRequestError(format!("Could not download JWKS: {}", e))
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

    pub(super) fn get_key(&self, key_id: Option<String>) -> Result<PubKey> {
        let keys = self.keys.read();
        match key_id {
            Some(kid) => match keys.get(&kid) {
                None => Err(ErrorCode::AuthenticateFailure(format!(
                    "key id {} not found",
                    &kid
                ))),
                Some(k) => Ok((*k).clone()),
            },
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
