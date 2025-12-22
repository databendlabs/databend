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

use std::time::Duration;
use std::time::SystemTime;

use base64::prelude::*;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::user_token::TokenType;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::Deserialize;
use serde::Serialize;

const TOKEN_PREFIX: &str = "bend-v1";
fn generate_secure_nonce() -> String {
    let mut random_bytes = [0u8; 16];
    OsRng.fill_bytes(&mut random_bytes);
    hex::encode(random_bytes)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionClaim {
    pub tenant: String,
    pub user: String,
    pub auth_role: Option<String>,
    #[serde(rename = "sid")]
    pub session_id: String,
    pub(crate) nonce: String,
    #[serde(rename = "exp")]
    pub expire_at_in_secs: u64,
}

pub fn unix_ts() -> Duration {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("The system clock is not properly set")
}

impl SessionClaim {
    pub fn new(
        session_id: String,
        tenant: &str,
        user: &str,
        auth_role: &Option<String>,
        ttl: Duration,
    ) -> Self {
        SessionClaim {
            tenant: tenant.to_string(),
            user: user.to_string(),
            auth_role: auth_role.clone(),
            session_id,
            nonce: generate_secure_nonce(),
            expire_at_in_secs: (unix_ts() + ttl).as_secs(),
        }
    }
    pub fn is_databend_token(token: &str) -> bool {
        token.starts_with(TOKEN_PREFIX)
    }

    pub fn get_type(token: &str) -> Result<TokenType> {
        TokenType::try_from(token.as_bytes()[TOKEN_PREFIX.len() + 1])
    }

    pub(crate) fn encode(&self, token_type: TokenType) -> String {
        let token = BASE64_STANDARD.encode(serde_json::to_vec(&self).unwrap());
        let t = token_type.to_string();
        format!("{TOKEN_PREFIX}-{t}-{token}")
    }

    pub fn decode(token: &str) -> Result<(Self, TokenType)> {
        let fmt_err = |reason: String| {
            ErrorCode::AuthenticateFailure(format!(
                "Failed to decode token: {reason}, token: {token}"
            ))
        };
        let t = Self::get_type(token)?;
        if token.len() < TOKEN_PREFIX.len() {
            return Err(fmt_err("too short".to_string()));
        }
        let token = &token.as_bytes()[TOKEN_PREFIX.len() + 3..];
        let json = BASE64_STANDARD
            .decode(token)
            .map_err(|e| fmt_err(format!("base64 decode error: {e}")))?;
        let claim = serde_json::from_slice::<SessionClaim>(&json)
            .map_err(|e| fmt_err(format!("json decode error: {e}")))?;
        Ok((claim, t))
    }

    pub fn expire_at(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(self.expire_at_in_secs)
    }
}
