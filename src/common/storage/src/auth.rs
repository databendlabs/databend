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

use std::io::Error;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use base64::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use http::HeaderValue;
use tokio::sync::RwLock;

// Token file reference.
#[derive(Debug)]
pub struct TokenFile {
    path: PathBuf,
    token: String,
    pub expires_at: DateTime<Utc>,
}

impl TokenFile {
    pub fn new(path: &Path) -> Result<Self, Error> {
        let token = std::fs::read_to_string(path).map_err(Error::other)?;
        Ok(Self {
            path: path.to_path_buf(),
            token,
            // This period was picked because it is half of the duration between when the kubelet
            // refreshes a projected service account token and when the original token expires.
            expires_at: Utc::now() + Duration::seconds(60),
        })
    }

    pub fn is_expiring(&self) -> bool {
        Utc::now() + Duration::seconds(10) > self.expires_at
    }

    // fast path return not null token from alive credential
    pub fn cached_token(&self) -> Option<&str> {
        (!self.is_expiring()).then(|| self.token.as_ref())
    }

    // slow path return token from credential
    pub fn token(&mut self) -> &str {
        if self.is_expiring() {
            // https://github.com/kubernetes/kubernetes/issues/68164
            if let Ok(token) = std::fs::read_to_string(&self.path) {
                self.token = token;
            }
            self.expires_at = Utc::now() + Duration::seconds(60);
        }
        self.token.as_ref()
    }
}

#[derive(Debug, Clone)]
pub enum RefreshableToken {
    File(Arc<RwLock<TokenFile>>),
    Direct(String),
}

fn bearer_header(token: &str) -> Result<HeaderValue, Error> {
    // trim spaces and base 64
    let token = BASE64_URL_SAFE.encode(token.trim());
    let mut value = HeaderValue::try_from(format!("Bearer {}", token))
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err))?;
    value.set_sensitive(true);
    Ok(value)
}

impl RefreshableToken {
    pub async fn to_header(&self) -> Result<HeaderValue, Error> {
        match self {
            RefreshableToken::File(file) => {
                let guard = file.read().await;
                if let Some(header) = guard.cached_token().map(bearer_header) {
                    return header;
                }
                // Drop the read guard before a write lock attempt to prevent deadlock.
                drop(guard);
                // Note that `token()` only reloads if the cached token is expiring.
                // A separate method to conditionally reload minimizes the need for an exclusive access.
                bearer_header(file.write().await.token())
            }
            RefreshableToken::Direct(token) => bearer_header(token),
        }
    }
}
