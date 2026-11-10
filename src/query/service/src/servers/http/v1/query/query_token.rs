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

use std::fmt;

use rand::RngCore;
use rand::rngs::OsRng;
use serde::Serialize;
use serde::Serializer;
use subtle::ConstantTimeEq;

/// A bearer capability for one retained HTTP query. Its lifetime is the query's
/// lifetime, including the closed-query retention period used for final retries.
/// Never format the secret except when serializing an authorized response.
#[derive(Clone)]
pub struct QueryToken([u8; 32]);

impl QueryToken {
    pub(crate) fn new() -> Self {
        let mut bytes = [0; 32];
        OsRng.fill_bytes(&mut bytes);
        Self(bytes)
    }

    pub(crate) fn verify(&self, token: &str) -> bool {
        let mut bytes = [0; 32];
        hex::decode_to_slice(token, &mut bytes).is_ok() && bool::from(self.0.ct_eq(&bytes))
    }
}

impl fmt::Debug for QueryToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("QueryToken([REDACTED])")
    }
}

impl Serialize for QueryToken {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::QueryToken;

    #[test]
    fn query_token_roundtrip_and_redaction() {
        let token = QueryToken::new();
        let encoded = serde_json::to_value(&token).unwrap();
        let secret = encoded.as_str().unwrap();
        assert_eq!(secret.len(), 64);
        assert!(token.verify(secret));
        assert!(token.clone().verify(secret));
        assert!(!QueryToken::new().verify(secret));
        assert!(!format!("{token:?}").contains(secret));
        assert!(format!("{token:?}").contains("REDACTED"));
        for invalid in [
            "",
            "invalid",
            &"0".repeat(63),
            &"g".repeat(64),
            &"0".repeat(65),
        ] {
            assert!(!token.verify(invalid));
        }
    }
}
