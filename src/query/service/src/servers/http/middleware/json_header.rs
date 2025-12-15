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

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE;
use serde::Serialize;
use serde::de;

pub fn encode_json_header<T>(v: &T) -> String
where T: ?Sized + Serialize {
    let s = serde_json::to_string(&v).unwrap();
    URL_SAFE.encode(&s)
}

// use base64 encode whenever possible for safety
// but also accept raw JSON for test/debug/one-shot operations
pub fn decode_json_header<T>(key: &str, value: &str) -> Result<T, String>
where T: de::DeserializeOwned {
    if value.starts_with("{") {
        serde_json::from_slice(value.as_bytes())
            .map_err(|e| format!("Invalid value {value} for {key} JSON decode error: {e}",))?
    } else {
        let json = URL_SAFE.decode(value).map_err(|e| {
            format!(
                "Invalid value {} for {key}, base64 decode error: {}",
                value, e
            )
        })?;
        serde_json::from_slice(&json).map_err(|e| {
            format!(
                "Invalid value {value} for {key}, JSON value {},  decode error: {e}",
                String::from_utf8_lossy(&json)
            )
        })
    }
}
