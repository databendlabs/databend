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

use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use display_more::DisplayUnixTimeStampExt;

use crate::TxnPutRequest;

impl TxnPutRequest {
    pub fn new(
        key: impl ToString,
        value: Vec<u8>,
        prev_value: bool,
        expire_at: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Self {
        Self {
            key: key.to_string(),
            value,
            prev_value,
            expire_at,
            ttl_ms,
        }
    }
}

impl Display for TxnPutRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Put key={}", self.key)?;
        if let Some(expire_at) = self.expire_at {
            write!(
                f,
                " expire_at: {}",
                Duration::from_millis(expire_at).display_unix_timestamp()
            )?;
        }
        if let Some(ttl_ms) = self.ttl_ms {
            write!(f, "  ttl: {:?}", Duration::from_millis(ttl_ms))?;
        }
        Ok(())
    }
}
