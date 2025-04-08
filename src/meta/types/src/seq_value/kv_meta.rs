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

use map_api::expirable::Expirable;
use serde::Deserialize;
use serde::Serialize;

/// The meta data of a record in kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVMeta {
    /// expiration time in second since 1970
    pub(crate) expire_at: Option<u64>,
}

impl KVMeta {
    /// Create a new KVMeta
    pub fn new(expire_at: Option<u64>) -> Self {
        Self { expire_at }
    }

    /// Create a KVMeta with a absolute expiration time in second since 1970-01-01.
    pub fn new_expire(expire_at: u64) -> Self {
        Self {
            expire_at: Some(expire_at),
        }
    }

    /// Returns expire time in millisecond since 1970.
    pub fn get_expire_at_ms(&self) -> Option<u64> {
        self.expire_at.map(|t| t * 1000)
    }
}

impl fmt::Display for KVMeta {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.expire_at {
            Some(expire_at) => write!(f, "(expire_at: {})", expire_at),
            None => write!(f, "()"),
        }
    }
}

impl Expirable for KVMeta {
    fn expires_at_ms_opt(&self) -> Option<u64> {
        self.expire_at.map(|t| t * 1000)
    }
}
