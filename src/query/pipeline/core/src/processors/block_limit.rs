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

use std::sync::atomic::AtomicU64;

/// DataBlock limit in rows and bytes
pub struct BlockLimit {
    rows: AtomicU64,
    bytes: AtomicU64,
}

impl BlockLimit {
    pub fn new(rows: u64, bytes: u64) -> Self {
        BlockLimit {
            rows: AtomicU64::new(rows),
            bytes: AtomicU64::new(bytes),
        }
    }
}

impl Default for BlockLimit {
    fn default() -> Self {
        BlockLimit {
            rows: AtomicU64::new(u64::MAX),
            bytes: AtomicU64::new(u64::MAX),
        }
    }
}
