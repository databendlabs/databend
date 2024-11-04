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

use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::Vote;

/// Contains information about a received snapshot data.
#[derive(Debug)]
pub struct Received {
    pub format: String,
    pub vote: Vote,
    pub snapshot_meta: SnapshotMeta,
    pub temp_path: String,

    pub remote_addr: String,

    /// number of bytes received.
    pub n_received: usize,

    /// number of bytes received.
    pub size_received: usize,
}

impl Received {
    pub fn stat_str(&self) -> impl fmt::Display {
        format!(
            "received {} chunks, {} bytes from {}",
            self.n_received, self.size_received, self.remote_addr
        )
    }
}
