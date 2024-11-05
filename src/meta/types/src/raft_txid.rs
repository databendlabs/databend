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

use serde::Deserialize;
use serde::Serialize;

/// RaftTxId is the essential info to identify an write operation to raft.
/// Logs with the same RaftTxId are considered the same and only the first of them will be applied.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct RaftTxId {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    /// TODO(xp): a client must generate consistent `client` and globally unique serial.
    /// TODO(xp): in this impl the state machine records only one serial, which implies serial must be monotonic incremental for every client.
    pub serial: u64,
}

impl RaftTxId {
    pub fn new(client: &str, serial: u64) -> Self {
        Self {
            client: client.to_string(),
            serial,
        }
    }
}

impl Display for RaftTxId {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "txid-{}-{}", &self.client, self.serial)
    }
}
