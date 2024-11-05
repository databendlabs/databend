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

use crate::Cmd;
use crate::RaftTxId;

/// The application data request type which the `metasrv` works with.
///
/// The client and the serial together provides external consistency:
/// If a client failed to recv the response, it  re-send another RaftRequest with the same
/// "client" and "serial", thus the raft engine is able to distinguish if a request is duplicated.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct LogEntry {
    /// When not None, it is used to filter out duplicated logs, which are caused by retries by client.
    pub txid: Option<RaftTxId>,

    /// The time in millisecond when this log is proposed by the leader.
    ///
    /// State machine depends on clock time to expire values.
    /// The time to use has to be consistent on leader and followers.
    /// Otherwise an `apply` results in different state on leader and followers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_ms: Option<u64>,

    /// The action a client want to take.
    pub cmd: Cmd,
}

impl Display for LogEntry {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if let Some(txid) = &self.txid {
            write!(f, "txid: {:?}", txid)?;
        }
        if let Some(time) = &self.time_ms {
            write!(f, "time: {} ms", time)?;
        }

        write!(f, " cmd: {}", self.cmd)
    }
}

impl LogEntry {
    pub fn new(cmd: Cmd) -> Self {
        Self {
            txid: None,
            time_ms: None,
            cmd,
        }
    }
    pub fn with_txid(mut self, txid: Option<RaftTxId>) -> Self {
        self.txid = txid;
        self
    }
}
