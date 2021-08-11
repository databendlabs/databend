// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use serde::Deserialize;
use serde::Serialize;

/// RaftTxId is the essential info to identify an write operation to raft.
/// Logs with the same RaftTxId are considered the same and only the first of them will be applied.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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
