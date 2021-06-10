// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_tracing::tracing;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Meta;

/// The state machine of the `MemStore`.
/// It includes a core storage engine `Meta` and raft-related information: `last_applied_logs` and `client_serial_responses` to achieve idempotence.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MemStoreStateMachine {
    pub last_applied_log: u64,
    /// A mapping of client IDs to their state info:
    /// (serial, ClientResponse)
    pub client_serial_responses: HashMap<String, (u64, ClientResponse)>,

    pub meta: Meta,
}

impl MemStoreStateMachine {
    /// Apply an log entry to state machine.
    ///
    /// If a duplicated log entry is detected by checking data.txid, no update
    /// will be made and the previous resp is returned. In this way a client is able to re-send a
    /// command safely in case of network failure etc.
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn apply(&mut self, index: u64, data: &ClientRequest) -> anyhow::Result<ClientResponse> {
        self.last_applied_log = index;
        if let Some(ref txid) = data.txid {
            if let Some((serial, resp)) = self.client_serial_responses.get(&txid.client) {
                if serial == &txid.serial {
                    return Ok(resp.clone());
                }
            }
        }

        let resp = self.meta.apply(data)?;

        if let Some(ref txid) = data.txid {
            self.client_serial_responses
                .insert(txid.client.clone(), (txid.serial, resp.clone()));
        }
        Ok(resp)
    }
}
