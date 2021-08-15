// Copyright 2020 Datafuse Labs.
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

use std::convert::TryFrom;

use async_raft::AppData;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::Cmd;
use crate::meta_service::RaftMes;
use crate::meta_service::RaftTxId;

/// The application data request type which the `MetaStore` works with.
///
/// The client and the serial together provides external consistency:
/// If a client failed to recv the response, it  re-send another RaftRequest with the same
/// "client" and "serial", thus the raft engine is able to distinguish if a request is duplicated.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LogEntry {
    /// When not None, it is used to filter out duplicated logs, which are caused by retries by client.
    pub txid: Option<RaftTxId>,

    /// The action a client want to take.
    pub cmd: Cmd,
}

impl AppData for LogEntry {}

impl tonic::IntoRequest<RaftMes> for LogEntry {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftMes> for LogEntry {
    type Error = tonic::Status;

    fn try_from(mes: RaftMes) -> Result<Self, Self::Error> {
        let req: LogEntry =
            serde_json::from_str(&mes.data).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(req)
    }
}
