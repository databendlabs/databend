// Copyright 2021 Datafuse Labs.
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

use openraft::error::ChangeMembershipError;
use openraft::error::Fatal;
use openraft::NodeId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

pub type ForwardToLeader = openraft::error::ForwardToLeader;

// represent raft related errors
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MetaRaftError {
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader),

    #[error(transparent)]
    ChangeMembershipError(#[from] ChangeMembershipError),

    #[error("{0}")]
    ConsistentReadError(String),

    #[error("{0}")]
    RaftFatal(#[from] Fatal),

    #[error("{0}")]
    ForwardRequestError(String),

    #[error("{0}")]
    NoLeaderError(String),

    #[error("{0}")]
    RequestNotForwardToLeaderError(String),
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RetryableError {
    /// Trying to write to a non-leader returns the latest leader the raft node knows,
    /// to indicate the client to retry.
    #[error("request must be forwarded to leader: {leader}")]
    ForwardToLeader { leader: NodeId },
}
