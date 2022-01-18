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

use anyerror::AnyError;
use common_exception::ErrorCode;
use common_exception::SerializedError;
use common_meta_types::NodeId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

/// Top level error MetaNode would return.
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MetaError {
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader),

    #[error("MembershipChangeInProgress")]
    MembershipChangeInProgress,

    #[error(transparent)]
    InvalidMembership(#[from] InvalidMembership),

    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),

    #[error(transparent)]
    ErrorCode(#[from] SerializedError),

    #[error("{0}")]
    UnknownError(String),
    // TODO(xp): RaftError needs impl Serialize etc.
    // #[error(transparent)]
    // RaftError(RaftError)
}

impl From<ErrorCode> for MetaError {
    fn from(e: ErrorCode) -> Self {
        MetaError::ErrorCode(SerializedError::from(e))
    }
}

impl From<MetaError> for ErrorCode {
    fn from(e: MetaError) -> Self {
        match e {
            MetaError::ErrorCode(err_code) => err_code.into(),
            _ => ErrorCode::MetaServiceError(e.to_string()),
        }
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("ConnectionError: {msg} source: {source}")]
pub struct ConnectionError {
    msg: String,
    #[source]
    source: AnyError,
}

impl ConnectionError {
    pub fn new(source: tonic::transport::Error, msg: String) -> Self {
        Self {
            msg,
            source: AnyError::new(&source),
        }
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("InvalidMembership")]
pub struct InvalidMembership {}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("ForwardToLeader: {leader:?}")]
pub struct ForwardToLeader {
    pub leader: Option<NodeId>,
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RetryableError {
    /// Trying to write to a non-leader returns the latest leader the raft node knows,
    /// to indicate the client to retry.
    #[error("request must be forwarded to leader: {leader}")]
    ForwardToLeader { leader: NodeId },
}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError,
}
