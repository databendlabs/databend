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
use common_exception::SerializedError;
use openraft::error::ChangeMembershipError;
use openraft::NodeId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::MetaStorageError;

/// Top level error MetaNode would return.
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MetaError {
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader),

    #[error(transparent)]
    ChangeMembershipError(#[from] ChangeMembershipError),

    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),

    #[error(transparent)]
    ErrorCode(#[from] SerializedError),

    #[error(transparent)]
    MetaStorageError(MetaStorageError),

    #[error("{0}")]
    UnknownError(String),

    #[error("{0}")]
    InvalidConfig(String),

    #[error("raft state present id={0}, can not create")]
    MetaStoreAlreadyExists(u64),

    #[error("raft state absent, can not open")]
    MetaStoreNotFound,

    #[error("serde_json error: {0}")]
    SerdeJsonError(String),

    #[error("{0}")]
    MetaStoreDamaged(String),

    #[error("{0}")]
    BadBytes(String),

    #[error("{0}")]
    LoadConfigError(String),

    #[error("{0}")]
    TLSConfigurationFailure(String),

    #[error("{0}")]
    StartMetaServiceError(String),

    #[error("{0}")]
    BadAddressFormat(String),

    #[error("{0}")]
    UnknownTable(String),

    #[error("{0}")]
    UnknownTableId(String),

    #[error("{0}")]
    MetaSrvError(String),

    #[error("{0}")]
    ConcurrentSnapshotInstall(String),

    #[error("{0}")]
    UnknownNode(String),

    #[error("{0}")]
    MetaServiceError(String),

    #[error("{0}")]
    CannotConnectNode(String),

    #[error("{0}")]
    IllegalRoleInfoFormat(String),

    #[error("{0}")]
    IllegalUserInfoFormat(String),

    #[error("{0}")]
    UnknownException(String),
}

pub type MetaResult<T> = std::result::Result<T, MetaError>;

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

/*
/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError,
}
*/
