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

use common_exception::SerializedError;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::MetaNetworkError;
use crate::MetaRaftError;
use crate::MetaStorageError;

/// Top level error MetaNode would return.
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MetaError {
    #[error(transparent)]
    ErrorCode(#[from] SerializedError),

    #[error(transparent)]
    MetaNetworkError(#[from] MetaNetworkError),

    #[error(transparent)]
    MetaRaftError(#[from] MetaRaftError),

    #[error(transparent)]
    MetaStorageError(#[from] MetaStorageError),

    #[error("{0}")]
    InvalidConfig(String),

    #[error("raft state present id={0}, can not create")]
    MetaStoreAlreadyExists(u64),

    #[error("raft state absent, can not open")]
    MetaStoreNotFound,

    #[error("{0}")]
    LoadConfigError(String),

    #[error("{0}")]
    TLSConfigurationFailure(String),

    #[error("{0}")]
    StartMetaServiceError(String),

    #[error("{0}")]
    ConcurrentSnapshotInstall(String),

    #[error("{0}")]
    MetaServiceError(String),

    #[error("{0}")]
    IllegalRoleInfoFormat(String),

    #[error("{0}")]
    IllegalUserInfoFormat(String),
}

pub type MetaResult<T> = std::result::Result<T, MetaError>;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("InvalidMembership")]
pub struct InvalidMembership {}
