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
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::AppError;
use crate::MetaNetworkError;
use crate::MetaRaftError;
use crate::MetaResultError;
use crate::MetaStorageError;

/// Top level error MetaNode would return.
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MetaError {
    #[error(transparent)]
    MetaNetworkError(#[from] MetaNetworkError),

    #[error(transparent)]
    MetaRaftError(#[from] MetaRaftError),

    #[error(transparent)]
    MetaStorageError(MetaStorageError),

    #[error(transparent)]
    MetaResultError(#[from] MetaResultError),

    #[error("{0}")]
    InvalidConfig(String),

    #[error("raft state present id={0}, can not create")]
    MetaStoreAlreadyExists(u64),

    #[error("raft state absent, can not open")]
    MetaStoreNotFound,

    #[error("{0}")]
    LoadConfigError(String),

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

    /// type to represent serialize/deserialize errors
    #[error(transparent)]
    SerdeError(AnyError),

    /// Error when encoding auth
    #[error(transparent)]
    EncodeError(AnyError),

    #[error(transparent)]
    AppError(#[from] AppError),

    /// Any other unclassified error.
    /// Other crate may return general error such as ErrorCode or anyhow::Error, which can not be classified by type.
    #[error(transparent)]
    Fatal(AnyError),
}

pub type MetaResult<T> = std::result::Result<T, MetaError>;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("InvalidMembership")]
pub struct InvalidMembership {}

impl From<MetaStorageError> for MetaError {
    fn from(e: MetaStorageError) -> Self {
        match e {
            MetaStorageError::AppError(app_err) => MetaError::AppError(app_err),
            _ => MetaError::MetaStorageError(e),
        }
    }
}
