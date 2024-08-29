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

use anyerror::AnyError;
use databend_common_meta_stoerr::MetaStorageError;

use crate::raft_types::InitializeError;
use crate::MetaNetworkError;
use crate::RaftError;

/// Error raised when meta-server startup.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum MetaStartupError {
    #[error(transparent)]
    InitializeError(#[from] InitializeError),

    #[error("fail to add node to cluster: {source}")]
    AddNodeError { source: AnyError },

    #[error("{0}")]
    InvalidConfig(String),

    #[error("fail to open store: {0}")]
    StoreOpenError(#[from] MetaStorageError),

    #[error(transparent)]
    ServiceStartupError(#[from] MetaNetworkError),

    #[error("raft state present id={0}, can not create")]
    MetaStoreAlreadyExists(u64),

    #[error("raft state absent, can not open")]
    MetaStoreNotFound,

    #[error("{0}")]
    MetaServiceError(String),
}

impl From<RaftError<InitializeError>> for MetaStartupError {
    fn from(value: RaftError<InitializeError>) -> Self {
        match value {
            RaftError::APIError(e) => e.into(),
            RaftError::Fatal(f) => Self::MetaServiceError(f.to_string()),
        }
    }
}
