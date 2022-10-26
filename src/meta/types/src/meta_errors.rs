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

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::MetaAPIError;
use crate::MetaClientError;
use crate::MetaNetworkError;
use crate::MetaStorageError;

/// Top level error MetaNode would return.
#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MetaError {
    /// Errors occurred when accessing remote meta store service.
    #[error(transparent)]
    NetworkError(#[from] MetaNetworkError),

    #[error(transparent)]
    StorageError(#[from] MetaStorageError),

    #[error(transparent)]
    ClientError(#[from] MetaClientError),

    #[error(transparent)]
    APIError(#[from] MetaAPIError),
}

pub type MetaResult<T> = Result<T, MetaError>;
