// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use tonic::Status;

use crate::AppError;
use crate::MetaAPIError;
use crate::MetaClientError;
use crate::MetaNetworkError;
use crate::MetaStorageError;

/// Errors for a KVApi based application, such SchemaApi, ShareApi.
///
/// There are three subset of errors in it:
///
/// - (1) AppError: the errors that relate to the application of meta but not about meta itself.
///
/// - (2) Meta data errors raised by a embedded meta-store(not the remote meta-service): StorageError.
///
/// - (3) Meta data errors returned when accessing the remote meta-store service:
///   - ClientError: errors returned when creating a client.
///   - NetworkError: errors returned when sending/receiving RPC to/from a remote meta-store service.
///   - APIError: errors returned by the remote meta-store service.
///
/// Either a local or remote meta-store will returns (1) AppError.
/// An embedded meta-store only returns (1) and (2), while a remote meta-store service only returns (1) and (3)
#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum KVAppError {
    /// An error that indicates something wrong for the application of KVApi, but nothing wrong about meta.
    #[error(transparent)]
    AppError(#[from] AppError),

    // ---
    // --- Local embedded meta-store errors ---
    // ---
    /// Errors occurred when accessing local embedded meta-store.
    #[error(transparent)]
    StorageError(#[from] MetaStorageError),

    // ---
    // --- Remote meta-store service errors ---
    // ---
    /// Errors when invoking remote meta-service RPC.
    #[error(transparent)]
    NetworkError(#[from] MetaNetworkError),

    /// Errors when creating or accessing the client to a remote meta-service.
    #[error(transparent)]
    ClientError(#[from] MetaClientError),

    /// Remote error occurred when meta-service handling a request
    #[error(transparent)]
    APIError(#[from] MetaAPIError),
}

impl From<KVAppError> for ErrorCode {
    fn from(e: KVAppError) -> Self {
        match e {
            KVAppError::AppError(app_err) => app_err.into(),
            KVAppError::NetworkError(net_err) => net_err.into(),
            KVAppError::StorageError(sto_err) => sto_err.into(),
            KVAppError::ClientError(ce) => ce.into(),
            KVAppError::APIError(e) => e.into(),
        }
    }
}

impl From<Status> for KVAppError {
    fn from(s: Status) -> Self {
        let net_err = MetaNetworkError::from(s);
        Self::NetworkError(net_err)
    }
}
