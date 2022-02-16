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

use std::fmt::Display;

use anyerror::AnyError;
use common_exception::ErrorCode;
use prost::EncodeError;

use crate::ConnectionError;
use crate::MetaError;
use crate::MetaNetworkError;
use crate::MetaResult;

impl From<MetaError> for ErrorCode {
    fn from(e: MetaError) -> Self {
        match e {
            MetaError::AppError(app_err) => app_err.into(),
            MetaError::MetaNetworkError(net_err) => net_err.into(),

            // Except application error and part of network error,
            // all other errors are not handleable and can only be converted to a fatal error.
            //
            MetaError::MetaRaftError(e) => ErrorCode::MetaServiceError(e.to_string()),
            MetaError::MetaStorageError(e) => ErrorCode::MetaServiceError(e.to_string()),
            MetaError::MetaResultError(e) => ErrorCode::MetaServiceError(e.to_string()),
            MetaError::InvalidConfig(e) => ErrorCode::MetaServiceError(e),
            MetaError::MetaStoreAlreadyExists(e) => {
                ErrorCode::MetaServiceError(format!("meta store already exists: {}", e))
            }
            MetaError::MetaStoreNotFound => {
                ErrorCode::MetaServiceError("MetaStoreNotFound".to_string())
            }
            MetaError::LoadConfigError(e) => ErrorCode::MetaServiceError(e),
            MetaError::StartMetaServiceError(e) => ErrorCode::MetaServiceError(e),
            MetaError::ConcurrentSnapshotInstall(e) => ErrorCode::MetaServiceError(e),
            MetaError::MetaServiceError(e) => ErrorCode::MetaServiceError(e),
            MetaError::IllegalRoleInfoFormat(e) => ErrorCode::MetaServiceError(e),
            MetaError::IllegalUserInfoFormat(e) => ErrorCode::MetaServiceError(e),
            MetaError::SerdeError(e) => ErrorCode::MetaServiceError(e.to_string()),
            MetaError::EncodeError(e) => ErrorCode::MetaServiceError(e.to_string()),
            MetaError::Fatal(e) => ErrorCode::MetaServiceError(e.to_string()),
        }
    }
}

pub trait ToMetaError<T, E, CtxFn>
where E: Display + Send + Sync + 'static
{
    /// Wrap the error value with MetaError. It is lazily evaluated:
    /// only when an error does occur.
    ///
    /// `err_code_fn` is one of the MetaError builder function such as `MetaError::Ok`.
    /// `context_fn` builds display_text for the MetaError.
    fn map_error_to_meta_error<ErrFn, D>(
        self,
        err_code_fn: ErrFn,
        context_fn: CtxFn,
    ) -> MetaResult<T>
    where
        ErrFn: FnOnce(String) -> MetaError,
        D: Display,
        CtxFn: FnOnce() -> D;
}

impl<T, E, CtxFn> ToMetaError<T, E, CtxFn> for std::result::Result<T, E>
where E: Display + Send + Sync + 'static
{
    fn map_error_to_meta_error<ErrFn, D>(
        self,
        make_exception: ErrFn,
        context_fn: CtxFn,
    ) -> MetaResult<T>
    where
        ErrFn: FnOnce(String) -> MetaError,
        D: Display,
        CtxFn: FnOnce() -> D,
    {
        self.map_err(|error| {
            let err_text = format!("{}, cause: {}", context_fn(), error);
            make_exception(err_text)
        })
    }
}

// ser/de to/from tonic::Status
impl From<tonic::Status> for MetaError {
    fn from(status: tonic::Status) -> Self {
        MetaError::MetaNetworkError(MetaNetworkError::ConnectionError(ConnectionError::new(
            status, "",
        )))
    }
}

impl From<serde_json::Error> for MetaError {
    fn from(error: serde_json::Error) -> MetaError {
        MetaError::SerdeError(AnyError::new(&error))
    }
}

impl From<EncodeError> for MetaError {
    fn from(e: EncodeError) -> MetaError {
        MetaError::EncodeError(AnyError::new(&e))
    }
}
