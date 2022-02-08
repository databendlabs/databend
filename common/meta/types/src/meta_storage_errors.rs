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

use common_exception::ErrorCode;
use common_exception::SerializedError;
use serde::Deserialize;
use serde::Serialize;
use sled::Error as SledError;
use thiserror::Error;

use crate::MetaError;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MetaStorageError {
    // type to represent serialize errors
    #[error("{0}")]
    SerializeError(String),

    #[error(transparent)]
    ErrorCode(#[from] SerializedError),

    #[error("{0}")]
    BadBytes(String),

    #[error("{0}")]
    StorageError(String),

    #[error("{0}")]
    MetaStoreDamaged(String),

    #[error("{0}")]
    TransactionAbort(String),

    #[error("{0}")]
    TransactionError(String),

    #[error("{0}")]
    MetaSrvError(String),
}

pub type MetaStorageResult<T> = std::result::Result<T, MetaStorageError>;

impl From<MetaError> for MetaStorageError {
    fn from(e: MetaError) -> Self {
        MetaStorageError::MetaSrvError(format!("metasrv error: {:?}", e))
    }
}

impl From<ErrorCode> for MetaStorageError {
    fn from(e: ErrorCode) -> Self {
        MetaStorageError::ErrorCode(SerializedError::from(e))
    }
}

impl From<MetaStorageError> for ErrorCode {
    fn from(e: MetaStorageError) -> Self {
        match e {
            MetaStorageError::ErrorCode(err_code) => err_code.into(),
            _ => ErrorCode::MetaStorageError(e.to_string()),
        }
    }
}

impl From<std::string::FromUtf8Error> for MetaStorageError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        MetaStorageError::BadBytes(format!(
            "Bad bytes, cannot parse bytes with UTF8, cause: {}",
            error
        ))
    }
}

// from serde error to MetaStorageError::SerializeError
impl From<serde_json::Error> for MetaStorageError {
    fn from(error: serde_json::Error) -> MetaStorageError {
        MetaStorageError::SerializeError(format!("json se/de error: {:?}", error))
    }
}

// from sled error to MetaStorageError::StorageError
impl From<SledError> for MetaStorageError {
    fn from(error: SledError) -> MetaStorageError {
        MetaStorageError::StorageError(format!("sled error: {:?}", error))
    }
}

pub trait ToMetaStorageError<T, E, CtxFn>
where E: Display + Send + Sync + 'static
{
    /// Wrap the error value with MetaError. It is lazily evaluated:
    /// only when an error does occur.
    ///
    /// `err_code_fn` is one of the MetaError builder function such as `MetaError::Ok`.
    /// `context_fn` builds display_text for the MetaError.
    fn map_error_to_meta_storage_error<ErrFn, D>(
        self,
        err_code_fn: ErrFn,
        context_fn: CtxFn,
    ) -> MetaStorageResult<T>
    where
        ErrFn: FnOnce(String) -> MetaStorageError,
        D: Display,
        CtxFn: FnOnce() -> D;
}

impl<T, E, CtxFn> ToMetaStorageError<T, E, CtxFn> for std::result::Result<T, E>
where E: Display + Send + Sync + 'static
{
    fn map_error_to_meta_storage_error<ErrFn, D>(
        self,
        make_exception: ErrFn,
        context_fn: CtxFn,
    ) -> MetaStorageResult<T>
    where
        ErrFn: FnOnce(String) -> MetaStorageError,
        D: Display,
        CtxFn: FnOnce() -> D,
    {
        self.map_err(|error| {
            let err_text = format!("meta storage error: {}, cause: {}", context_fn(), error);
            make_exception(err_text)
        })
    }
}

// ser/de to/from sled::transaction::TransactionError,sled::transaction::ConflictableTransactionError
impl<T: Display> From<sled::transaction::ConflictableTransactionError<T>> for MetaStorageError {
    fn from(error: sled::transaction::ConflictableTransactionError<T>) -> Self {
        match error {
            sled::transaction::ConflictableTransactionError::Abort(e) => {
                MetaStorageError::TransactionAbort(format!("Transaction abort, cause: {}", e))
            }
            sled::transaction::ConflictableTransactionError::Storage(e) => {
                MetaStorageError::TransactionError(format!(
                    "Transaction storage error, cause: {}",
                    e
                ))
            }
            _ => MetaStorageError::TransactionError(String::from("Unexpect transaction error")),
        }
    }
}

impl<E: Display> From<sled::transaction::TransactionError<E>> for MetaStorageError {
    fn from(error: sled::transaction::TransactionError<E>) -> Self {
        match error {
            sled::transaction::TransactionError::Abort(e) => {
                MetaStorageError::TransactionAbort(format!("Transaction abort, cause: {}", e))
            }
            sled::transaction::TransactionError::Storage(e) => MetaStorageError::TransactionError(
                format!("Transaction storage error, cause :{}", e),
            ),
        }
    }
}
