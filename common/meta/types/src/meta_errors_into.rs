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

use crate::MetaError;
use crate::MetaResult;

impl From<ErrorCode> for MetaError {
    fn from(e: ErrorCode) -> Self {
        MetaError::ErrorCode(SerializedError::from(e))
    }
}

impl From<MetaError> for ErrorCode {
    fn from(e: MetaError) -> Self {
        match e {
            MetaError::ErrorCode(err_code) => err_code.into(),
            _ => {
                println!("MetaError:{:?}", e);
                ErrorCode::MetaServiceError(e.to_string())
            }
        }
    }
}

impl From<serde_json::Error> for MetaError {
    fn from(error: serde_json::Error) -> Self {
        MetaError::SerdeJsonError(format!("{}", error))
    }
}

impl From<std::string::FromUtf8Error> for MetaError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        MetaError::BadBytes(format!(
            "Bad bytes, cannot parse bytes with UTF8, cause: {}",
            error
        ))
    }
}

impl From<std::net::AddrParseError> for MetaError {
    fn from(error: std::net::AddrParseError) -> Self {
        MetaError::BadAddressFormat(format!("Bad address format, cause: {}", error))
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

// ser/de to/from sled::transaction::TransactionError,sled::transaction::ConflictableTransactionError
impl<T: Display> From<sled::transaction::ConflictableTransactionError<T>> for MetaError {
    fn from(error: sled::transaction::ConflictableTransactionError<T>) -> Self {
        match error {
            sled::transaction::ConflictableTransactionError::Abort(e) => {
                MetaError::TransactionAbort(format!("Transaction abort, cause: {}", e))
            }
            sled::transaction::ConflictableTransactionError::Storage(e) => {
                MetaError::TransactionError(format!("Transaction storage error, cause: {}", e))
            }
            _ => MetaError::MetaSrvError(String::from("Unexpect transaction error")),
        }
    }
}

impl<E: Display> From<sled::transaction::TransactionError<E>> for MetaError {
    fn from(error: sled::transaction::TransactionError<E>) -> Self {
        match error {
            sled::transaction::TransactionError::Abort(e) => {
                MetaError::TransactionAbort(format!("Transaction abort, cause: {}", e))
            }
            sled::transaction::TransactionError::Storage(e) => {
                MetaError::TransactionError(format!("Transaction storage error, cause :{}", e))
            }
        }
    }
}

// ser/de to/from tonic::Status
impl From<&tonic::Status> for MetaError {
    fn from(status: &tonic::Status) -> Self {
        MetaError::ErrorCode(SerializedError::from(ErrorCode::from(status)))
    }
}

impl From<tonic::Status> for MetaError {
    fn from(status: tonic::Status) -> Self {
        (&status).into()
    }
}
