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
use serde::Deserialize;
use serde::Serialize;
use sled::transaction::UnabortableTransactionError;
use thiserror::Error;

use crate::error_context::ErrorWithContext;
use crate::MatchSeq;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error)]
pub enum MetaStorageError {
    // type to represent bytes format errors
    #[error("{0}")]
    BytesError(String),

    // type to represent serialize/deserialize errors
    #[error("{0}")]
    SerdeError(String),

    /// An AnyError built from sled::Error.
    #[error(transparent)]
    SledError(AnyError),

    #[error(transparent)]
    Damaged(AnyError),

    /// Error that is related to snapshot
    #[error(transparent)]
    SnapshotError(AnyError),

    /// An internal error that inform txn to retry.
    #[error("Conflict when execute transaction, just retry")]
    TransactionConflict,

    /// An application error that cause transaction to abort.
    #[error("{0}")]
    AppError(#[from] AppError),
}

/// Output message for end users, with sensitive info stripped.
pub trait AppErrorMessage: Display {
    fn message(&self) -> String {
        format!("{}", self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error)]
#[error("DatabaseAlreadyExists: `{db_name}` while `{context}`")]
pub struct DatabaseAlreadyExists {
    db_name: String,
    context: String,
}

impl DatabaseAlreadyExists {
    pub fn new(db_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error)]
#[error("TableAlreadyExists: {table_name} while {context}")]
pub struct TableAlreadyExists {
    table_name: String,
    context: String,
}

impl TableAlreadyExists {
    pub fn new(table_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error)]
#[error("TableVersionMismatched: {table_id} expect `{expect}` but `{curr}`  while `{context}`")]
pub struct TableVersionMismatched {
    table_id: u64,
    expect: MatchSeq,
    curr: u64,
    context: String,
}

impl TableVersionMismatched {
    pub fn new(table_id: u64, expect: MatchSeq, curr: u64, context: impl Into<String>) -> Self {
        Self {
            table_id,
            expect,
            curr,
            context: context.into(),
        }
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("UnknownDatabase: `{db_name}` while `{context}`")]
pub struct UnknownDatabase {
    db_name: String,
    context: String,
}

impl UnknownDatabase {
    pub fn new(db_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("UnknownDatabaseId: `{db_id}` while `{context}`")]
pub struct UnknownDatabaseId {
    db_id: u64,
    context: String,
}

impl UnknownDatabaseId {
    pub fn new(db_id: u64, context: String) -> UnknownDatabaseId {
        Self { db_id, context }
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("UnknownTable: `{table_name}` while `{context}`")]
pub struct UnknownTable {
    table_name: String,
    context: String,
}

impl UnknownTable {
    pub fn new(table_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[error("UnknownTableId: `{table_id}` while `{context}`")]
pub struct UnknownTableId {
    table_id: u64,
    context: String,
}

impl UnknownTableId {
    pub fn new(table_id: u64, context: impl Into<String>) -> UnknownTableId {
        Self {
            table_id,
            context: context.into(),
        }
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum AppError {
    #[error(transparent)]
    TableVersionMismatched(#[from] TableVersionMismatched),

    #[error(transparent)]
    TableAlreadyExists(#[from] TableAlreadyExists),

    #[error(transparent)]
    DatabaseAlreadyExists(#[from] DatabaseAlreadyExists),

    #[error(transparent)]
    UnknownDatabase(#[from] UnknownDatabase),

    #[error(transparent)]
    UnknownDatabaseId(#[from] UnknownDatabaseId),

    #[error(transparent)]
    UnknownTable(#[from] UnknownTable),

    #[error(transparent)]
    UnknownTableId(#[from] UnknownTableId),
}

impl AppErrorMessage for UnknownDatabase {
    fn message(&self) -> String {
        self.db_name.to_string()
    }
}
impl AppErrorMessage for UnknownTable {
    fn message(&self) -> String {
        format!("Unknown table: '{}'", self.table_name)
    }
}

impl AppErrorMessage for UnknownTableId {}
impl AppErrorMessage for UnknownDatabaseId {}
impl AppErrorMessage for DatabaseAlreadyExists {}
impl AppErrorMessage for TableVersionMismatched {}

impl AppErrorMessage for TableAlreadyExists {
    fn message(&self) -> String {
        format!("table exists: {}", self.table_name)
    }
}

impl From<AppError> for ErrorCode {
    fn from(app_err: AppError) -> Self {
        match app_err {
            AppError::UnknownDatabase(err) => ErrorCode::UnknownDatabase(err.message()),
            AppError::UnknownDatabaseId(err) => ErrorCode::UnknownDatabaseId(err.message()),
            AppError::UnknownTableId(err) => ErrorCode::UnknownTableId(err.message()),
            AppError::UnknownTable(err) => ErrorCode::UnknownTable(err.message()),
            AppError::DatabaseAlreadyExists(err) => ErrorCode::DatabaseAlreadyExists(err.message()),
            AppError::TableAlreadyExists(err) => ErrorCode::TableAlreadyExists(err.message()),
            AppError::TableVersionMismatched(err) => {
                ErrorCode::TableVersionMismatched(err.message())
            }
        }
    }
}

pub type MetaStorageResult<T> = std::result::Result<T, MetaStorageError>;

impl From<MetaStorageError> for ErrorCode {
    fn from(e: MetaStorageError) -> Self {
        match e {
            MetaStorageError::AppError(app_err) => app_err.into(),
            _ => ErrorCode::MetaStorageError(e.to_string()),
        }
    }
}

impl From<std::string::FromUtf8Error> for MetaStorageError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        MetaStorageError::BytesError(format!(
            "Bad bytes, cannot parse bytes with UTF8, cause: {}",
            error
        ))
    }
}

// from serde error to MetaStorageError::SerdeError
impl From<serde_json::Error> for MetaStorageError {
    fn from(error: serde_json::Error) -> MetaStorageError {
        MetaStorageError::SerdeError(format!("serde json se/de error: {:?}", error))
    }
}

impl From<sled::Error> for MetaStorageError {
    fn from(e: sled::Error) -> MetaStorageError {
        MetaStorageError::SledError(AnyError::new(&e))
    }
}

impl From<ErrorWithContext<sled::Error>> for MetaStorageError {
    fn from(e: ErrorWithContext<sled::Error>) -> MetaStorageError {
        MetaStorageError::SledError(AnyError::new(&e.err).add_context(|| e.context))
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

impl From<UnabortableTransactionError> for MetaStorageError {
    fn from(error: UnabortableTransactionError) -> Self {
        match error {
            UnabortableTransactionError::Storage(e) => {
                MetaStorageError::SledError(AnyError::new(&e))
            }
            UnabortableTransactionError::Conflict => MetaStorageError::TransactionConflict,
        }
    }
}
