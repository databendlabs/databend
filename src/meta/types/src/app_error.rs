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
use serde::Deserialize;
use serde::Serialize;

use crate::MatchSeq;

/// Output message for end users, with sensitive info stripped.
pub trait AppErrorMessage: Display {
    fn message(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("CreateDatabaseWithDropTime: `{db_name}` with drop_on")]
pub struct CreateDatabaseWithDropTime {
    db_name: String,
}

impl CreateDatabaseWithDropTime {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("DropDbWithDropTime: drop {db_name} with drop_on time")]
pub struct DropDbWithDropTime {
    db_name: String,
}

impl DropDbWithDropTime {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("UndropDbWithNoDropTime: undrop {db_name} with no drop_on time")]
pub struct UndropDbWithNoDropTime {
    db_name: String,
}

impl UndropDbWithNoDropTime {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("UndropDbHasNoHistory: undrop {db_name} has no db id history")]
pub struct UndropDbHasNoHistory {
    db_name: String,
}

impl UndropDbHasNoHistory {
    pub fn new(db_name: impl Into<String>) -> Self {
        Self {
            db_name: db_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("CreateTableWithDropTime: create {table_name} with drop time")]
pub struct CreateTableWithDropTime {
    table_name: String,
}

impl CreateTableWithDropTime {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("UndropTableAlreadyExists: undrop {table_name} already exists")]
pub struct UndropTableAlreadyExists {
    table_name: String,
}

impl UndropTableAlreadyExists {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("UndropTableWithNoDropTime: undrop {table_name} with no drop_on time")]
pub struct UndropTableWithNoDropTime {
    table_name: String,
}

impl UndropTableWithNoDropTime {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("DropTableWithDropTime: drop {table_name} with drop_on time")]
pub struct DropTableWithDropTime {
    table_name: String,
}

impl DropTableWithDropTime {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("UndropTableHasNoHistory: undrop {table_name} has no table id history")]
pub struct UndropTableHasNoHistory {
    table_name: String,
}

impl UndropTableHasNoHistory {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
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

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("ShareAlreadyExists: {share_name} while {context}")]
pub struct ShareAlreadyExists {
    share_name: String,
    context: String,
}

impl ShareAlreadyExists {
    pub fn new(share_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            share_name: share_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("ShareAccountsAlreadyExists: {share_name} while {context}")]
pub struct ShareAccountsAlreadyExists {
    share_name: String,
    accounts: Vec<String>,
    context: String,
}

impl ShareAccountsAlreadyExists {
    pub fn new(
        share_name: impl Into<String>,
        accounts: &[String],
        context: impl Into<String>,
    ) -> Self {
        Self {
            share_name: share_name.into(),
            accounts: accounts.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("UnknownShareAccounts: {share_id} while {context}")]
pub struct UnknownShareAccounts {
    accounts: Vec<String>,
    share_id: u64,
    context: String,
}

impl UnknownShareAccounts {
    pub fn new(accounts: &[String], share_id: u64, context: impl Into<String>) -> Self {
        Self {
            accounts: accounts.into(),
            share_id,
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("WrongShareObject: {obj_name} does not belong to the database that is being shared")]
pub struct WrongShareObject {
    obj_name: String,
}

impl WrongShareObject {
    pub fn new(obj_name: impl Into<String>) -> Self {
        Self {
            obj_name: obj_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("ShareHasNoGrantedDatabase: {tenant}.{share_name} has no granted database")]
pub struct ShareHasNoGrantedDatabase {
    pub tenant: String,
    pub share_name: String,
}

impl ShareHasNoGrantedDatabase {
    pub fn new(tenant: impl Into<String>, share_name: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            share_name: share_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("ShareHasNoGrantedPrivilege: {tenant}.{share_name} has no proper granted privilege")]
pub struct ShareHasNoGrantedPrivilege {
    pub tenant: String,
    pub share_name: String,
}

impl ShareHasNoGrantedPrivilege {
    pub fn new(tenant: impl Into<String>, share_name: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            share_name: share_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("WrongShare: {share_name} has the wrong format")]
pub struct WrongShare {
    share_name: String,
}

impl WrongShare {
    pub fn new(share_name: impl Into<String>) -> Self {
        Self {
            share_name: share_name.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("UnknownShare: {share_name} while {context}")]
pub struct UnknownShare {
    share_name: String,
    context: String,
}

impl UnknownShare {
    pub fn new(share_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            share_name: share_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("UnknownShareID: {share_id} while {context}")]
pub struct UnknownShareId {
    share_id: u64,
    context: String,
}

impl UnknownShareId {
    pub fn new(share_id: u64, context: impl Into<String>) -> Self {
        Self {
            share_id,
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("TxnRetryMaxTimes: Txn {op} has retry {max_retry} times, abort.")]
pub struct TxnRetryMaxTimes {
    op: String,
    max_retry: u32,
}

impl TxnRetryMaxTimes {
    pub fn new(op: &str, max_retry: u32) -> Self {
        Self {
            op: op.to_string(),
            max_retry,
        }
    }
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AppError {
    #[error(transparent)]
    TableVersionMismatched(#[from] TableVersionMismatched),

    #[error(transparent)]
    TableAlreadyExists(#[from] TableAlreadyExists),

    #[error(transparent)]
    CreateTableWithDropTime(#[from] CreateTableWithDropTime),

    #[error(transparent)]
    UndropTableAlreadyExists(#[from] UndropTableAlreadyExists),

    #[error(transparent)]
    UndropTableWithNoDropTime(#[from] UndropTableWithNoDropTime),

    #[error(transparent)]
    DropTableWithDropTime(#[from] DropTableWithDropTime),

    #[error(transparent)]
    UndropTableHasNoHistory(#[from] UndropTableHasNoHistory),

    #[error(transparent)]
    DatabaseAlreadyExists(#[from] DatabaseAlreadyExists),

    #[error(transparent)]
    CreateDatabaseWithDropTime(#[from] CreateDatabaseWithDropTime),

    #[error(transparent)]
    DropDbWithDropTime(#[from] DropDbWithDropTime),

    #[error(transparent)]
    UndropDbWithNoDropTime(#[from] UndropDbWithNoDropTime),

    #[error(transparent)]
    UndropDbHasNoHistory(#[from] UndropDbHasNoHistory),

    #[error(transparent)]
    UnknownDatabase(#[from] UnknownDatabase),

    #[error(transparent)]
    UnknownDatabaseId(#[from] UnknownDatabaseId),

    #[error(transparent)]
    UnknownTable(#[from] UnknownTable),

    #[error(transparent)]
    UnknownTableId(#[from] UnknownTableId),

    #[error(transparent)]
    TxnRetryMaxTimes(#[from] TxnRetryMaxTimes),

    // share api errors
    #[error(transparent)]
    ShareAlreadyExists(#[from] ShareAlreadyExists),

    #[error(transparent)]
    UnknownShare(#[from] UnknownShare),

    #[error(transparent)]
    UnknownShareId(#[from] UnknownShareId),

    #[error(transparent)]
    ShareAccountsAlreadyExists(#[from] ShareAccountsAlreadyExists),

    #[error(transparent)]
    UnknownShareAccounts(#[from] UnknownShareAccounts),

    #[error(transparent)]
    WrongShareObject(#[from] WrongShareObject),

    #[error(transparent)]
    ShareHasNoGrantedDatabase(#[from] ShareHasNoGrantedDatabase),

    #[error(transparent)]
    ShareHasNoGrantedPrivilege(#[from] ShareHasNoGrantedPrivilege),

    #[error(transparent)]
    WrongShare(#[from] WrongShare),
}

impl AppErrorMessage for UnknownDatabase {
    fn message(&self) -> String {
        format!("Unknown database '{}'", self.db_name)
    }
}

impl AppErrorMessage for DatabaseAlreadyExists {
    fn message(&self) -> String {
        format!("Database '{}' already exists", self.db_name)
    }
}

impl AppErrorMessage for CreateDatabaseWithDropTime {
    fn message(&self) -> String {
        format!("Create database '{}' with drop time", self.db_name)
    }
}

impl AppErrorMessage for UndropDbHasNoHistory {
    fn message(&self) -> String {
        format!("Undrop database '{}' has no id history", self.db_name)
    }
}

impl AppErrorMessage for UnknownTable {
    fn message(&self) -> String {
        format!("Unknown table '{}'", self.table_name)
    }
}

impl AppErrorMessage for UnknownTableId {}

impl AppErrorMessage for UnknownDatabaseId {}

impl AppErrorMessage for TableVersionMismatched {}

impl AppErrorMessage for TableAlreadyExists {
    fn message(&self) -> String {
        format!("Table '{}' already exists", self.table_name)
    }
}

impl AppErrorMessage for CreateTableWithDropTime {
    fn message(&self) -> String {
        format!("Create Table '{}' with drop time", self.table_name)
    }
}

impl AppErrorMessage for UndropTableAlreadyExists {
    fn message(&self) -> String {
        format!("Undrop Table '{}' already exists", self.table_name)
    }
}

impl AppErrorMessage for UndropTableHasNoHistory {
    fn message(&self) -> String {
        format!("Undrop Table '{}' has no table id list", self.table_name)
    }
}

impl AppErrorMessage for ShareAlreadyExists {
    fn message(&self) -> String {
        format!("Share '{}' already exists", self.share_name)
    }
}

impl AppErrorMessage for UnknownShare {
    fn message(&self) -> String {
        format!("Unknown share '{}'", self.share_name)
    }
}

impl AppErrorMessage for UnknownShareId {
    fn message(&self) -> String {
        format!("Unknown share id '{}'", self.share_id)
    }
}

impl AppErrorMessage for ShareAccountsAlreadyExists {
    fn message(&self) -> String {
        format!(
            "Share accounts for ({},{:?}) already exists",
            self.share_name, self.accounts
        )
    }
}

impl AppErrorMessage for UnknownShareAccounts {
    fn message(&self) -> String {
        format!(
            "Unknown share account for ({:?},{})",
            self.accounts, self.share_id
        )
    }
}

impl AppErrorMessage for WrongShareObject {
    fn message(&self) -> String {
        format!(
            " {} does not belong to the database that is being shared",
            self.obj_name
        )
    }
}

impl AppErrorMessage for ShareHasNoGrantedDatabase {
    fn message(&self) -> String {
        format!(
            "share {}.{} has no granted database",
            self.tenant, self.share_name
        )
    }
}

impl AppErrorMessage for ShareHasNoGrantedPrivilege {
    fn message(&self) -> String {
        format!(
            "share {}.{} has no proper granted privilege",
            self.tenant, self.share_name
        )
    }
}

impl AppErrorMessage for WrongShare {
    fn message(&self) -> String {
        format!("share {} has the wrong format", self.share_name)
    }
}

impl AppErrorMessage for TxnRetryMaxTimes {
    fn message(&self) -> String {
        format!(
            "TxnRetryMaxTimes: Txn {} has retry {} times",
            self.op, self.max_retry
        )
    }
}

impl AppErrorMessage for UndropTableWithNoDropTime {
    fn message(&self) -> String {
        format!("Undrop table '{}' with no drop_on time", self.table_name)
    }
}

impl AppErrorMessage for DropTableWithDropTime {
    fn message(&self) -> String {
        format!("Drop table '{}' with drop_on time", self.table_name)
    }
}

impl AppErrorMessage for UndropDbWithNoDropTime {
    fn message(&self) -> String {
        format!("Undrop db '{}' with no drop_on time", self.db_name)
    }
}

impl AppErrorMessage for DropDbWithDropTime {
    fn message(&self) -> String {
        format!("Drop db '{}' with drop_on time", self.db_name)
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
            AppError::CreateDatabaseWithDropTime(err) => {
                ErrorCode::CreateDatabaseWithDropTime(err.message())
            }
            AppError::UndropDbHasNoHistory(err) => ErrorCode::UndropDbHasNoHistory(err.message()),
            AppError::UndropTableWithNoDropTime(err) => {
                ErrorCode::UndropTableWithNoDropTime(err.message())
            }
            AppError::DropTableWithDropTime(err) => ErrorCode::DropTableWithDropTime(err.message()),
            AppError::DropDbWithDropTime(err) => ErrorCode::DropDbWithDropTime(err.message()),
            AppError::UndropDbWithNoDropTime(err) => {
                ErrorCode::UndropDbWithNoDropTime(err.message())
            }
            AppError::TableAlreadyExists(err) => ErrorCode::TableAlreadyExists(err.message()),
            AppError::CreateTableWithDropTime(err) => {
                ErrorCode::CreateTableWithDropTime(err.message())
            }
            AppError::UndropTableAlreadyExists(err) => {
                ErrorCode::UndropTableAlreadyExists(err.message())
            }
            AppError::UndropTableHasNoHistory(err) => {
                ErrorCode::UndropTableHasNoHistory(err.message())
            }
            AppError::TableVersionMismatched(err) => {
                ErrorCode::TableVersionMismatched(err.message())
            }
            AppError::ShareAlreadyExists(err) => ErrorCode::ShareAlreadyExists(err.message()),
            AppError::UnknownShare(err) => ErrorCode::UnknownShare(err.message()),
            AppError::UnknownShareId(err) => ErrorCode::UnknownShareId(err.message()),
            AppError::ShareAccountsAlreadyExists(err) => {
                ErrorCode::ShareAccountsAlreadyExists(err.message())
            }
            AppError::UnknownShareAccounts(err) => ErrorCode::UnknownShareAccounts(err.message()),
            AppError::WrongShareObject(err) => ErrorCode::WrongShareObject(err.message()),
            AppError::ShareHasNoGrantedDatabase(err) => {
                ErrorCode::ShareHasNoGrantedDatabase(err.message())
            }
            AppError::ShareHasNoGrantedPrivilege(err) => {
                ErrorCode::ShareHasNoGrantedPrivilege(err.message())
            }
            AppError::WrongShare(err) => ErrorCode::WrongShare(err.message()),
            AppError::TxnRetryMaxTimes(err) => ErrorCode::TxnRetryMaxTimes(err.message()),
        }
    }
}
