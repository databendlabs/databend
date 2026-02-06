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

use std::fmt::Display;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_meta_types::MatchSeq;

use crate::data_mask::data_mask_name_ident;
use crate::principal::ProcedureIdentity;
use crate::principal::procedure_name_ident;
use crate::row_access_policy::row_access_policy_name_ident;
use crate::schema::DictionaryIdentity;
use crate::schema::SequenceRsc;
use crate::schema::catalog_name_ident;
use crate::schema::dictionary_name_ident;
use crate::schema::index_name_ident;
use crate::tenant_key::errors::ExistError;
use crate::tenant_key::errors::UnknownError;
use crate::tenant_key::ident::TIdent;

/// Output message for end users, with sensitive info stripped.
pub trait AppErrorMessage: Display {
    fn message(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Tenant is empty when: `{context}`")]
pub struct TenantIsEmpty {
    context: String,
}

impl TenantIsEmpty {
    pub fn new(context: impl ToString) -> Self {
        Self {
            context: context.to_string(),
        }
    }
}

impl From<TenantIsEmpty> for ErrorCode {
    fn from(err: TenantIsEmpty) -> Self {
        ErrorCode::TenantIsEmpty(err.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("CreateDatabaseWithDropTime: `{db_name}` with drop_on")]
pub struct CreateDatabaseWithDropTime {
    db_name: String,
}

impl CreateDatabaseWithDropTime {
    pub fn new(db_name: impl ToString) -> Self {
        Self {
            db_name: db_name.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("CommitTableMetaError: {table_name} while {context}")]
pub struct CommitTableMetaError {
    table_name: String,
    context: String,
}

impl CommitTableMetaError {
    pub fn new(table_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("ViewAlreadyExists: {view_name} while {context}")]
pub struct ViewAlreadyExists {
    view_name: String,
    context: String,
}

impl ViewAlreadyExists {
    pub fn new(view_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            view_name: view_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("CreateAsDropTableWithoutDropTime: create as_drop {table_name} without drop time")]
pub struct CreateAsDropTableWithoutDropTime {
    table_name: String,
}

impl CreateAsDropTableWithoutDropTime {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "Cannot undrop table '{table_name}': table was dropped at {drop_time} before vacuum started at {retention}. Data may have been cleaned up."
)]
pub struct UndropTableRetentionGuard {
    table_name: String,
    drop_time: DateTime<Utc>,
    retention: DateTime<Utc>,
}

impl UndropTableRetentionGuard {
    pub fn new(
        table_name: impl Into<String>,
        drop_time: DateTime<Utc>,
        retention: DateTime<Utc>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            drop_time,
            retention,
        }
    }

    pub fn drop_time(&self) -> DateTime<Utc> {
        self.drop_time
    }

    pub fn retention(&self) -> DateTime<Utc> {
        self.retention
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("DatabaseVersionMismatched: {db_id} expect `{expect}` but `{curr:?}`  while `{context}`")]
pub struct DatabaseVersionMismatched {
    db_id: u64,
    expect: MatchSeq,
    curr: Option<u64>,
    context: String,
}

impl DatabaseVersionMismatched {
    pub fn new(
        db_id: u64,
        expect: MatchSeq,
        curr: Option<u64>,
        context: impl Into<String>,
    ) -> Self {
        Self {
            db_id,
            expect,
            curr,
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("StreamAlreadyExists: {name} while {context}")]
pub struct StreamAlreadyExists {
    name: String,
    context: String,
}

impl StreamAlreadyExists {
    pub fn new(name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("StreamVersionMismatched: {stream_id} expect `{expect}` but `{curr}`  while `{context}`")]
pub struct StreamVersionMismatched {
    stream_id: u64,
    expect: MatchSeq,
    curr: u64,
    context: String,
}

impl StreamVersionMismatched {
    pub fn new(stream_id: u64, expect: MatchSeq, curr: u64, context: impl Into<String>) -> Self {
        Self {
            stream_id,
            expect,
            curr,
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("UnknownStreamId: `{stream_id}` while `{context}`")]
pub struct UnknownStreamId {
    stream_id: u64,
    context: String,
}

impl UnknownStreamId {
    pub fn new(stream_id: u64, context: impl Into<String>) -> UnknownStreamId {
        Self {
            stream_id,
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("MultiStmtTxnCommitFailed: {context}")]
pub struct MultiStmtTxnCommitFailed {
    context: String,
}

impl MultiStmtTxnCommitFailed {
    pub fn new(context: impl Into<String>) -> MultiStmtTxnCommitFailed {
        Self {
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("UpdateStreamMetasFailed: {message}")]
pub struct UpdateStreamMetasFailed {
    message: String,
}

impl UpdateStreamMetasFailed {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("MarkDatabaseMetaAsGCInProgressFailed : {message}")]
pub struct MarkDatabaseMetaAsGCInProgressFailed {
    message: String,
}

impl MarkDatabaseMetaAsGCInProgressFailed {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("CleanDbIdTableNamesFailed : {message}")]
pub struct CleanDbIdTableNamesFailed {
    message: String,
}

impl CleanDbIdTableNamesFailed {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("DuplicatedUpsertFiles: {table_id:?} , in operation `{context}`")]
pub struct DuplicatedUpsertFiles {
    table_id: Vec<u64>,
    context: String,
}

impl DuplicatedUpsertFiles {
    pub fn new(table_id: Vec<u64>, context: impl Into<String>) -> Self {
        DuplicatedUpsertFiles {
            table_id,
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
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

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("UnknownCatalog: `{catalog_name}` while `{context}`")]
pub struct UnknownCatalog {
    catalog_name: String,
    context: String,
}

impl UnknownCatalog {
    pub fn new(catalog_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            catalog_name: catalog_name.into(),
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("UnknownDatamask: `{name}` while `{context}`")]
pub struct UnknownDatamask {
    name: String,
    context: String,
}

impl UnknownDatamask {
    pub fn new(name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("UnknownDatabaseId: `{db_id}` while `{context}`")]
pub struct UnknownDatabaseId {
    db_id: u64,
    context: String,
}

impl UnknownDatabaseId {
    pub fn new(db_id: u64, context: impl Into<String>) -> UnknownDatabaseId {
        Self {
            db_id,
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("UnmatchColumnDataType: `{name}`:`{data_type}` while `{context}`")]
pub struct UnmatchColumnDataType {
    name: String,
    data_type: String,
    context: String,
}

impl UnmatchColumnDataType {
    pub fn new(
        name: impl Into<String>,
        data_type: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error(
    "UnmatchMaskPolicyReturnType: `{arg_name}`:`{arg_type}` mismatch with return type `{return_type}` while `{context}`"
)]
pub struct UnmatchMaskPolicyReturnType {
    arg_name: String,
    arg_type: String,
    return_type: String,
    context: String,
}

impl UnmatchMaskPolicyReturnType {
    pub fn new(
        arg_name: impl Into<String>,
        arg_type: impl Into<String>,
        return_type: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        Self {
            arg_name: arg_name.into(),
            arg_type: arg_type.into(),
            return_type: return_type.into(),
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
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

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("TableSnapshotExpired: {table_id} while {context}")]
pub struct TableSnapshotExpired {
    table_id: u64,
    context: String,
}

impl TableSnapshotExpired {
    pub fn new(table_id: u64, context: impl Into<String>) -> Self {
        Self {
            table_id,
            context: context.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error(
    "VirtualColumnIdOutBound: the virtual column id `{column_id}` is outside the range `{lower}` to `{upper}`"
)]
pub struct VirtualColumnIdOutBound {
    column_id: u32,
    lower: u32,
    upper: u32,
}

impl VirtualColumnIdOutBound {
    pub fn new(column_id: u32, lower: u32, upper: u32) -> Self {
        Self {
            column_id,
            lower,
            upper,
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("VirtualColumnTooMany: the number of virtual columns in `{table_id}` exceeds `{limit}`")]
pub struct VirtualColumnTooMany {
    table_id: u64,
    limit: usize,
}

impl VirtualColumnTooMany {
    pub fn new(table_id: u64, limit: usize) -> VirtualColumnTooMany {
        Self { table_id, limit }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("ShareEndpointAlreadyExists: {endpoint} while {context}")]
pub struct ShareEndpointAlreadyExists {
    endpoint: String,
    context: String,
}

impl ShareEndpointAlreadyExists {
    pub fn new(endpoint: impl ToString, context: impl ToString) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            context: context.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("WrongShareObject: {obj_name} does not belong to the database that is being shared")]
pub struct WrongShareObject {
    obj_name: String,
}

impl WrongShareObject {
    pub fn new(obj_name: impl ToString) -> Self {
        Self {
            obj_name: obj_name.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("WrongSharePrivileges: wrong share privileges of {obj_name}")]
pub struct WrongSharePrivileges {
    obj_name: String,
}

impl WrongSharePrivileges {
    pub fn new(obj_name: impl ToString) -> Self {
        Self {
            obj_name: obj_name.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "CannotAccessShareTable: cannot access share table {table_name} from {tenant}.{share_name}"
)]
pub struct CannotAccessShareTable {
    pub tenant: String,
    pub share_name: String,
    pub table_name: String,
}

impl CannotAccessShareTable {
    pub fn new(
        tenant: impl Into<String>,
        share_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            share_name: share_name.into(),
            table_name: table_name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("UnknownShareEndpoint: {endpoint} while {context}")]
pub struct UnknownShareEndpoint {
    endpoint: String,
    context: String,
}

impl UnknownShareEndpoint {
    pub fn new(endpoint: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("UnknownShareEndpointId: {share_endpoint_id} while {context}")]
pub struct UnknownShareEndpointId {
    share_endpoint_id: u64,
    context: String,
}

impl UnknownShareEndpointId {
    pub fn new(share_endpoint_id: u64, context: impl Into<String>) -> Self {
        Self {
            share_endpoint_id,
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("TableLockExpired: `{table_id}` while `{context}`")]
pub struct TableLockExpired {
    table_id: u64,
    context: String,
}

impl TableLockExpired {
    pub fn new(table_id: u64, context: impl Into<String>) -> Self {
        Self {
            table_id,
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "CannotShareDatabaseCreatedFromShare: cannot share database {database_name} which created from share while {context}"
)]
pub struct CannotShareDatabaseCreatedFromShare {
    database_name: String,
    context: String,
}

impl CannotShareDatabaseCreatedFromShare {
    pub fn new(database_name: impl Into<String>, context: impl Into<String>) -> Self {
        Self {
            database_name: database_name.into(),
            context: context.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("CreateIndexWithDropTime: create {index_name} with drop time")]
pub struct CreateIndexWithDropTime {
    index_name: String,
}

impl CreateIndexWithDropTime {
    pub fn new(index_name: impl Into<String>) -> Self {
        Self {
            index_name: index_name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("DropIndexWithDropTime: drop {index_name} with drop time")]
pub struct DropIndexWithDropTime {
    index_name: String,
}

impl DropIndexWithDropTime {
    pub fn new(index_name: impl Into<String>) -> Self {
        Self {
            index_name: index_name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("DuplicatedIndexColumnId: {column_id} is duplicated with index {index_name}")]
pub struct DuplicatedIndexColumnId {
    column_id: u32,
    index_name: String,
}

impl DuplicatedIndexColumnId {
    pub fn new(column_id: u32, index_name: impl Into<String>) -> Self {
        Self {
            column_id,
            index_name: index_name.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("IndexColumnIdNotFound: index {index_name} column id {column_id} is not found")]
pub struct IndexColumnIdNotFound {
    column_id: u32,
    index_name: String,
}

impl IndexColumnIdNotFound {
    pub fn new(column_id: u32, index_name: impl Into<String>) -> Self {
        Self {
            column_id,
            index_name: index_name.into(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("OutOfSequenceRange: `{name}` while `{context}`")]
pub struct OutOfSequenceRange {
    name: String,
    context: String,
}

impl OutOfSequenceRange {
    pub fn new(name: impl ToString, context: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            context: context.to_string(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("WrongSequenceCount: `{name}`")]
pub struct WrongSequenceCount {
    name: String,
}

impl WrongSequenceCount {
    pub fn new(name: impl ToString) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("UnsupportedSequenceStorageVersion: `{version}`")]
pub struct UnsupportedSequenceStorageVersion {
    version: u64,
}

impl UnsupportedSequenceStorageVersion {
    pub fn new(version: u64) -> Self {
        Self { version }
    }
}

/// Application error.
///
/// The application does not get expected result but there is nothing wrong with meta-service.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum AppError {
    #[error(transparent)]
    TenantIsEmpty(#[from] TenantIsEmpty),

    #[error(transparent)]
    TableVersionMismatched(#[from] TableVersionMismatched),

    #[error(transparent)]
    DatabaseVersionMismatched(#[from] DatabaseVersionMismatched),

    #[error(transparent)]
    DuplicatedUpsertFiles(#[from] DuplicatedUpsertFiles),

    #[error(transparent)]
    CommitTableMetaError(#[from] CommitTableMetaError),

    #[error(transparent)]
    TableAlreadyExists(#[from] TableAlreadyExists),

    #[error(transparent)]
    ViewAlreadyExists(#[from] ViewAlreadyExists),

    #[error(transparent)]
    CreateTableWithDropTime(#[from] CreateTableWithDropTime),

    #[error(transparent)]
    CreateAsDropTableWithoutDropTime(#[from] CreateAsDropTableWithoutDropTime),

    #[error(transparent)]
    UndropTableAlreadyExists(#[from] UndropTableAlreadyExists),

    #[error(transparent)]
    UndropTableWithNoDropTime(#[from] UndropTableWithNoDropTime),

    #[error(transparent)]
    DropTableWithDropTime(#[from] DropTableWithDropTime),

    #[error(transparent)]
    UndropTableHasNoHistory(#[from] UndropTableHasNoHistory),

    #[error(transparent)]
    UndropTableRetentionGuard(#[from] UndropTableRetentionGuard),

    #[error(transparent)]
    DatabaseAlreadyExists(#[from] DatabaseAlreadyExists),

    #[error(transparent)]
    CatalogAlreadyExists(#[from] ExistError<catalog_name_ident::CatalogNameRsc>),

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
    UnknownCatalog(#[from] UnknownError<catalog_name_ident::CatalogNameRsc>),

    #[error(transparent)]
    UnknownDatabaseId(#[from] UnknownDatabaseId),

    #[error(transparent)]
    UnknownTable(#[from] UnknownTable),

    #[error(transparent)]
    UnknownTableId(#[from] UnknownTableId),

    #[error(transparent)]
    TableSnapshotExpired(#[from] TableSnapshotExpired),

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
    WrongSharePrivileges(#[from] WrongSharePrivileges),

    #[error(transparent)]
    ShareHasNoGrantedDatabase(#[from] ShareHasNoGrantedDatabase),

    #[error(transparent)]
    ShareHasNoGrantedPrivilege(#[from] ShareHasNoGrantedPrivilege),

    #[error(transparent)]
    CannotAccessShareTable(#[from] CannotAccessShareTable),

    #[error(transparent)]
    WrongShare(#[from] WrongShare),

    #[error(transparent)]
    ShareEndpointAlreadyExists(#[from] ShareEndpointAlreadyExists),

    #[error(transparent)]
    UnknownShareEndpoint(#[from] UnknownShareEndpoint),

    #[error(transparent)]
    UnknownShareEndpointId(#[from] UnknownShareEndpointId),

    #[error(transparent)]
    TableLockExpired(#[from] TableLockExpired),

    #[error(transparent)]
    CannotShareDatabaseCreatedFromShare(#[from] CannotShareDatabaseCreatedFromShare),

    #[error(transparent)]
    CreateIndexWithDropTime(#[from] CreateIndexWithDropTime),

    #[error(transparent)]
    IndexAlreadyExists(#[from] ExistError<index_name_ident::IndexName>),

    #[error(transparent)]
    UnknownIndex(#[from] UnknownError<index_name_ident::IndexName>),

    #[error(transparent)]
    DropIndexWithDropTime(#[from] DropIndexWithDropTime),

    #[error(transparent)]
    DuplicatedIndexColumnId(#[from] DuplicatedIndexColumnId),

    #[error(transparent)]
    IndexColumnIdNotFound(#[from] IndexColumnIdNotFound),

    #[error(transparent)]
    DatamaskAlreadyExists(#[from] ExistError<data_mask_name_ident::Resource>),

    #[error(transparent)]
    UnknownDataMask(#[from] UnknownError<data_mask_name_ident::Resource>),

    #[error(transparent)]
    UnknownRowAccessPolicy(#[from] UnknownError<row_access_policy_name_ident::Resource>),

    #[error(transparent)]
    UnmatchColumnDataType(#[from] UnmatchColumnDataType),

    #[error(transparent)]
    UnmatchMaskPolicyReturnType(#[from] UnmatchMaskPolicyReturnType),

    #[error(transparent)]
    VirtualColumnIdOutBound(#[from] VirtualColumnIdOutBound),

    #[error(transparent)]
    VirtualColumnTooMany(#[from] VirtualColumnTooMany),

    #[error(transparent)]
    StreamAlreadyExists(#[from] StreamAlreadyExists),

    #[error(transparent)]
    StreamVersionMismatched(#[from] StreamVersionMismatched),

    #[error(transparent)]
    UnknownStreamId(#[from] UnknownStreamId),

    #[error(transparent)]
    MultiStatementTxnCommitFailed(#[from] MultiStmtTxnCommitFailed),

    // sequence
    #[error(transparent)]
    SequenceError(#[from] SequenceError),

    #[error(transparent)]
    UpdateStreamMetasFailed(#[from] UpdateStreamMetasFailed),

    #[error(transparent)]
    MarkDatabaseMetaAsGCInProgressFailed(#[from] MarkDatabaseMetaAsGCInProgressFailed),

    #[error(transparent)]
    CleanDbIdTableNamesFailed(#[from] CleanDbIdTableNamesFailed),

    // dictionary
    #[error(transparent)]
    DictionaryAlreadyExists(
        #[from] ExistError<dictionary_name_ident::DictionaryNameRsc, DictionaryIdentity>,
    ),

    #[error(transparent)]
    UnknownDictionary(
        #[from] UnknownError<dictionary_name_ident::DictionaryNameRsc, DictionaryIdentity>,
    ),

    // Procedure
    #[error(transparent)]
    ProcedureAlreadyExists(
        #[from] ExistError<procedure_name_ident::ProcedureName, ProcedureIdentity>,
    ),

    #[error(transparent)]
    UnknownProcedure(#[from] UnknownError<procedure_name_ident::ProcedureName, ProcedureIdentity>),
}

impl AppError {
    /// Create an `unknown` TIdent error.
    pub fn unknown<R, N>(ident: &TIdent<R, N>, ctx: impl Display) -> AppError
    where
        N: Clone,
        AppError: From<UnknownError<R, N>>,
    {
        AppError::from(ident.unknown_error(ctx))
    }

    /// Create an `exist` TIdent error.
    pub fn exists<R, N>(ident: &TIdent<R, N>, ctx: impl Display) -> AppError
    where
        N: Clone,
        AppError: From<ExistError<R, N>>,
    {
        AppError::from(ident.exist_error(ctx))
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum SequenceError {
    #[error(transparent)]
    SequenceAlreadyExists(#[from] ExistError<SequenceRsc>),

    #[error(transparent)]
    UnknownSequence(#[from] UnknownError<SequenceRsc>),

    #[error(transparent)]
    OutOfSequenceRange(#[from] OutOfSequenceRange),

    #[error(transparent)]
    WrongSequenceCount(#[from] WrongSequenceCount),

    #[error(transparent)]
    UnsupportedSequenceStorageVersion(#[from] UnsupportedSequenceStorageVersion),
}

impl AppErrorMessage for TenantIsEmpty {
    fn message(&self) -> String {
        self.to_string()
    }
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

impl AppErrorMessage for TableSnapshotExpired {}

impl AppErrorMessage for UnknownDatabaseId {}

impl AppErrorMessage for TableVersionMismatched {}

impl AppErrorMessage for DatabaseVersionMismatched {}

impl AppErrorMessage for StreamAlreadyExists {
    fn message(&self) -> String {
        format!("'{}' as stream Already Exists", self.name)
    }
}

impl AppErrorMessage for StreamVersionMismatched {}

impl AppErrorMessage for UnknownStreamId {}

impl AppErrorMessage for MultiStmtTxnCommitFailed {}

impl AppErrorMessage for UpdateStreamMetasFailed {}

impl AppErrorMessage for MarkDatabaseMetaAsGCInProgressFailed {}

impl AppErrorMessage for CleanDbIdTableNamesFailed {}

impl AppErrorMessage for DuplicatedUpsertFiles {}

impl AppErrorMessage for CommitTableMetaError {
    fn message(&self) -> String {
        format!(
            "Create table '{}' failed, possibly because a table with the same name already exists. Context: {}",
            self.table_name, self.context
        )
    }
}

impl AppErrorMessage for TableAlreadyExists {
    fn message(&self) -> String {
        format!("Table '{}' already exists", self.table_name)
    }
}

impl AppErrorMessage for ViewAlreadyExists {
    fn message(&self) -> String {
        format!("'{}' as view Already Exists", self.view_name)
    }
}

impl AppErrorMessage for CreateTableWithDropTime {
    fn message(&self) -> String {
        format!("Create Table '{}' with drop time", self.table_name)
    }
}

impl AppErrorMessage for CreateAsDropTableWithoutDropTime {
    fn message(&self) -> String {
        format!(
            "Create as drop Table '{}' without drop time",
            self.table_name
        )
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

impl AppErrorMessage for WrongSharePrivileges {
    fn message(&self) -> String {
        format!("wrong share privileges of {}", self.obj_name)
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

impl AppErrorMessage for CannotAccessShareTable {
    fn message(&self) -> String {
        format!(
            "cannot access to share table {} from share {}.{}",
            self.table_name, self.tenant, self.share_name
        )
    }
}

impl AppErrorMessage for WrongShare {
    fn message(&self) -> String {
        format!("share {} has the wrong format", self.share_name)
    }
}

impl AppErrorMessage for ShareEndpointAlreadyExists {
    fn message(&self) -> String {
        format!("Share endpoint '{}' already exists", self.endpoint)
    }
}

impl AppErrorMessage for UnknownShareEndpoint {
    fn message(&self) -> String {
        format!("Unknown share endpoint '{}'", self.endpoint)
    }
}

impl AppErrorMessage for UnknownShareEndpointId {
    fn message(&self) -> String {
        format!("Unknown share endpoint id '{}'", self.share_endpoint_id)
    }
}

impl AppErrorMessage for TableLockExpired {
    fn message(&self) -> String {
        format!(
            "the acquired table lock in '{}' has been expired",
            self.table_id
        )
    }
}

impl AppErrorMessage for CannotShareDatabaseCreatedFromShare {
    fn message(&self) -> String {
        format!(
            "Cannot share database '{}' which created from share",
            self.database_name
        )
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

impl AppErrorMessage for UndropTableRetentionGuard {
    // Use default implementation that calls self.to_string()
    // since there's no sensitive information to strip
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

impl AppErrorMessage for CreateIndexWithDropTime {
    fn message(&self) -> String {
        format!("Create Index '{}' with drop time", self.index_name)
    }
}

impl AppErrorMessage for DropIndexWithDropTime {
    fn message(&self) -> String {
        format!("Drop Index '{}' with drop time", self.index_name)
    }
}

impl AppErrorMessage for DuplicatedIndexColumnId {
    fn message(&self) -> String {
        format!(
            "{} is duplicated with index '{}'",
            self.column_id, self.index_name
        )
    }
}

impl AppErrorMessage for IndexColumnIdNotFound {
    fn message(&self) -> String {
        format!(
            "index '{}' column id {} is not found",
            self.index_name, self.column_id
        )
    }
}

impl AppErrorMessage for UnmatchColumnDataType {
    fn message(&self) -> String {
        format!(
            "Column '{}' data type {} does not match",
            self.name, self.data_type
        )
    }
}

impl AppErrorMessage for UnmatchMaskPolicyReturnType {
    fn message(&self) -> String {
        format!(
            "'{}':'{}' mismatch with return type '{}'",
            self.arg_name, self.arg_type, self.return_type
        )
    }
}

impl AppErrorMessage for OutOfSequenceRange {
    fn message(&self) -> String {
        format!("Sequence '{}' out of range", self.name)
    }
}

impl AppErrorMessage for WrongSequenceCount {
    fn message(&self) -> String {
        format!("Require zero Sequence count for '{}'", self.name)
    }
}

impl AppErrorMessage for UnsupportedSequenceStorageVersion {
    fn message(&self) -> String {
        format!(
            "Sequence storage version({}) is not supported",
            self.version
        )
    }
}

impl AppErrorMessage for SequenceError {
    fn message(&self) -> String {
        match self {
            SequenceError::SequenceAlreadyExists(e) => {
                format!("SequenceAlreadyExists: '{}'", e.message())
            }
            SequenceError::UnknownSequence(e) => format!("UnknownSequence: '{}'", e.message()),
            SequenceError::OutOfSequenceRange(e) => {
                format!("OutOfSequenceRange: '{}'", e.message())
            }
            SequenceError::WrongSequenceCount(e) => {
                format!("WrongSequenceCount: '{}'", e.message())
            }
            SequenceError::UnsupportedSequenceStorageVersion(e) => {
                format!("UnsupportedSequenceStorageVersion: '{}'", e.message())
            }
        }
    }
}

impl AppErrorMessage for VirtualColumnIdOutBound {}

impl AppErrorMessage for VirtualColumnTooMany {}

impl From<AppError> for ErrorCode {
    fn from(app_err: AppError) -> Self {
        match app_err {
            AppError::TenantIsEmpty(err) => ErrorCode::TenantIsEmpty(err.message()),
            AppError::UnknownDatabase(err) => ErrorCode::UnknownDatabase(err.message()),
            AppError::UnknownDatabaseId(err) => ErrorCode::UnknownDatabaseId(err.message()),
            AppError::UnknownTableId(err) => ErrorCode::UnknownTableId(err.message()),
            AppError::TableSnapshotExpired(err) => ErrorCode::TableSnapshotExpired(err.message()),
            AppError::UnknownTable(err) => ErrorCode::UnknownTable(err.message()),
            AppError::UnknownCatalog(err) => ErrorCode::UnknownCatalog(err.message()),
            AppError::DatabaseAlreadyExists(err) => ErrorCode::DatabaseAlreadyExists(err.message()),
            AppError::CatalogAlreadyExists(err) => ErrorCode::CatalogAlreadyExists(err.message()),
            AppError::CreateDatabaseWithDropTime(err) => {
                ErrorCode::CreateDatabaseWithDropTime(err.message())
            }
            AppError::UndropDbHasNoHistory(err) => ErrorCode::UndropDbHasNoHistory(err.message()),
            AppError::UndropTableWithNoDropTime(err) => {
                ErrorCode::UndropTableWithNoDropTime(err.message())
            }
            AppError::UndropTableRetentionGuard(err) => {
                ErrorCode::UndropTableRetentionGuard(err.message())
            }
            AppError::DropTableWithDropTime(err) => ErrorCode::DropTableWithDropTime(err.message()),
            AppError::DropDbWithDropTime(err) => ErrorCode::DropDbWithDropTime(err.message()),
            AppError::UndropDbWithNoDropTime(err) => {
                ErrorCode::UndropDbWithNoDropTime(err.message())
            }
            AppError::CommitTableMetaError(err) => ErrorCode::CommitTableMetaError(err.message()),
            AppError::TableAlreadyExists(err) => ErrorCode::TableAlreadyExists(err.message()),
            AppError::ViewAlreadyExists(err) => ErrorCode::ViewAlreadyExists(err.message()),
            AppError::CreateTableWithDropTime(err) => {
                ErrorCode::CreateTableWithDropTime(err.message())
            }
            AppError::CreateAsDropTableWithoutDropTime(err) => {
                ErrorCode::CreateAsDropTableWithoutDropTime(err.message())
            }
            AppError::UndropTableAlreadyExists(err) => {
                ErrorCode::UndropTableAlreadyExists(err.message())
            }
            AppError::UndropTableHasNoHistory(err) => {
                ErrorCode::UndropTableHasNoHistory(err.message())
            }
            AppError::DatabaseVersionMismatched(err) => {
                ErrorCode::DatabaseVersionMismatched(err.message())
            }
            AppError::TableVersionMismatched(err) => {
                ErrorCode::TableVersionMismatched(err.message())
            }
            AppError::StreamAlreadyExists(err) => ErrorCode::StreamAlreadyExists(err.message()),
            AppError::StreamVersionMismatched(err) => {
                ErrorCode::StreamVersionMismatched(err.message())
            }
            AppError::UnknownStreamId(err) => ErrorCode::UnknownStreamId(err.message()),
            AppError::ShareAlreadyExists(err) => ErrorCode::ShareAlreadyExists(err.message()),
            AppError::UnknownShare(err) => ErrorCode::UnknownShare(err.message()),
            AppError::UnknownShareId(err) => ErrorCode::UnknownShareId(err.message()),
            AppError::ShareAccountsAlreadyExists(err) => {
                ErrorCode::ShareAccountsAlreadyExists(err.message())
            }
            AppError::UnknownShareAccounts(err) => ErrorCode::UnknownShareAccounts(err.message()),
            AppError::WrongShareObject(err) => ErrorCode::WrongShareObject(err.message()),
            AppError::WrongSharePrivileges(err) => ErrorCode::WrongSharePrivileges(err.message()),
            AppError::ShareHasNoGrantedDatabase(err) => {
                ErrorCode::ShareHasNoGrantedDatabase(err.message())
            }
            AppError::ShareHasNoGrantedPrivilege(err) => {
                ErrorCode::ShareHasNoGrantedPrivilege(err.message())
            }
            AppError::CannotAccessShareTable(err) => {
                ErrorCode::CannotAccessShareTable(err.message())
            }
            AppError::WrongShare(err) => ErrorCode::WrongShare(err.message()),
            AppError::ShareEndpointAlreadyExists(err) => {
                ErrorCode::ShareEndpointAlreadyExists(err.message())
            }
            AppError::UnknownShareEndpoint(err) => ErrorCode::UnknownShareEndpoint(err.message()),
            AppError::UnknownShareEndpointId(err) => {
                ErrorCode::UnknownShareEndpointId(err.message())
            }
            AppError::TableLockExpired(err) => ErrorCode::TableLockExpired(err.message()),
            AppError::CannotShareDatabaseCreatedFromShare(err) => {
                ErrorCode::CannotShareDatabaseCreatedFromShare(err.message())
            }
            AppError::TxnRetryMaxTimes(err) => ErrorCode::TxnRetryMaxTimes(err.message()),
            AppError::DuplicatedUpsertFiles(err) => ErrorCode::DuplicatedUpsertFiles(err.message()),
            AppError::CreateIndexWithDropTime(err) => {
                ErrorCode::CreateIndexWithDropTime(err.message())
            }
            AppError::IndexAlreadyExists(err) => ErrorCode::IndexAlreadyExists(err.message()),
            AppError::UnknownIndex(err) => ErrorCode::UnknownIndex(err.message()),
            AppError::DropIndexWithDropTime(err) => ErrorCode::DropIndexWithDropTime(err.message()),
            AppError::DuplicatedIndexColumnId(err) => {
                ErrorCode::DuplicatedIndexColumnId(err.message())
            }
            AppError::IndexColumnIdNotFound(err) => ErrorCode::IndexColumnIdNotFound(err.message()),

            AppError::DatamaskAlreadyExists(err) => ErrorCode::DatamaskAlreadyExists(err.message()),
            AppError::UnknownDataMask(err) => ErrorCode::UnknownDatamask(err.message()),
            AppError::UnknownRowAccessPolicy(err) => {
                ErrorCode::UnknownRowAccessPolicy(err.message())
            }

            AppError::UnmatchColumnDataType(err) => ErrorCode::UnmatchColumnDataType(err.message()),
            AppError::UnmatchMaskPolicyReturnType(err) => {
                ErrorCode::UnmatchMaskPolicyReturnType(err.message())
            }
            AppError::MultiStatementTxnCommitFailed(err) => {
                ErrorCode::UnresolvableConflict(err.message())
            }
            AppError::SequenceError(err) => ErrorCode::SequenceError(err.message()),
            AppError::UpdateStreamMetasFailed(e) => ErrorCode::UnresolvableConflict(e.message()),
            // dictionary
            AppError::DictionaryAlreadyExists(err) => {
                ErrorCode::DictionaryAlreadyExists(err.message())
            }
            AppError::UnknownDictionary(err) => ErrorCode::UnknownDictionary(err.message()),
            AppError::UnknownProcedure(err) => ErrorCode::UnknownProcedure(err.message()),
            AppError::ProcedureAlreadyExists(err) => {
                ErrorCode::ProcedureAlreadyExists(err.message())
            }
            AppError::VirtualColumnIdOutBound(err) => {
                ErrorCode::VirtualColumnIdOutBound(err.message())
            }
            AppError::VirtualColumnTooMany(err) => ErrorCode::VirtualColumnTooMany(err.message()),
            AppError::MarkDatabaseMetaAsGCInProgressFailed(err) => {
                ErrorCode::GeneralDbGcFailure(err.message())
            }
            AppError::CleanDbIdTableNamesFailed(err) => {
                ErrorCode::GeneralDbGcFailure(err.message())
            }
        }
    }
}

impl From<SequenceError> for ErrorCode {
    fn from(app_err: SequenceError) -> Self {
        match app_err {
            SequenceError::SequenceAlreadyExists(err) => ErrorCode::SequenceError(err.message()),
            SequenceError::UnknownSequence(err) => ErrorCode::SequenceError(err.message()),
            SequenceError::OutOfSequenceRange(err) => ErrorCode::SequenceError(err.message()),
            SequenceError::WrongSequenceCount(err) => ErrorCode::SequenceError(err.message()),
            SequenceError::UnsupportedSequenceStorageVersion(err) => {
                ErrorCode::SequenceError(err.message())
            }
        }
    }
}
