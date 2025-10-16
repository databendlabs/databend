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

//! Schema types

mod auto_increment;
mod auto_increment_storage;
pub mod catalog;
pub mod catalog_id_ident;
pub mod catalog_id_to_name_ident;
pub mod catalog_name_ident;
mod constraint;
mod create_option;
mod database;
pub mod database_id;
pub mod database_id_history_ident;
pub mod database_name_ident;
mod dictionary;
pub mod dictionary_id_ident;
mod dictionary_identity;
pub mod dictionary_name_ident;
mod index;
pub mod index_id_ident;
pub mod index_id_to_name_ident;
pub mod index_name_ident;
mod least_visible_time;
pub mod least_visible_time_ident;
mod lock;
pub mod marked_deleted_index_id;
pub mod marked_deleted_index_ident;
pub mod marked_deleted_table_index_id;
pub mod marked_deleted_table_index_ident;
mod ownership;
mod sequence;
pub mod sequence_storage;
mod table;
pub mod table_lock_ident;
pub mod table_niv;

pub use auto_increment::GetAutoIncrementNextValueReply;
pub use auto_increment::GetAutoIncrementNextValueReq;
pub use auto_increment_storage::AutoIncrementStorageIdent;
pub use auto_increment_storage::AutoIncrementStorageRsc;
pub use auto_increment_storage::AutoIncrementStorageValue;
pub use catalog::*;
pub use catalog_id_ident::CatalogIdIdent;
pub use catalog_id_to_name_ident::CatalogIdToNameIdent;
pub use catalog_name_ident::CatalogNameIdent;
pub use constraint::Constraint;
pub use create_option::CreateOption;
pub use database::CreateDatabaseReply;
pub use database::CreateDatabaseReq;
pub use database::DatabaseIdToName;
pub use database::DatabaseInfo;
pub use database::DatabaseMeta;
pub use database::DbIdList;
pub use database::DropDatabaseReply;
pub use database::DropDatabaseReq;
pub use database::GetDatabaseReq;
pub use database::ListDatabaseReq;
pub use database::RenameDatabaseReply;
pub use database::RenameDatabaseReq;
pub use database::ShareDbId;
pub use database::UndropDatabaseReply;
pub use database::UndropDatabaseReq;
pub use database_id::DatabaseId;
pub use database_id_history_ident::DatabaseIdHistoryIdent;
pub use dictionary::*;
pub use dictionary_identity::DictionaryIdentity;
pub use index::*;
pub use index_name_ident::IndexNameIdent;
pub use index_name_ident::IndexNameIdentRaw;
pub use least_visible_time::LeastVisibleTime;
pub use lock::CreateLockRevReply;
pub use lock::CreateLockRevReq;
pub use lock::DeleteLockRevReq;
pub use lock::ExtendLockRevReq;
pub use lock::ListLockRevReq;
pub use lock::ListLocksReq;
pub use lock::LockInfo;
pub use lock::LockKey;
pub use lock::LockMeta;
pub use lock::LockType;
pub use ownership::Ownership;
pub use sequence::*;
pub use table::CommitTableMetaReply;
pub use table::CommitTableMetaReq;
pub use table::CreateTableIndexReq;
pub use table::CreateTableReply;
pub use table::CreateTableReq;
pub use table::DBIdTableName;
pub use table::DatabaseType;
pub use table::DropTableByIdReq;
pub use table::DropTableIndexReq;
pub use table::DropTableReply;
pub use table::DroppedId;
pub use table::EmptyProto;
pub use table::GcDroppedTableReq;
pub use table::GetMarkedDeletedTableIndexesReply;
pub use table::GetTableCopiedFileReply;
pub use table::GetTableCopiedFileReq;
pub use table::GetTableReq;
pub use table::ListDroppedTableReq;
pub use table::ListDroppedTableResp;
pub use table::ListTableCopiedFileReply;
pub use table::ListTableReq;
pub use table::RenameTableReply;
pub use table::RenameTableReq;
pub use table::SecurityPolicyColumnMap;
pub use table::SetSecurityPolicyAction;
pub use table::SetTableColumnMaskPolicyReply;
pub use table::SetTableColumnMaskPolicyReq;
pub use table::SetTableRowAccessPolicyReply;
pub use table::SetTableRowAccessPolicyReq;
pub use table::SwapTableReply;
pub use table::SwapTableReq;
pub use table::TableCopiedFileInfo;
pub use table::TableCopiedFileNameIdent;
pub use table::TableId;
pub use table::TableIdHistoryIdent;
pub use table::TableIdList;
pub use table::TableIdToName;
pub use table::TableIdent;
pub use table::TableIndex;
pub use table::TableIndexType;
pub use table::TableInfo;
pub use table::TableMeta;
pub use table::TableNameIdent;
pub use table::TablePartition;
pub use table::TableStatistics;
pub use table::TruncateTableReply;
pub use table::TruncateTableReq;
pub use table::UndropTableByIdReq;
pub use table::UndropTableReq;
pub use table::UpdateMultiTableMetaReq;
pub use table::UpdateMultiTableMetaResult;
pub use table::UpdateStreamMetaReq;
pub use table::UpdateTableMetaReply;
pub use table::UpdateTableMetaReq;
pub use table::UpdateTempTableReq;
pub use table::UpsertTableCopiedFileReply;
pub use table::UpsertTableCopiedFileReq;
pub use table::UpsertTableOptionReply;
pub use table::UpsertTableOptionReq;
pub use table_lock_ident::TableLockIdent;
