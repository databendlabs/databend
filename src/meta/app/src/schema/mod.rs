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

mod catalog;
mod database;
mod index;
mod table;
mod virtual_column;

pub use catalog::*;
pub use database::CreateDatabaseReply;
pub use database::CreateDatabaseReq;
pub use database::DatabaseId;
pub use database::DatabaseIdToName;
pub use database::DatabaseIdent;
pub use database::DatabaseInfo;
pub use database::DatabaseInfoFilter;
pub use database::DatabaseMeta;
pub use database::DatabaseNameIdent;
pub use database::DbIdList;
pub use database::DbIdListKey;
pub use database::DropDatabaseReply;
pub use database::DropDatabaseReq;
pub use database::GetDatabaseReq;
pub use database::ListDatabaseReq;
pub use database::RenameDatabaseReply;
pub use database::RenameDatabaseReq;
pub use database::UndropDatabaseReply;
pub use database::UndropDatabaseReq;
pub use index::*;
pub use table::CountTablesKey;
pub use table::CountTablesReply;
pub use table::CountTablesReq;
pub use table::CreateTableLockRevReply;
pub use table::CreateTableLockRevReq;
pub use table::CreateTableReply;
pub use table::CreateTableReq;
pub use table::DBIdTableName;
pub use table::DatabaseType;
pub use table::DeleteTableLockRevReq;
pub use table::DropTableByIdReq;
pub use table::DropTableReply;
pub use table::DroppedId;
pub use table::EmptyProto;
pub use table::ExtendTableLockRevReq;
pub use table::GcDroppedTableReq;
pub use table::GcDroppedTableResp;
pub use table::GetTableCopiedFileReply;
pub use table::GetTableCopiedFileReq;
pub use table::GetTableReq;
pub use table::ListDroppedTableReq;
pub use table::ListDroppedTableResp;
pub use table::ListTableLockRevReq;
pub use table::ListTableReq;
pub use table::RenameTableReply;
pub use table::RenameTableReq;
pub use table::SetTableColumnMaskPolicyAction;
pub use table::SetTableColumnMaskPolicyReply;
pub use table::SetTableColumnMaskPolicyReq;
pub use table::TableCopiedFileInfo;
pub use table::TableCopiedFileLockKey;
pub use table::TableCopiedFileNameIdent;
pub use table::TableId;
pub use table::TableIdList;
pub use table::TableIdListKey;
pub use table::TableIdToName;
pub use table::TableIdent;
pub use table::TableInfo;
pub use table::TableInfoFilter;
pub use table::TableLockKey;
pub use table::TableMeta;
pub use table::TableNameIdent;
pub use table::TableStatistics;
pub use table::TruncateTableReply;
pub use table::TruncateTableReq;
pub use table::UndropTableReply;
pub use table::UndropTableReq;
pub use table::UpdateTableMetaReply;
pub use table::UpdateTableMetaReq;
pub use table::UpsertTableCopiedFileReply;
pub use table::UpsertTableCopiedFileReq;
pub use table::UpsertTableOptionReply;
pub use table::UpsertTableOptionReq;
pub use virtual_column::CreateVirtualColumnReply;
pub use virtual_column::CreateVirtualColumnReq;
pub use virtual_column::DropVirtualColumnReply;
pub use virtual_column::DropVirtualColumnReq;
pub use virtual_column::ListVirtualColumnsReq;
pub use virtual_column::UpdateVirtualColumnReply;
pub use virtual_column::UpdateVirtualColumnReq;
pub use virtual_column::VirtualColumnMeta;
pub use virtual_column::VirtualColumnNameIdent;

const PREFIX_CATALOG: &str = "__fd_catalog";
const PREFIX_CATALOG_BY_ID: &str = "__fd_catalog_by_id";
const PREFIX_CATALOG_ID_TO_NAME: &str = "__fd_catalog_id_to_name";

const PREFIX_DB_ID_LIST: &str = "__fd_db_id_list";
const PREFIX_DATABASE: &str = "__fd_database";
const PREFIX_DATABASE_BY_ID: &str = "__fd_database_by_id";
const PREFIX_DATABASE_ID_TO_NAME: &str = "__fd_database_id_to_name";

const PREFIX_TABLE: &str = "__fd_table";
const PREFIX_TABLE_BY_ID: &str = "__fd_table_by_id";
const PREFIX_TABLE_ID_LIST: &str = "__fd_table_id_list";
const PREFIX_TABLE_COUNT: &str = "__fd_table_count";
const PREFIX_TABLE_ID_TO_NAME: &str = "__fd_table_id_to_name";
const PREFIX_TABLE_COPIED_FILES: &str = "__fd_table_copied_files";
const PREFIX_TABLE_COPIED_FILES_LOCK: &str = "__fd_table_copied_file_lock";
const PREFIX_INDEX: &str = "__fd_index";
const PREFIX_INDEX_ID_TO_NAME: &str = "__fd_index_id_to_name";
const PREFIX_INDEX_BY_ID: &str = "__fd_index_by_id";
const PREFIX_TABLE_LOCK: &str = "__fd_table_lock";
const PREFIX_VIRTUAL_COLUMN: &str = "__fd_virtual_column";
