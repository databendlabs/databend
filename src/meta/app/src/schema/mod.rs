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
mod table;

pub use catalog::CatalogMeta;
pub use catalog::CatalogNameIdent;
pub use catalog::CatalogOption;
pub use catalog::CatalogType;
pub use catalog::CreateCatalogReq;
pub use catalog::DropCatalogReq;
pub use catalog::IcebergCatalogOption;
pub use database::CreateDatabaseReply;
pub use database::CreateDatabaseReq;
pub use database::DatabaseId;
pub use database::DatabaseIdToName;
pub use database::DatabaseIdent;
pub use database::DatabaseInfo;
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
pub use table::CountTablesKey;
pub use table::CountTablesReply;
pub use table::CountTablesReq;
pub use table::CreateTableReply;
pub use table::CreateTableReq;
pub use table::DBIdTableName;
pub use table::DatabaseType;
pub use table::DropTableByIdReq;
pub use table::DropTableReply;
pub use table::GetTableCopiedFileReply;
pub use table::GetTableCopiedFileReq;
pub use table::GetTableReq;
pub use table::ListTableReq;
pub use table::RenameTableReply;
pub use table::RenameTableReq;
pub use table::TableCopiedFileInfo;
pub use table::TableCopiedFileLock;
pub use table::TableCopiedFileLockKey;
pub use table::TableCopiedFileNameIdent;
pub use table::TableId;
pub use table::TableIdList;
pub use table::TableIdListKey;
pub use table::TableIdToName;
pub use table::TableIdent;
pub use table::TableInfo;
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
