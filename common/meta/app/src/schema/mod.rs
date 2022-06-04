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

//! Schema types

mod database;
mod table;

pub use database::CreateDatabaseReply;
pub use database::CreateDatabaseReq;
pub use database::DatabaseId;
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
pub use table::DropTableReply;
pub use table::DropTableReq;
pub use table::GetTableReq;
pub use table::ListTableReq;
pub use table::RenameTableReply;
pub use table::RenameTableReq;
pub use table::TableId;
pub use table::TableIdList;
pub use table::TableIdListKey;
pub use table::TableIdent;
pub use table::TableInfo;
pub use table::TableMeta;
pub use table::TableNameIdent;
pub use table::TableStatistics;
pub use table::UndropTableReply;
pub use table::UndropTableReq;
pub use table::UpdateTableMetaReply;
pub use table::UpdateTableMetaReq;
pub use table::UpsertTableOptionReply;
pub use table::UpsertTableOptionReq;
