//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DropDatabaseReply;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::GetDatabaseReq;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::ListDatabaseReq;
use common_meta_app::schema::ListTableReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TruncateTableReply;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableCopiedFileReply;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_types::GCDroppedDataReply;
use common_meta_types::GCDroppedDataReq;
use common_meta_types::KVAppError;
use common_meta_types::MetaId;

/// SchemaApi defines APIs that provides schema storage, such as database, table.
#[async_trait::async_trait]
pub trait SchemaApi: Send + Sync {
    // database

    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, KVAppError>;

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, KVAppError>;

    async fn undrop_database(
        &self,
        req: UndropDatabaseReq,
    ) -> Result<UndropDatabaseReply, KVAppError>;

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, KVAppError>;

    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, KVAppError>;

    async fn rename_database(
        &self,
        req: RenameDatabaseReq,
    ) -> Result<RenameDatabaseReply, KVAppError>;

    async fn get_database_history(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, KVAppError>;

    // table

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, KVAppError>;

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply, KVAppError>;

    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply, KVAppError>;

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply, KVAppError>;

    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, KVAppError>;

    async fn get_table_history(&self, req: ListTableReq)
    -> Result<Vec<Arc<TableInfo>>, KVAppError>;

    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, KVAppError>;

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> Result<(TableIdent, Arc<TableMeta>), KVAppError>;

    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply, KVAppError>;

    async fn upsert_table_copied_file_info(
        &self,
        req: UpsertTableCopiedFileReq,
    ) -> Result<UpsertTableCopiedFileReply, KVAppError>;

    async fn truncate_table(&self, req: TruncateTableReq)
    -> Result<TruncateTableReply, KVAppError>;

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, KVAppError>;

    async fn update_table_meta(
        &self,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply, KVAppError>;

    // gc dropped {table|db} which out of retention time.
    async fn gc_dropped_data(
        &self,
        req: GCDroppedDataReq,
    ) -> Result<GCDroppedDataReply, KVAppError>;

    async fn count_tables(&self, req: CountTablesReq) -> Result<CountTablesReply, KVAppError>;

    fn name(&self) -> String;
}
