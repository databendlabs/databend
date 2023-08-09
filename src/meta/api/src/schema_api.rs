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

use std::sync::Arc;

use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateCatalogReply;
use common_meta_app::schema::CreateCatalogReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateIndexReply;
use common_meta_app::schema::CreateIndexReq;
use common_meta_app::schema::CreateTableLockRevReply;
use common_meta_app::schema::CreateTableLockRevReq;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::CreateVirtualColumnReply;
use common_meta_app::schema::CreateVirtualColumnReq;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DeleteTableLockRevReq;
use common_meta_app::schema::DropCatalogReply;
use common_meta_app::schema::DropCatalogReq;
use common_meta_app::schema::DropDatabaseReply;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropIndexReply;
use common_meta_app::schema::DropIndexReq;
use common_meta_app::schema::DropTableByIdReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropVirtualColumnReply;
use common_meta_app::schema::DropVirtualColumnReq;
use common_meta_app::schema::ExtendTableLockRevReq;
use common_meta_app::schema::GcDroppedTableReq;
use common_meta_app::schema::GcDroppedTableResp;
use common_meta_app::schema::GetCatalogReq;
use common_meta_app::schema::GetDatabaseReq;
use common_meta_app::schema::GetIndexReply;
use common_meta_app::schema::GetIndexReq;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::ListCatalogReq;
use common_meta_app::schema::ListDatabaseReq;
use common_meta_app::schema::ListDroppedTableReq;
use common_meta_app::schema::ListDroppedTableResp;
use common_meta_app::schema::ListIndexesByIdReq;
use common_meta_app::schema::ListIndexesReq;
use common_meta_app::schema::ListTableLockRevReq;
use common_meta_app::schema::ListTableReq;
use common_meta_app::schema::ListVirtualColumnsReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::SetTableColumnMaskPolicyReply;
use common_meta_app::schema::SetTableColumnMaskPolicyReq;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TruncateTableReply;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateIndexReply;
use common_meta_app::schema::UpdateIndexReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpdateVirtualColumnReply;
use common_meta_app::schema::UpdateVirtualColumnReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_app::schema::VirtualColumnMeta;
use common_meta_types::MetaId;

use crate::kv_app_error::KVAppError;

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

    // index

    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply, KVAppError>;

    async fn drop_index(&self, req: DropIndexReq) -> Result<DropIndexReply, KVAppError>;

    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply, KVAppError>;

    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply, KVAppError>;

    async fn list_indexes(
        &self,
        req: ListIndexesReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>, KVAppError>;

    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<u64>, KVAppError>;

    // virtual column

    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply, KVAppError>;

    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply, KVAppError>;

    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply, KVAppError>;

    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>, KVAppError>;

    // table

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, KVAppError>;

    /// List all tables belonging to every db and every tenant.
    ///
    /// I.e.: all tables found in key-space: `__fd_table_by_id/`.
    /// Notice that this key space includes both deleted and non-deleted table-metas. `TableMeta.drop_on.is_some()` indicates it's a deleted(but not yet garbage collected) one.
    ///
    /// It returns a list of (table-id, table-meta-seq, table-meta).
    async fn list_all_tables(&self) -> Result<Vec<(TableId, u64, TableMeta)>, KVAppError>;

    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply, KVAppError>;

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

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply, KVAppError>;

    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<ListDroppedTableResp, KVAppError>;

    async fn gc_drop_tables(
        &self,
        req: GcDroppedTableReq,
    ) -> Result<GcDroppedTableResp, KVAppError>;

    async fn count_tables(&self, req: CountTablesReq) -> Result<CountTablesReply, KVAppError>;

    async fn list_table_lock_revs(&self, req: ListTableLockRevReq) -> Result<Vec<u64>, KVAppError>;

    async fn create_table_lock_rev(
        &self,
        req: CreateTableLockRevReq,
    ) -> Result<CreateTableLockRevReply, KVAppError>;

    async fn extend_table_lock_rev(&self, req: ExtendTableLockRevReq) -> Result<(), KVAppError>;

    async fn delete_table_lock_rev(&self, req: DeleteTableLockRevReq) -> Result<(), KVAppError>;

    async fn create_catalog(&self, req: CreateCatalogReq)
    -> Result<CreateCatalogReply, KVAppError>;

    async fn get_catalog(&self, req: GetCatalogReq) -> Result<Arc<CatalogInfo>, KVAppError>;

    async fn drop_catalog(&self, req: DropCatalogReq) -> Result<DropCatalogReply, KVAppError>;

    async fn list_catalogs(&self, req: ListCatalogReq)
    -> Result<Vec<Arc<CatalogInfo>>, KVAppError>;

    fn name(&self) -> String;
}
