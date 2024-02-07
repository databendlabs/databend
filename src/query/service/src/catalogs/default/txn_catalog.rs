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

use std::any::Any;
use std::cell::RefCell;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_catalog::catalog::Catalog;
use databend_common_exception::Result;
use databend_common_meta_api::SchemaApi;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CountTablesReply;
use databend_common_meta_app::schema::CountTablesReq;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReply;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DatabaseIdent;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DatabaseNameIdent;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReply;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GcDroppedTableResp;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListDatabaseReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableReply;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpdateTableMetaReply;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateVirtualColumnReply;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::MetaId;
use databend_storages_common_txn::TxnManagerRef;
use log::info;

use crate::catalogs::default::catalog_context::CatalogContext;
use crate::catalogs::default::MutableCatalog;
use crate::databases::Database;
use crate::databases::DatabaseContext;
use crate::databases::DatabaseFactory;
use crate::storages::StorageDescription;
use crate::storages::StorageFactory;
use crate::storages::Table;

#[derive(Clone)]
pub struct TxnCatalog {
    inner: MutableCatalog,
    txn_mgr: Arc<Mutex<Option<TxnManagerRef>>>,
}

impl Debug for TxnCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnCatalog").finish_non_exhaustive()
    }
}

impl TxnCatalog {
    pub fn create(inner: MutableCatalog) -> Self {
        TxnCatalog {
            inner,
            txn_mgr: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl Catalog for TxnCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        self.inner.name()
    }

    fn info(&self) -> CatalogInfo {
        self.inner.info()
    }

    /// Database.

    // Get the database by name.
    async fn get_database(&self, tenant: &str, db_name: &str) -> Result<Arc<dyn Database>> {
        todo!()
    }

    // Get all the databases.
    async fn list_databases(&self, tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        todo!()
    }

    // Operation with database.
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        todo!()
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        todo!()
    }

    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        todo!()
    }

    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply> {
        todo!()
    }

    async fn drop_index(&self, req: DropIndexReq) -> Result<DropIndexReply> {
        todo!()
    }

    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply> {
        todo!()
    }

    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        todo!()
    }

    async fn list_indexes(&self, req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        todo!()
    }

    async fn list_index_ids_by_table_id(&self, req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        todo!()
    }

    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        todo!()
    }

    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        todo!()
    }

    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        todo!()
    }

    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
        todo!()
    }

    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        todo!()
    }

    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        todo!()
    }

    /// Table.

    // Build a `Arc<dyn Table>` from `TableInfo`.
    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        todo!()
    }

    // Get the table meta by meta id.
    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        todo!()
    }

    // Get the table name by meta id.
    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<String> {
        todo!()
    }

    // Get the db name by meta id.
    async fn get_db_name_by_id(&self, db_id: MetaId) -> databend_common_exception::Result<String> {
        todo!()
    }

    // Get one table by db and table name.
    async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        todo!()
    }

    async fn list_tables(&self, tenant: &str, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        todo!()
    }
    async fn list_tables_history(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        todo!()
    }

    async fn get_drop_table_infos(
        &self,
        _req: ListDroppedTableReq,
    ) -> Result<(Vec<Arc<dyn Table>>, Vec<DroppedId>)> {
        todo!()
    }

    async fn gc_drop_tables(&self, _req: GcDroppedTableReq) -> Result<GcDroppedTableResp> {
        todo!()
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        todo!()
    }

    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        todo!()
    }

    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply> {
        todo!()
    }

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        todo!()
    }

    async fn upsert_table_option(
        &self,
        tenant: &str,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        todo!()
    }

    async fn update_table_meta(
        &self,
        table_info: &TableInfo,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply> {
        todo!()
    }

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        todo!()
    }

    async fn count_tables(&self, req: CountTablesReq) -> Result<CountTablesReply> {
        todo!()
    }

    async fn get_table_copied_file_info(
        &self,
        tenant: &str,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        todo!()
    }

    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        todo!()
    }

    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        todo!()
    }

    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        todo!()
    }

    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()> {
        todo!()
    }

    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()> {
        todo!()
    }

    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>> {
        todo!()
    }

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        todo!()
    }

    fn set_txn_manager(&self, txn_mgr: Option<TxnManagerRef>) {
        let mut guard = self.txn_mgr.lock().unwrap();
        *guard = txn_mgr;
    }
}
