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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use databend_common_catalog::catalog::StorageDescription;
use databend_common_exception::Result;
use databend_common_grpc::RpcClientConf;
use databend_common_meta_api::kv_app_error::KVAppError;
use databend_common_meta_api::SchemaApi;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CountTablesReply;
use databend_common_meta_app::schema::CountTablesReq;
use databend_common_meta_app::schema::CreateCatalogReply;
use databend_common_meta_app::schema::CreateCatalogReq;
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
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DropCatalogReply;
use databend_common_meta_app::schema::DropCatalogReq;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReply;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GcDroppedTableResp;
use databend_common_meta_app::schema::GetCatalogReq;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetLVTReply;
use databend_common_meta_app::schema::GetLVTReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListCatalogReq;
use databend_common_meta_app::schema::ListDatabaseReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListDroppedTableResp;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::ListTableReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetLVTReply;
use databend_common_meta_app::schema::SetLVTReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableId;
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
use databend_common_meta_embedded::MetaEmbedded;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::GetKVReply;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::MGetKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaId;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use futures::Stream;
use log::info;

#[async_trait::async_trait]
pub trait TableMetaRefresher: Send + Sync {
    async fn refresh(&self, table_meta: TableMeta) -> Result<TableMeta>;
}

pub struct MetaWithRefresher {
    pub(crate) table_meta_refresher: Arc<dyn TableMetaRefresher>,
    pub(crate) meta: MetaStore,
}
#[async_trait::async_trait]
impl SchemaApi for MetaWithRefresher {
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> std::result::Result<CreateDatabaseReply, KVAppError> {
        self.meta.create_database(req).await
    }

    async fn drop_database(
        &self,
        req: DropDatabaseReq,
    ) -> std::result::Result<DropDatabaseReply, KVAppError> {
        self.meta.drop_database(req).await
    }

    async fn undrop_database(
        &self,
        req: UndropDatabaseReq,
    ) -> std::result::Result<UndropDatabaseReply, KVAppError> {
        self.meta.undrop_database(req).await
    }

    async fn get_database(
        &self,
        req: GetDatabaseReq,
    ) -> std::result::Result<Arc<DatabaseInfo>, KVAppError> {
        self.meta.get_database(req).await
    }

    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> std::result::Result<Vec<Arc<DatabaseInfo>>, KVAppError> {
        self.meta.list_databases(req).await
    }

    async fn rename_database(
        &self,
        req: RenameDatabaseReq,
    ) -> std::result::Result<RenameDatabaseReply, KVAppError> {
        self.meta.rename_database(req).await
    }

    async fn get_database_history(
        &self,
        req: ListDatabaseReq,
    ) -> std::result::Result<Vec<Arc<DatabaseInfo>>, KVAppError> {
        self.meta.get_database_history(req).await
    }

    // index

    async fn create_index(
        &self,
        req: CreateIndexReq,
    ) -> std::result::Result<CreateIndexReply, KVAppError> {
        self.meta.create_index(req).await
    }

    async fn drop_index(
        &self,
        req: DropIndexReq,
    ) -> std::result::Result<DropIndexReply, KVAppError> {
        self.meta.drop_index(req).await
    }

    async fn get_index(&self, req: GetIndexReq) -> std::result::Result<GetIndexReply, KVAppError> {
        self.meta.get_index(req).await
    }

    async fn update_index(
        &self,
        req: UpdateIndexReq,
    ) -> std::result::Result<UpdateIndexReply, KVAppError> {
        self.meta.update_index(req).await
    }

    async fn list_indexes(
        &self,
        req: ListIndexesReq,
    ) -> std::result::Result<Vec<(u64, String, IndexMeta)>, KVAppError> {
        self.meta.list_indexes(req).await
    }

    async fn list_index_ids_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> std::result::Result<Vec<u64>, KVAppError> {
        self.meta.list_indexe_ids_by_table_id(req).await
    }

    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> std::result::Result<Vec<(u64, String, IndexMeta)>, KVAppError> {
        self.meta.list_indexes_by_table_id(req).await
    }

    // virtual column

    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> std::result::Result<CreateVirtualColumnReply, KVAppError> {
        self.meta.create_virual_column(req).await
    }

    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> std::result::Result<UpdateVirtualColumnReply, KVAppError> {
        self.meta.update_virtual_column(req).await
    }

    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> std::result::Result<DropVirtualColumnReply, KVAppError>;

    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> std::result::Result<Vec<VirtualColumnMeta>, KVAppError>;

    // table

    async fn create_table(
        &self,
        req: CreateTableReq,
    ) -> std::result::Result<CreateTableReply, KVAppError>;

    /// List all tables belonging to every db and every tenant.
    ///
    /// I.e.: all tables found in key-space: `__fd_table_by_id/`.
    /// Notice that this key space includes both deleted and non-deleted table-metas. `TableMeta.drop_on.is_some()` indicates it's a deleted(but not yet garbage collected) one.
    ///
    /// It returns a list of (table-id, table-meta-seq, table-meta).
    async fn list_all_tables(
        &self,
    ) -> std::result::Result<Vec<(TableId, u64, TableMeta)>, KVAppError>;

    async fn drop_table_by_id(
        &self,
        req: DropTableByIdReq,
    ) -> std::result::Result<DropTableReply, KVAppError> {
    }

    async fn undrop_table(
        &self,
        req: UndropTableReq,
    ) -> std::result::Result<UndropTableReply, KVAppError>;

    async fn rename_table(
        &self,
        req: RenameTableReq,
    ) -> std::result::Result<RenameTableReply, KVAppError>;

    async fn get_table(&self, req: GetTableReq) -> std::result::Result<Arc<TableInfo>, KVAppError>;

    async fn get_table_history(
        &self,
        req: ListTableReq,
    ) -> std::result::Result<Vec<Arc<TableInfo>>, KVAppError>;

    async fn list_tables(
        &self,
        req: ListTableReq,
    ) -> std::result::Result<Vec<Arc<TableInfo>>, KVAppError>;

    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> std::result::Result<(TableIdent, Arc<TableMeta>), KVAppError>;

    async fn get_table_name_by_id(
        &self,
        table_id: MetaId,
    ) -> std::result::Result<String, KVAppError>;

    async fn get_db_name_by_id(&self, db_id: MetaId) -> std::result::Result<String, KVAppError>;

    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> std::result::Result<GetTableCopiedFileReply, KVAppError>;

    async fn truncate_table(
        &self,
        req: TruncateTableReq,
    ) -> std::result::Result<TruncateTableReply, KVAppError>;

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> std::result::Result<UpsertTableOptionReply, KVAppError>;

    async fn update_table_meta(
        &self,
        req: UpdateTableMetaReq,
    ) -> std::result::Result<UpdateTableMetaReply, KVAppError>;

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> std::result::Result<SetTableColumnMaskPolicyReply, KVAppError>;

    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> std::result::Result<ListDroppedTableResp, KVAppError>;

    async fn gc_drop_tables(
        &self,
        req: GcDroppedTableReq,
    ) -> std::result::Result<GcDroppedTableResp, KVAppError>;

    async fn count_tables(
        &self,
        req: CountTablesReq,
    ) -> std::result::Result<CountTablesReply, KVAppError>;

    async fn list_lock_revisions(
        &self,
        req: ListLockRevReq,
    ) -> std::result::Result<Vec<(u64, LockMeta)>, KVAppError>;

    async fn create_lock_revision(
        &self,
        req: CreateLockRevReq,
    ) -> std::result::Result<CreateLockRevReply, KVAppError>;

    async fn extend_lock_revision(
        &self,
        req: ExtendLockRevReq,
    ) -> std::result::Result<(), KVAppError>;

    async fn delete_lock_revision(
        &self,
        req: DeleteLockRevReq,
    ) -> std::result::Result<(), KVAppError>;

    async fn list_locks(&self, req: ListLocksReq)
    -> std::result::Result<Vec<LockInfo>, KVAppError>;

    async fn create_catalog(
        &self,
        req: CreateCatalogReq,
    ) -> std::result::Result<CreateCatalogReply, KVAppError>;

    async fn get_catalog(
        &self,
        req: GetCatalogReq,
    ) -> std::result::Result<Arc<CatalogInfo>, KVAppError>;

    async fn drop_catalog(
        &self,
        req: DropCatalogReq,
    ) -> std::result::Result<DropCatalogReply, KVAppError>;

    async fn list_catalogs(
        &self,
        req: ListCatalogReq,
    ) -> std::result::Result<Vec<Arc<CatalogInfo>>, KVAppError>;

    // least visible time
    async fn set_table_lvt(&self, req: SetLVTReq) -> std::result::Result<SetLVTReply, KVAppError>;
    async fn get_table_lvt(&self, req: GetLVTReq) -> std::result::Result<GetLVTReply, KVAppError>;

    fn name(&self) -> String;
}
