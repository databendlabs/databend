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
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogCreator;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReply;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReply;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReply;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReply;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::IndexMeta;
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
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MetaId;
use databend_common_meta_types::SeqV;
use databend_common_storage::DataOperator;
use futures::TryStreamExt;
use minitrace::func_name;
use opendal::Metakey;

use crate::database::IcebergDatabase;
use crate::IcebergTable;

pub const ICEBERG_CATALOG: &str = "iceberg";

#[derive(Debug)]
pub struct IcebergCreator;

impl CatalogCreator for IcebergCreator {
    fn try_create(&self, info: &CatalogInfo) -> Result<Arc<dyn Catalog>> {
        let opt = match &info.meta.catalog_option {
            CatalogOption::Iceberg(opt) => opt,
            _ => unreachable!(
                "trying to create iceberg catalog from other catalog, must be an internal bug"
            ),
        };

        let data_operator = DataOperator::try_new(&opt.storage_params)?;
        let catalog: Arc<dyn Catalog> =
            Arc::new(IcebergCatalog::try_create(info.clone(), data_operator)?);

        Ok(catalog)
    }
}

/// `Catalog` for a external iceberg storage
///
/// - Metadata of databases are saved in meta store
/// - Instances of `Database` are created from reading subdirectories of
///    Iceberg table
/// - Table metadata are saved in external Iceberg storage
#[derive(Clone, Debug)]
pub struct IcebergCatalog {
    /// info of this iceberg table.
    info: CatalogInfo,

    /// underlying storage access operator
    operator: DataOperator,
}

impl IcebergCatalog {
    /// create a new iceberg catalog from the endpoint_address
    ///
    /// # NOTE
    ///
    /// endpoint_url should be set as in `Stage`s.
    /// For example, to create a iceberg catalog on S3, the endpoint_url should be:
    ///
    /// `s3://bucket_name/path/to/iceberg_catalog`
    ///
    /// Some iceberg storages barely store tables in the root directory,
    /// making there no path for database.
    ///
    /// Such catalog will be seen as an `flatten` catalogs,
    /// a `default` database will be generated directly
    #[minitrace::trace]
    pub fn try_create(info: CatalogInfo, operator: DataOperator) -> Result<Self> {
        Ok(Self { info, operator })
    }

    /// list read databases
    #[minitrace::trace]
    #[async_backtrace::framed]
    pub async fn list_database_from_read(&self) -> Result<Vec<Arc<dyn Database>>> {
        let op = self.operator.operator();
        let mut dbs = vec![];
        let mut ls = op.lister_with("/").metakey(Metakey::Mode).await?;
        while let Some(dir) = ls.try_next().await? {
            let meta = dir.metadata();
            if !meta.is_dir() {
                continue;
            }
            let db_name = dir.name().strip_suffix('/').unwrap_or_default();
            if db_name.is_empty() {
                // skip empty named directory
                // but I can hardly imagine an empty named folder.
                continue;
            }
            let dummy = Tenant::new_or_err("dummy", func_name!()).unwrap();
            let db: Arc<dyn Database> = self.get_database(&dummy, db_name).await?;
            dbs.push(db);
        }
        Ok(dbs)
    }
}

#[async_trait]
impl Catalog for IcebergCatalog {
    fn name(&self) -> String {
        self.info.name_ident.catalog_name.clone()
    }
    fn info(&self) -> CatalogInfo {
        self.info.clone()
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn get_database(&self, _tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        let rel_path = format!("{db_name}/");

        let operator = self.operator.operator();
        if !operator.is_exist(&rel_path).await? {
            return Err(ErrorCode::UnknownDatabase(format!(
                "Database {db_name} does not exist"
            )));
        }

        // storage params for database
        let db_sp = self
            .operator
            .params()
            .map_root(|root| format!("{root}{rel_path}"));
        let db_root = DataOperator::try_create(&db_sp).await?;

        Ok(Arc::new(IcebergDatabase::create(
            &self.name(),
            db_name,
            db_root,
        )))
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        self.list_database_from_read().await
    }

    #[async_backtrace::framed]
    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        unimplemented!()
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        if table_info.meta.storage_params.is_none() {
            return Err(ErrorCode::BadArguments(
                "table storage params not set, this is not a valid table info for iceberg table",
            ));
        }

        let table: Arc<dyn Table> = IcebergTable::try_create(table_info.clone())?.into();

        Ok(table)
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, _table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        unimplemented!()
    }

    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        _table_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get tables name by ids in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_db_name_by_id(&self, _table_id: MetaId) -> Result<String> {
        Err(ErrorCode::Unimplemented(
            "Cannot get db name by id in ICEBERG catalog",
        ))
    }

    async fn mget_database_names_by_ids(
        &self,
        _tenant: &Tenant,
        _db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get dbs name by ids in ICEBERG catalog",
        ))
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn get_table(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let db = self.get_database(tenant, db_name).await?;
        db.get_table(table_name).await
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let db = self.get_database(tenant, db_name).await?;
        db.list_tables().await
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn exists_table(&self, tenant: &Tenant, db_name: &str, table_name: &str) -> Result<bool> {
        let db = self.get_database(tenant, db_name).await?;
        match db.get_table(table_name).await {
            Ok(_) => Ok(true),
            Err(e) => match e.code() {
                ErrorCode::UNKNOWN_TABLE => Ok(false),
                _ => Err(e),
            },
        }
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_table_meta(
        &self,
        _table_info: &TableInfo,
        _req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<CreateTableIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<DropTableIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_lock_revisions(&self, _req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_lock_revision(&self, _req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn extend_lock_revision(&self, _req: ExtendLockRevReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn delete_lock_revision(&self, _req: DeleteLockRevReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_locks(&self, _req: ListLocksReq) -> Result<Vec<LockInfo>> {
        unimplemented!()
    }

    // Table index

    #[async_backtrace::framed]
    async fn create_index(&self, _req: CreateIndexReq) -> Result<CreateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_index(&self, _req: DropIndexReq) -> Result<DropIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_index(&self, _req: GetIndexReq) -> Result<GetIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_index(&self, _req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_indexes(&self, _req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_index_ids_by_table_id(&self, _req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_indexes_by_table_id(
        &self,
        _req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        unimplemented!()
    }

    // Virtual column

    #[async_backtrace::framed]
    async fn create_virtual_column(
        &self,
        _req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_virtual_column(
        &self,
        _req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_virtual_column(
        &self,
        _req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_virtual_columns(
        &self,
        _req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        unimplemented!()
    }

    /// Table function

    // Get function by name.
    fn get_table_function(
        &self,
        _func_name: &str,
        _tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        unimplemented!()
    }

    // List all table functions' names.
    fn list_table_functions(&self) -> Vec<String> {
        vec![]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }

    async fn create_sequence(&self, _req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        unimplemented!()
    }
    async fn get_sequence(&self, _req: GetSequenceReq) -> Result<GetSequenceReply> {
        unimplemented!()
    }

    async fn get_sequence_next_value(
        &self,
        _req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply> {
        unimplemented!()
    }

    async fn drop_sequence(&self, _req: DropSequenceReq) -> Result<DropSequenceReply> {
        unimplemented!()
    }
}
