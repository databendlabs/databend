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
use std::fmt::Debug;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogCreator;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UDTFServer;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateDictionaryReply;
use databend_common_meta_app::schema::CreateDictionaryReq;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReply;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReq;
use databend_common_meta_app::schema::GetDictionaryReply;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::ListSequencesReply;
use databend_common_meta_app::schema::ListSequencesReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameDictionaryReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReply;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReq;
use databend_common_meta_app::schema::SwapTableReply;
use databend_common_meta_app::schema::SwapTableReq;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateDictionaryReply;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_meta_types::*;
use faststr::FastStr;
use hive_metastore::Partition;
use hive_metastore::ThriftHiveMetastoreClient;
use hive_metastore::ThriftHiveMetastoreClientBuilder;
use volo_thrift::MaybeException;
use volo_thrift::transport::pool;

use super::hive_database::HiveDatabase;
use crate::hive_table::HiveTable;
use crate::utils::from_thrift_error;
use crate::utils::from_thrift_exception;

#[derive(Debug)]
pub struct HiveCreator;

impl CatalogCreator for HiveCreator {
    fn try_create(&self, info: Arc<CatalogInfo>) -> Result<Arc<dyn Catalog>> {
        let opt = match &info.meta.catalog_option {
            CatalogOption::Hive(opt) => opt,
            _ => unreachable!(
                "trying to create hive catalog from other catalog, must be an internal bug"
            ),
        };

        let catalog: Arc<dyn Catalog> = Arc::new(HiveCatalog::try_create(
            info.clone(),
            opt.storage_params.clone().map(|v| *v),
            &opt.address,
        )?);

        Ok(catalog)
    }
}

#[derive(Clone)]
pub struct HiveCatalog {
    info: Arc<CatalogInfo>,

    /// storage params for this hive catalog
    ///
    /// - Some(sp) means the catalog has its own storage.
    /// - None means the catalog is using the same storage with default catalog.
    sp: Option<StorageParams>,

    /// address of hive meta store service
    client_address: String,
    client: ThriftHiveMetastoreClient,
}

impl Debug for HiveCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("HiveCatalog")
            .field("info", &self.info)
            .field("sp", &self.sp)
            .field("client_address", &self.client_address)
            .finish_non_exhaustive()
    }
}

impl HiveCatalog {
    pub fn try_create(
        info: Arc<CatalogInfo>,
        sp: Option<StorageParams>,
        hms_address: impl Into<String>,
    ) -> Result<HiveCatalog> {
        let client_address = hms_address.into();

        let address = client_address
            .as_str()
            .to_socket_addrs()
            .map_err(|e| {
                ErrorCode::InvalidConfig(format!(
                    "hms address {} is not valid: {}",
                    client_address, e
                ))
            })?
            .next()
            .ok_or_else(|| {
                ErrorCode::InvalidConfig(format!("hms address {} is not valid", client_address))
            })?;

        let client = ThriftHiveMetastoreClientBuilder::new("hms")
            .address(address)
            // Framed thrift rpc is not enabled by default, we use buffered instead.
            .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
            // TODO: Disable connection pool now to avoid cross runtime issues.
            .pool_config(pool::Config::new(0, Duration::NANOSECOND))
            .build();

        Ok(HiveCatalog {
            info,
            sp,
            client_address,
            client,
        })
    }

    #[async_backtrace::framed]
    pub async fn get_partitions(
        &self,
        db: String,
        table: String,
        partition_names: Vec<String>,
    ) -> Result<Vec<Partition>> {
        self.client
            .get_partitions_by_names(
                FastStr::new(db),
                FastStr::new(table),
                partition_names.into_iter().map(FastStr::new).collect(),
            )
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)?
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn get_partition_names(
        &self,
        db: String,
        table: String,
        max_parts: i16,
    ) -> Result<Vec<String>> {
        let partition_names = self
            .client
            .get_partition_names(FastStr::new(db), FastStr::new(table), max_parts)
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        Ok(partition_names.into_iter().map(|v| v.to_string()).collect())
    }

    fn handle_table_meta(table_meta: &hive_metastore::Table) -> Result<()> {
        if let Some(sd) = table_meta.sd.as_ref() {
            if let Some(input_format) = sd.input_format.as_ref() {
                if input_format != "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" {
                    return Err(ErrorCode::Unimplemented(format!(
                        "only support parquet, {} not support",
                        input_format
                    )));
                }
            }
        }

        if let Some(t) = table_meta.table_type.as_ref() {
            if t == "VIRTUAL_VIEW" {
                return Err(ErrorCode::Unimplemented("not support view table"));
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Catalog for HiveCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        self.info.name_ident.catalog_name.clone()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        self.info.clone()
    }

    fn is_external(&self) -> bool {
        true
    }

    fn disable_table_info_refresh(self: Arc<Self>) -> Result<Arc<dyn Catalog>> {
        Ok(self)
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn get_database(&self, _tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        let db = self
            .client
            .get_database(FastStr::new(db_name))
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let hive_database: HiveDatabase = db.into();
        let res: Arc<dyn Database> = Arc::new(hive_database);
        Ok(res)
    }

    async fn list_databases_history(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        // TODO: Implement list_databases_history
        unimplemented!()
    }

    // Get all the databases.
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let db_names = self
            .client
            .get_all_databases()
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let mut dbs = Vec::with_capacity(db_names.len());

        for name in db_names {
            let db = self
                .client
                .get_database(name)
                .await
                .map(from_thrift_exception)
                .map_err(from_thrift_error)??;

            let hive_database: HiveDatabase = db.into();
            let res: Arc<dyn Database> = Arc::new(hive_database);
            dbs.push(res)
        }

        Ok(dbs)
    }

    // Operation with database.
    #[async_backtrace::framed]
    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot create database in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot drop database in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop database in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot rename database in HIVE catalog",
        ))
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let res: Arc<dyn Table> = Arc::new(HiveTable::try_create(table_info.clone())?);
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, _table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get table by id in HIVE catalog",
        ))
    }

    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        _table_ids: &[MetaId],
        _get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get tables name by ids in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_name_by_id(&self, _table_id: MetaId) -> Result<Option<String>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get table name by id in HIVE catalog",
        ))
    }

    async fn get_db_name_by_id(&self, _db_id: MetaId) -> Result<String> {
        Err(ErrorCode::Unimplemented(
            "Cannot get db name by id in HIVE catalog",
        ))
    }

    async fn mget_databases(
        &self,
        tenant: &Tenant,
        db_names: &[DatabaseNameIdent],
    ) -> Result<Vec<Arc<dyn Database>>> {
        let mut dbs = Vec::with_capacity(db_names.len());
        for name in db_names {
            dbs.push(self.get_database(tenant, name.database_name()).await?);
        }
        Ok(dbs)
    }

    async fn mget_database_names_by_ids(
        &self,
        _tenant: &Tenant,
        _db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get dbs name by ids in HIVE catalog",
        ))
    }

    // Get one table by db and table name.
    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn get_table(
        &self,
        _tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let table_meta = match self
            .client
            .get_table(FastStr::new(db_name), FastStr::new(table_name))
            .await
        {
            Ok(MaybeException::Ok(meta)) => meta,
            Ok(MaybeException::Exception(exception)) => {
                return Err(ErrorCode::TableInfoError(format!("{exception:?}")));
            }
            Err(e) => {
                return Err(from_thrift_error(e));
            }
        };

        Self::handle_table_meta(&table_meta)?;

        let fields = self
            .client
            .get_schema(FastStr::new(db_name), FastStr::new(table_name))
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;
        let table_info: TableInfo = super::converters::try_into_table_info(
            self.info.clone(),
            self.sp.clone(),
            table_meta,
            fields,
        )?;
        let res: Arc<dyn Table> = Arc::new(HiveTable::try_create(table_info)?);

        Ok(res)
    }

    async fn mget_tables(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_names: &[String],
    ) -> Result<Vec<Arc<dyn Table>>> {
        let mut tables = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            if let Ok(table) = self.get_table(tenant, db_name, table_name).await {
                tables.push(table);
            }
        }
        Ok(tables)
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn list_tables(&self, _tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let table_names = self
            .client
            .get_all_tables(FastStr::new(db_name))
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;

        let mut tables = Vec::with_capacity(table_names.len());

        for name in table_names {
            let table = self.get_table(_tenant, db_name, name.as_str()).await?;
            tables.push(table)
        }

        Ok(tables)
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn list_tables_names(&self, _tenant: &Tenant, db_name: &str) -> Result<Vec<String>> {
        let table_names = self
            .client
            .get_all_tables(FastStr::new(db_name))
            .await
            .map(from_thrift_exception)
            .map_err(from_thrift_error)??;
        Ok(table_names
            .into_iter()
            .map(|name| name.as_str().to_string())
            .collect())
    }

    async fn get_table_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get table history in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot list table history in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot create table in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot drop table in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop table in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot commit_table_meta in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot rename table in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn swap_table(&self, _req: SwapTableReq) -> Result<SwapTableReply> {
        unimplemented!()
    }

    // Check a db.table is exists or not.
    #[async_backtrace::framed]
    async fn exists_table(&self, tenant: &Tenant, db_name: &str, table_name: &str) -> Result<bool> {
        // TODO refine this
        match self.get_table(tenant, db_name, table_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UNKNOWN_TABLE {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot upsert table option in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot set_table_column_mask_policy in HIVE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn set_table_row_access_policy(
        &self,
        _req: SetTableRowAccessPolicyReq,
    ) -> Result<SetTableRowAccessPolicyReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot set_table_row_access_policy in HIVE catalog",
        ))
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

    #[async_backtrace::framed]
    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_index(&self, _req: CreateIndexReq) -> Result<CreateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_index(&self, _req: DropIndexReq) -> Result<()> {
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

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }

    async fn create_sequence(&self, _req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        unimplemented!()
    }
    async fn get_sequence(
        &self,
        _req: GetSequenceReq,
        _visibility_checker: &Option<GrantObjectVisibilityChecker>,
    ) -> Result<GetSequenceReply> {
        unimplemented!()
    }
    async fn list_sequences(&self, _req: ListSequencesReq) -> Result<ListSequencesReply> {
        unimplemented!()
    }

    async fn get_sequence_next_value(
        &self,
        _req: GetSequenceNextValueReq,
        _visibility_checker: &Option<GrantObjectVisibilityChecker>,
    ) -> Result<GetSequenceNextValueReply> {
        unimplemented!()
    }

    async fn drop_sequence(&self, _req: DropSequenceReq) -> Result<DropSequenceReply> {
        unimplemented!()
    }

    /// Dictionary
    #[async_backtrace::framed]
    async fn create_dictionary(&self, _req: CreateDictionaryReq) -> Result<CreateDictionaryReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_dictionary(&self, _req: UpdateDictionaryReq) -> Result<UpdateDictionaryReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_dictionary(
        &self,
        _dict_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_dictionary(
        &self,
        _req: DictionaryNameIdent,
    ) -> Result<Option<GetDictionaryReply>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_dictionaries(
        &self,
        _req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>> {
        unimplemented!()
    }

    async fn rename_dictionary(&self, _req: RenameDictionaryReq) -> Result<()> {
        unimplemented!()
    }

    async fn get_autoincrement_next_value(
        &self,
        _req: GetAutoIncrementNextValueReq,
    ) -> Result<GetAutoIncrementNextValueReply> {
        unimplemented!()
    }

    fn transform_udtf_as_table_function(
        &self,
        _ctx: &dyn TableContext,
        _table_args: &TableArgs,
        _udtf: UDTFServer,
        _func_name: &str,
    ) -> Result<Arc<dyn TableFunction>> {
        unimplemented!()
    }
}
