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
use databend_common_ast::ast::Engine;
use databend_common_base::http_client::HttpClient;
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
use databend_common_meta_app::KeyWithTenant;
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
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseId;
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
use databend_common_meta_app::schema::IcebergCatalogOption;
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
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_meta_types::MetaId;
use databend_meta_types::SeqV;
use educe::Educe;
use iceberg::CatalogBuilder;
use iceberg::NamespaceIdent;
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_hms::HmsCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;

use crate::IcebergTable;
use crate::database::IcebergDatabase;

pub const ICEBERG_CATALOG: &str = "iceberg";

#[derive(Debug)]
pub struct IcebergMutableCreator;

impl CatalogCreator for IcebergMutableCreator {
    fn try_create(&self, info: Arc<CatalogInfo>) -> Result<Arc<dyn Catalog>> {
        let catalog: Arc<dyn Catalog> = Arc::new(IcebergMutableCatalog::try_create(info)?);
        Ok(catalog)
    }
}

/// `Catalog` for a external iceberg storage
///
/// - Metadata of databases are saved in meta store
/// - Instances of `Database` are created from reading subdirectories of
///   Iceberg table
/// - Table metadata are saved in external Iceberg storage
#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct IcebergMutableCatalog {
    /// info of this iceberg table.
    info: Arc<CatalogInfo>,

    /// iceberg catalogs
    ctl: Arc<dyn iceberg::Catalog>,
}

impl IcebergMutableCatalog {
    /// create a new iceberg catalog from the endpoint_address
    #[fastrace::trace]
    pub fn try_create(info: Arc<CatalogInfo>) -> Result<Self> {
        let opt = match &info.meta.catalog_option {
            CatalogOption::Iceberg(opt) => opt,
            _ => unreachable!(
                "trying to create iceberg catalog from other catalog, must be an internal bug"
            ),
        };

        // Trick
        //
        // Our parser doesn't allow users to write `s3.region` in options. Instead, users must use
        // `"s3.region"`, but it's stored as is. We need to remove the quotes here.
        //
        // We only do this while building catalog so this won't affect existing catalogs.
        let ctl: Arc<dyn iceberg::Catalog> = match opt {
            IcebergCatalogOption::Hms(hms) => {
                let mut props: std::collections::HashMap<String, String> = hms
                    .props
                    .iter()
                    .map(|(k, v)| (k.trim_matches('"').to_string(), v.clone()))
                    .collect();
                props.insert("uri".to_string(), hms.address.clone());
                props.insert("warehouse".to_string(), hms.warehouse.clone());

                let ctl = databend_common_base::runtime::block_on(
                    HmsCatalogBuilder::default().load(&info.name_ident.catalog_name, props),
                )
                .map_err(|err| {
                    ErrorCode::BadArguments(format!("Iceberg build hms catalog failed: {err:?}"))
                })?;
                Arc::new(ctl)
            }
            IcebergCatalogOption::Rest(rest) => {
                let client = HttpClient::default();
                let mut props: std::collections::HashMap<String, String> = rest
                    .props
                    .iter()
                    .map(|(k, v)| (k.trim_matches('"').to_string(), v.clone()))
                    .collect();
                props.insert("uri".to_string(), rest.uri.clone());
                props.insert("warehouse".to_string(), rest.warehouse.clone());

                let ctl = databend_common_base::runtime::block_on(
                    RestCatalogBuilder::default()
                        .with_client(client.inner())
                        .load(&info.name_ident.catalog_name, props),
                )
                .map_err(|err| {
                    ErrorCode::BadArguments(format!("Iceberg build rest catalog failed: {err:?}"))
                })?;
                Arc::new(ctl)
            }
            IcebergCatalogOption::Glue(glue) => {
                let mut props: std::collections::HashMap<String, String> = glue
                    .props
                    .iter()
                    .map(|(k, v)| (k.trim_matches('"').to_string(), v.clone()))
                    .collect();
                props.insert("uri".to_string(), glue.address.clone());
                props.insert("warehouse".to_string(), glue.warehouse.clone());

                let ctl = databend_common_base::runtime::block_on(
                    GlueCatalogBuilder::default().load(&info.name_ident.catalog_name, props),
                )
                .map_err(|err| {
                    ErrorCode::BadArguments(format!(
                        "There was an error building the AWS Glue catalog: {err:?}"
                    ))
                })?;
                Arc::new(ctl)
            }
            IcebergCatalogOption::Storage(s) => {
                let mut props: std::collections::HashMap<String, String> = s
                    .props
                    .iter()
                    .map(|(k, v)| (k.trim_matches('"').to_string(), v.clone()))
                    .collect();
                props.insert("endpoint_url".to_string(), s.address.clone());
                props.insert("table_bucket_arn".to_string(), s.table_bucket_arn.clone());

                let ctl = databend_common_base::runtime::block_on(
                    S3TablesCatalogBuilder::default().load(&info.name_ident.catalog_name, props),
                )
                .map_err(|err| {
                    ErrorCode::BadArguments(format!(
                        "There was an error building the s3 tables catalog: {err:?}"
                    ))
                })?;
                Arc::new(ctl)
            }
        };

        Ok(Self { info, ctl })
    }

    /// Get the iceberg catalog.
    pub fn iceberg_catalog(&self) -> Arc<dyn iceberg::Catalog> {
        self.ctl.clone()
    }
}

#[async_trait]
impl Catalog for IcebergMutableCatalog {
    fn name(&self) -> String {
        self.info.name_ident.catalog_name.clone()
    }
    fn info(&self) -> Arc<CatalogInfo> {
        self.info.clone()
    }

    fn support_partition(&self) -> bool {
        true
    }

    fn is_external(&self) -> bool {
        true
    }

    fn disable_table_info_refresh(self: Arc<Self>) -> Result<Arc<dyn Catalog>> {
        Ok(self)
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn get_database(&self, tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        let c = self.exists_database(tenant, db_name).await?;
        if !c {
            return Err(ErrorCode::UnknownDatabase(db_name.to_string()));
        }
        Ok(Arc::new(IcebergDatabase::create(self.clone(), db_name)))
    }

    async fn list_databases_history(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let db_names = self
            .iceberg_catalog()
            .list_namespaces(None)
            .await
            .map_err(|err| {
                ErrorCode::Internal(format!("Iceberg catalog load database failed: {err:?}"))
            })?;
        let mut dbs = Vec::new();
        for db_name in db_names {
            let db = Arc::new(IcebergDatabase::create(
                self.clone(),
                &db_name.to_url_string(),
            )) as Arc<dyn Database>;
            dbs.push(db);
        }
        Ok(dbs)
    }

    #[async_backtrace::framed]
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        match req.create_option {
            CreateOption::Create => {}
            CreateOption::CreateIfNotExists => {
                if self
                    .exists_database(req.name_ident.tenant(), req.name_ident.name())
                    .await?
                {
                    return Ok(CreateDatabaseReply {
                        db_id: DatabaseId::new(0),
                    });
                }
            }
            CreateOption::CreateOrReplace => {
                self.drop_database(DropDatabaseReq {
                    if_exists: true,
                    name_ident: req.name_ident.clone(),
                })
                .await?;
            }
        }

        let ns = NamespaceIdent::new(req.name_ident.name().to_owned());
        let _ = self
            .iceberg_catalog()
            .create_namespace(&ns, Default::default())
            .await
            .map_err(|err| {
                ErrorCode::Internal(format!(
                    "Iceberg create database {} failed: {err:?}",
                    req.name_ident.name()
                ))
            })?;

        Ok(CreateDatabaseReply {
            db_id: DatabaseId::new(0),
        })
    }

    #[async_backtrace::framed]
    async fn exists_database(&self, _tenant: &Tenant, db_name: &str) -> Result<bool> {
        let db_names = self
            .iceberg_catalog()
            .list_namespaces(None)
            .await
            .map_err(|err| {
                ErrorCode::Internal(format!("Iceberg catalog load database failed: {err:?}"))
            })?;
        Ok(db_names.iter().any(|name| name.to_url_string() == db_name))
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        let ns = NamespaceIdent::new(req.name_ident.name().to_owned());
        if req.if_exists
            && !self
                .exists_database(req.name_ident.tenant(), req.name_ident.name())
                .await?
        {
            return Ok(DropDatabaseReply { db_id: 0 });
        }

        let _ = self
            .iceberg_catalog()
            .drop_namespace(&ns)
            .await
            .map_err(|err| {
                ErrorCode::Internal(format!(
                    "Iceberg drop database {} failed: {err:?}",
                    req.name_ident.name()
                ))
            })?;
        Ok(DropDatabaseReply { db_id: 0 })
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
        let table = IcebergTable::try_create(table_info.clone())?;
        Ok(table.into())
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, _table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        unimplemented!()
    }

    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        _table_ids: &[MetaId],
        _get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get tables name by ids in ICEBERG catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_name_by_id(&self, _table_id: MetaId) -> Result<Option<String>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get table name by id in ICEBERG catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_db_name_by_id(&self, _table_id: MetaId) -> Result<String> {
        Err(ErrorCode::Unimplemented(
            "Cannot get db name by id in ICEBERG catalog",
        ))
    }
    async fn mget_databases(
        &self,
        _tenant: &Tenant,
        db_names: &[DatabaseNameIdent],
    ) -> Result<Vec<Arc<dyn Database>>> {
        Ok(db_names
            .iter()
            .map(|db_name| {
                Arc::new(IcebergDatabase::create(
                    self.clone(),
                    db_name.database_name(),
                )) as Arc<dyn Database>
            })
            .collect())
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

    #[fastrace::trace]
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

    async fn mget_tables(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_names: &[String],
    ) -> Result<Vec<Arc<dyn Table>>> {
        let db = self.get_database(tenant, db_name).await?;
        db.mget_tables(table_names).await
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let db = self.get_database(tenant, db_name).await?;
        db.list_tables().await
    }

    #[async_backtrace::framed]
    async fn list_tables_names(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<String>> {
        let db = self.get_database(tenant, db_name).await?;
        db.list_tables_names().await
    }

    async fn get_table_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        unimplemented!()
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
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        let db = self
            .get_database(&req.name_ident.tenant, &req.name_ident.db_name)
            .await?;
        db.create_table(req).await
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        let db = self.get_database(&req.tenant, &req.db_name).await?;
        db.drop_table_by_id(req).await
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        if req.if_exists
            && !self
                .exists_table(req.tenant(), req.db_name(), req.table_name())
                .await?
        {
            return Ok(RenameTableReply { table_id: 0 });
        }

        let src = NamespaceIdent::new(req.db_name().to_owned());
        let src_table = iceberg::TableIdent::new(src, req.table_name().to_owned());

        let dst = NamespaceIdent::new(req.new_db_name.clone());
        let dst_table = iceberg::TableIdent::new(dst, req.new_table_name.clone());
        self.iceberg_catalog()
            .rename_table(&src_table, &dst_table)
            .await
            .map_err(|err| {
                ErrorCode::BadArguments(format!("Iceberg rename table failed: {err:?}"))
            })?;
        return Ok(RenameTableReply { table_id: 0 });
    }

    #[async_backtrace::framed]
    async fn swap_table(&self, _req: SwapTableReq) -> Result<SwapTableReply> {
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
    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn set_table_row_access_policy(
        &self,
        _req: SetTableRowAccessPolicyReq,
    ) -> Result<SetTableRowAccessPolicyReply> {
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
        vec![]
    }

    fn default_table_engine(&self) -> Engine {
        Engine::Iceberg
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
