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
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_ast::ast::Engine;
use databend_common_catalog::catalog::Catalog;
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
use databend_common_meta_app::schema::PaimonCatalogOption;
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
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_meta_client::types::MetaId;
use databend_meta_client::types::SeqV;
use educe::Educe;
use paimon::CatalogFactory;
use paimon::Options;
use paimon::catalog::Catalog as PaimonInnerCatalog;

use crate::ParsedName;
use crate::error::map_paimon_error;
use crate::error::map_paimon_result;
use crate::error::read_only;
use crate::parse_system_name;
use crate::system::PaimonSystemTable;
use crate::table::PaimonTable;

pub fn table_from_info(table_info: &TableInfo) -> Result<Arc<dyn Table>> {
    match parse_system_name(&table_info.name) {
        ParsedName::System { .. } => PaimonSystemTable::try_create(table_info.clone()),
        _ => PaimonTable::try_create(table_info.clone()).map(|table| table.into()),
    }
}

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct PaimonCatalog {
    info: Arc<CatalogInfo>,
    #[educe(Debug(ignore))]
    catalog_options: HashMap<String, String>,
    #[educe(Debug(ignore))]
    ctl: Arc<dyn PaimonInnerCatalog>,
}

/// CONNECTION option keys with dots (e.g. `s3.region`) must be written as quoted idents
/// (`"s3.region"`). `Identifier::Display` keeps the quotes in the key string; strip them
/// when building the catalog / reopening tables, same as Iceberg `trim_props`.
pub(crate) fn trim_option_key(key: &str) -> &str {
    key.trim_matches('"')
}

pub(crate) fn trim_option_keys(raw: &HashMap<String, String>) -> HashMap<String, String> {
    raw.iter()
        .map(|(k, v)| (trim_option_key(k).to_string(), v.clone()))
        .collect()
}

impl PaimonCatalog {
    pub fn try_create(info: Arc<CatalogInfo>) -> Result<Self> {
        let options = match &info.meta.catalog_option {
            CatalogOption::Paimon(PaimonCatalogOption { options }) => trim_option_keys(options),
            _ => {
                return Err(ErrorCode::Internal(
                    "trying to create paimon catalog from other catalog type".to_string(),
                ));
            }
        };
        let mut paimon_options = Options::new();
        for (key, value) in &options {
            paimon_options.set(key, value.clone());
        }
        let ctl = databend_common_base::runtime::block_on(CatalogFactory::create(paimon_options))
            .map_err(map_paimon_error)?;
        Ok(Self {
            info,
            catalog_options: options,
            ctl,
        })
    }

    pub fn info(&self) -> Arc<CatalogInfo> {
        self.info.clone()
    }

    pub fn catalog_options(&self) -> &HashMap<String, String> {
        &self.catalog_options
    }

    pub fn paimon_catalog(&self) -> Arc<dyn PaimonInnerCatalog> {
        self.ctl.clone()
    }
}

use crate::database::PaimonDatabase;

#[async_trait]
impl Catalog for PaimonCatalog {
    fn name(&self) -> String {
        self.info.name_ident.catalog_name.clone()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        self.info.clone()
    }

    fn support_partition(&self) -> bool {
        false
    }

    fn is_external(&self) -> bool {
        true
    }

    fn disable_table_info_refresh(self: Arc<Self>) -> Result<Arc<dyn Catalog>> {
        Ok(self)
    }

    #[async_backtrace::framed]
    async fn get_database(&self, tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        if !self.exists_database(tenant, db_name).await? {
            return Err(ErrorCode::UnknownDatabase(db_name.to_string()));
        }
        Ok(Arc::new(PaimonDatabase::create(
            self.clone(),
            db_name.to_string(),
        )))
    }

    #[async_backtrace::framed]
    async fn exists_database(&self, _tenant: &Tenant, db_name: &str) -> Result<bool> {
        let db_names = map_paimon_result(self.ctl.list_databases().await)?;
        Ok(db_names.iter().any(|name| name == db_name))
    }

    async fn list_databases_history(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        Err(read_only("list_databases_history"))
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let db_names = map_paimon_result(self.ctl.list_databases().await)?;
        Ok(db_names
            .into_iter()
            .map(|db_name| {
                Arc::new(PaimonDatabase::create(self.clone(), db_name)) as Arc<dyn Database>
            })
            .collect())
    }

    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(read_only("create_database"))
    }

    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        Err(read_only("drop_database"))
    }

    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        Err(read_only("undrop_database"))
    }

    async fn create_index(&self, _req: CreateIndexReq) -> Result<CreateIndexReply> {
        Err(read_only("create_index"))
    }

    async fn drop_index(&self, _req: DropIndexReq) -> Result<()> {
        Err(read_only("drop_index"))
    }

    async fn get_index(&self, _req: GetIndexReq) -> Result<GetIndexReply> {
        Err(read_only("get_index"))
    }

    async fn update_index(&self, _req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        Err(read_only("update_index"))
    }

    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        Err(read_only("rename_database"))
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        table_from_info(table_info)
    }

    async fn get_table_meta_by_id(&self, _table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        Err(read_only("get_table_meta_by_id"))
    }

    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        _table_ids: &[MetaId],
        _get_dropped_table: bool,
    ) -> Result<Vec<Option<String>>> {
        Err(read_only("mget_table_names_by_ids"))
    }

    async fn get_db_name_by_id(&self, _db_id: MetaId) -> Result<String> {
        Err(read_only("get_db_name_by_id"))
    }

    async fn mget_databases(
        &self,
        tenant: &Tenant,
        db_names: &[DatabaseNameIdent],
    ) -> Result<Vec<Arc<dyn Database>>> {
        let mut dbs = Vec::with_capacity(db_names.len());
        for db_name in db_names {
            dbs.push(self.get_database(tenant, db_name.database_name()).await?);
        }
        Ok(dbs)
    }

    async fn mget_database_names_by_ids(
        &self,
        _tenant: &Tenant,
        _db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(read_only("mget_database_names_by_ids"))
    }

    async fn get_table_name_by_id(&self, _table_id: MetaId) -> Result<Option<String>> {
        Err(read_only("get_table_name_by_id"))
    }

    #[async_backtrace::framed]
    async fn get_table(
        &self,
        _tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        self.get_database(&Tenant::new_literal("dummy"), db_name)
            .await?
            .get_table(table_name)
            .await
    }

    async fn mget_tables(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_names: &[String],
    ) -> Result<Vec<Arc<dyn Table>>> {
        self.get_database(tenant, db_name)
            .await?
            .mget_tables(table_names)
            .await
    }

    async fn get_table_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Err(read_only("get_table_history"))
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        self.get_database(tenant, db_name)
            .await?
            .list_tables()
            .await
    }

    #[async_backtrace::framed]
    async fn list_tables_names(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<String>> {
        self.get_database(tenant, db_name)
            .await?
            .list_tables_names()
            .await
    }

    async fn list_tables_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Err(read_only("list_tables_history"))
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(read_only("create_table"))
    }

    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(read_only("drop_table"))
    }

    async fn undrop_table(&self, _req: UndropTableReq) -> Result<()> {
        Err(read_only("undrop_table"))
    }

    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        Err(read_only("commit_table_meta"))
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(read_only("rename_table"))
    }

    async fn swap_table(&self, _req: SwapTableReq) -> Result<SwapTableReply> {
        Err(read_only("swap_table"))
    }

    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(read_only("upsert_table_option"))
    }

    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Err(read_only("set_table_column_mask_policy"))
    }

    async fn set_table_row_access_policy(
        &self,
        _req: SetTableRowAccessPolicyReq,
    ) -> Result<SetTableRowAccessPolicyReply> {
        Err(read_only("set_table_row_access_policy"))
    }

    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<()> {
        Err(read_only("create_table_index"))
    }

    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<()> {
        Err(read_only("drop_table_index"))
    }

    async fn get_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        Err(read_only("get_table_copied_file_info"))
    }

    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        Err(read_only("truncate_table"))
    }

    async fn list_lock_revisions(&self, _req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        Err(read_only("list_lock_revisions"))
    }

    async fn create_lock_revision(&self, _req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        Err(read_only("create_lock_revision"))
    }

    async fn extend_lock_revision(&self, _req: ExtendLockRevReq) -> Result<()> {
        Err(read_only("extend_lock_revision"))
    }

    async fn delete_lock_revision(&self, _req: DeleteLockRevReq) -> Result<()> {
        Err(read_only("delete_lock_revision"))
    }

    async fn list_locks(&self, _req: ListLocksReq) -> Result<Vec<LockInfo>> {
        Err(read_only("list_locks"))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn create_sequence(&self, _req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        Err(read_only("create_sequence"))
    }

    async fn get_sequence(
        &self,
        _req: GetSequenceReq,
        _visibility_checker: &Option<Arc<GrantObjectVisibilityChecker>>,
    ) -> Result<GetSequenceReply> {
        Err(read_only("get_sequence"))
    }

    async fn list_sequences(&self, _req: ListSequencesReq) -> Result<ListSequencesReply> {
        Err(read_only("list_sequences"))
    }

    async fn get_sequence_next_value(
        &self,
        _req: GetSequenceNextValueReq,
        _visibility_checker: &Option<Arc<GrantObjectVisibilityChecker>>,
    ) -> Result<GetSequenceNextValueReply> {
        Err(read_only("get_sequence_next_value"))
    }

    async fn drop_sequence(&self, _req: DropSequenceReq) -> Result<DropSequenceReply> {
        Err(read_only("drop_sequence"))
    }

    async fn create_dictionary(&self, _req: CreateDictionaryReq) -> Result<CreateDictionaryReply> {
        Err(read_only("create_dictionary"))
    }

    async fn drop_dictionary(
        &self,
        _dict_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>> {
        Err(read_only("drop_dictionary"))
    }

    async fn get_dictionary(
        &self,
        _req: DictionaryNameIdent,
    ) -> Result<Option<GetDictionaryReply>> {
        Err(read_only("get_dictionary"))
    }

    async fn list_dictionaries(
        &self,
        _req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>> {
        Err(read_only("list_dictionaries"))
    }

    async fn rename_dictionary(&self, _req: RenameDictionaryReq) -> Result<()> {
        Err(read_only("rename_dictionary"))
    }

    async fn get_autoincrement_next_value(
        &self,
        _req: GetAutoIncrementNextValueReq,
    ) -> Result<GetAutoIncrementNextValueReply> {
        Err(read_only("get_autoincrement_next_value"))
    }

    fn transform_udtf_as_table_function(
        &self,
        _ctx: &dyn TableContext,
        _table_args: &TableArgs,
        _udtf: UDTFServer,
        _func_name: &str,
    ) -> Result<Arc<dyn TableFunction>> {
        Err(read_only("transform_udtf_as_table_function"))
    }

    fn get_table_engines(&self) -> Vec<StorageDescription> {
        vec![PaimonTable::description()]
    }

    fn default_table_engine(&self) -> Engine {
        Engine::Paimon
    }
}
