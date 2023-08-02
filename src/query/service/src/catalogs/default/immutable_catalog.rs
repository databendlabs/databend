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
use std::fmt::Formatter;
use std::sync::Arc;

use common_catalog::catalog::Catalog;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateIndexReply;
use common_meta_app::schema::CreateIndexReq;
use common_meta_app::schema::CreateTableLockRevReply;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::CreateVirtualColumnReply;
use common_meta_app::schema::CreateVirtualColumnReq;
use common_meta_app::schema::DropDatabaseReply;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropIndexReply;
use common_meta_app::schema::DropIndexReq;
use common_meta_app::schema::DropTableByIdReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropVirtualColumnReply;
use common_meta_app::schema::DropVirtualColumnReq;
use common_meta_app::schema::GetIndexReply;
use common_meta_app::schema::GetIndexReq;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::ListIndexesByIdReq;
use common_meta_app::schema::ListIndexesReq;
use common_meta_app::schema::ListVirtualColumnsReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::SetTableColumnMaskPolicyReply;
use common_meta_app::schema::SetTableColumnMaskPolicyReq;
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

use crate::catalogs::InMemoryMetas;
use crate::catalogs::SYS_DB_ID_BEGIN;
use crate::catalogs::SYS_TBL_ID_BEGIN;
use crate::databases::Database;
use crate::databases::InformationSchemaDatabase;
use crate::databases::SystemDatabase;
use crate::storages::Table;

/// System Catalog contains ... all the system databases (no surprise :)
#[derive(Clone)]
pub struct ImmutableCatalog {
    // it's case sensitive, so we will need two same database only with the name's case
    info_schema_db: Arc<InformationSchemaDatabase>,
    sys_db: Arc<SystemDatabase>,
    sys_db_meta: Arc<InMemoryMetas>,
}

impl Debug for ImmutableCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImmutableCatalog").finish_non_exhaustive()
    }
}

impl ImmutableCatalog {
    #[async_backtrace::framed]
    pub async fn try_create_with_config(conf: &InnerConfig) -> Result<Self> {
        // The global db meta.
        let mut sys_db_meta = InMemoryMetas::create(SYS_DB_ID_BEGIN, SYS_TBL_ID_BEGIN);
        sys_db_meta.init_db("system");
        sys_db_meta.init_db("information_schema");

        let sys_db = SystemDatabase::create(&mut sys_db_meta, conf);
        let info_schema_db = InformationSchemaDatabase::create(&mut sys_db_meta);

        Ok(Self {
            info_schema_db: Arc::new(info_schema_db),
            sys_db: Arc::new(sys_db),
            sys_db_meta: Arc::new(sys_db_meta),
        })
    }
}

#[async_trait::async_trait]
impl Catalog for ImmutableCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        "default".to_string()
    }

    fn info(&self) -> CatalogInfo {
        CatalogInfo::new_default()
    }

    #[async_backtrace::framed]
    async fn get_database(&self, _tenant: &str, db_name: &str) -> Result<Arc<dyn Database>> {
        match db_name {
            "system" => Ok(self.sys_db.clone()),
            "information_schema" => Ok(self.info_schema_db.clone()),
            _ => Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database {}",
                db_name
            ))),
        }
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, _tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        Ok(vec![self.sys_db.clone(), self.info_schema_db.clone()])
    }

    #[async_backtrace::framed]
    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::Unimplemented("Cannot create system database"))
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        Err(ErrorCode::Unimplemented("Cannot drop system database"))
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        Err(ErrorCode::Unimplemented("Cannot rename system database"))
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let table_id = table_info.ident.table_id;

        let table = self
            .sys_db_meta
            .get_by_id(&table_id)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id)))?;
        Ok(table.clone())
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        let table = self
            .sys_db_meta
            .get_by_id(&table_id)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id)))?;
        let ti = table.get_table_info();
        Ok((ti.ident, Arc::new(ti.meta.clone())))
    }

    #[async_backtrace::framed]
    async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let _db = self.get_database(tenant, db_name).await?;

        self.sys_db_meta.get_by_name(db_name, table_name)
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, _tenant: &str, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        self.sys_db_meta.get_all_tables(db_name)
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        self.list_tables(tenant, db_name).await
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot create table in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot drop table in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop table in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop database in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot rename table in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn count_tables(&self, _req: CountTablesReq) -> Result<CountTablesReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot count tables in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        _tenant: &str,
        _db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        Err(ErrorCode::Unimplemented(format!(
            "get_table_copied_file_info not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        Err(ErrorCode::Unimplemented(format!(
            "truncate_table not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _tenant: &str,
        _db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::Unimplemented(format!(
            "upsert table option not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn update_table_meta(
        &self,
        _table_info: &TableInfo,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply> {
        Err(ErrorCode::Unimplemented(format!(
            "update table meta not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Err(ErrorCode::Unimplemented(format!(
            "set_table_column_mask_policy not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn list_table_lock_revs(&self, _table_id: u64) -> Result<Vec<u64>> {
        Err(ErrorCode::Unimplemented(
            "list_table_lock_revs not allowed for system database",
        ))
    }

    #[async_backtrace::framed]
    async fn create_table_lock_rev(
        &self,
        _expire_sec: u64,
        _table_info: &TableInfo,
    ) -> Result<CreateTableLockRevReply> {
        Err(ErrorCode::Unimplemented(
            "create_table_lock_rev not allowed for system database",
        ))
    }

    #[async_backtrace::framed]
    async fn extend_table_lock_rev(
        &self,
        _expire_sec: u64,
        _table_info: &TableInfo,
        _revision: u64,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "extend_table_lock_rev not allowed for system database",
        ))
    }

    #[async_backtrace::framed]
    async fn delete_table_lock_rev(&self, _table_info: &TableInfo, _revision: u64) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "delete_table_lock_rev not allowed for system database",
        ))
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
    async fn list_indexes_by_table_id(&self, _req: ListIndexesByIdReq) -> Result<Vec<u64>> {
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
}
