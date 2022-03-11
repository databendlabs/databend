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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReq;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;

use crate::catalogs::catalog::Catalog;
use crate::catalogs::InMemoryMetas;
use crate::catalogs::SYS_TBL_ID_BEGIN;
use crate::configs::Config;
use crate::databases::Database;
use crate::databases::SystemDatabase;
use crate::storages::Table;

/// System Catalog contains ... all the system databases (no surprise :)
/// Currently, this is only one database here, the "system" db.
/// "information_schema" db is supposed to held here
#[derive(Clone)]
pub struct ImmutableCatalog {
    sys_db: Arc<SystemDatabase>,
    sys_db_meta: Arc<InMemoryMetas>,
}

impl ImmutableCatalog {
    pub async fn try_create_with_config(_conf: &Config) -> Result<Self> {
        let system_table_id = SYS_TBL_ID_BEGIN;

        // The global db meta.
        let mut sys_db_meta = InMemoryMetas::create(system_table_id);
        let sys_db = SystemDatabase::create(&mut sys_db_meta);

        Ok(Self {
            sys_db: Arc::new(sys_db),
            sys_db_meta: Arc::new(sys_db_meta),
        })
    }
}

#[async_trait::async_trait]
impl Catalog for ImmutableCatalog {
    async fn get_database(&self, _tenant: &str, db_name: &str) -> Result<Arc<dyn Database>> {
        if db_name == "system" {
            return Ok(self.sys_db.clone());
        }
        Err(ErrorCode::UnknownDatabase(format!(
            "Unknown database {}",
            db_name
        )))
    }

    async fn list_databases(&self, _tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        Ok(vec![self.sys_db.clone()])
    }

    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::UnImplement("Cannot create system database"))
    }

    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<()> {
        Err(ErrorCode::UnImplement("Cannot drop system database"))
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let table_id = table_info.ident.table_id;

        let table = self
            .sys_db_meta
            .get_by_id(&table_id)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id)))?;
        Ok(table.clone())
    }

    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        let table = self
            .sys_db_meta
            .get_by_id(&table_id)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id)))?;
        let ti = table.get_table_info();
        Ok((ti.ident.clone(), Arc::new(ti.meta.clone())))
    }

    async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let _db = self.get_database(tenant, db_name).await?;

        let table = self
            .sys_db_meta
            .get_by_name(table_name)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name)))?;

        Ok(table.clone())
    }

    async fn list_tables(&self, tenant: &str, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        // ensure db exists
        let _db = self.get_database(tenant, db_name).await?;
        self.sys_db_meta.get_all_tables()
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "Cannot create table in system database",
        ))
    }

    async fn drop_table(&self, _req: DropTableReq) -> Result<DropTableReply> {
        Err(ErrorCode::UnImplement(
            "Cannot drop table in system database",
        ))
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::UnImplement(format!(
            "Commit table not allowed for system database {:?}",
            req
        )))
    }
}
