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
//

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::catalogs::Catalog;
use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::catalogs::TableFunction;
use crate::datasources::table_func_engine::TableArgs;
use crate::datasources::table_func_engine::TableFuncEngine;
use crate::datasources::table_func_engine_registry::TableFuncEngineRegistry;

/// Combine two catalogs together
/// - read/search like operations are always performed at
///   upper layer first, and bottom layer later(if necessary)  
/// - metadata are written to the bottom layer
pub struct OverlaidCatalog {
    /// the upper layer, read only
    read_only: Arc<dyn Catalog + Send + Sync>,
    /// bottom layer, writing goes here
    bottom: Arc<dyn Catalog + Send + Sync>,
    /// table function engine factories
    func_engine_registry: TableFuncEngineRegistry,
}

impl OverlaidCatalog {
    pub fn create(
        upper_read_only: Arc<dyn Catalog + Send + Sync>,
        bottom: Arc<dyn Catalog + Send + Sync>,
        func_engine_registry: HashMap<String, (u64, Arc<dyn TableFuncEngine>)>,
    ) -> Self {
        Self {
            read_only: upper_read_only,
            bottom,
            func_engine_registry,
        }
    }
}

#[async_trait::async_trait]
impl Catalog for OverlaidCatalog {
    async fn get_databases(&self) -> common_exception::Result<Vec<Arc<dyn Database>>> {
        let mut dbs = self.read_only.get_databases().await?;
        let mut other = self.bottom.get_databases().await?;
        dbs.append(&mut other);
        Ok(dbs)
    }

    async fn get_database(&self, db_name: &str) -> common_exception::Result<Arc<dyn Database>> {
        let r = self.read_only.get_database(db_name).await;
        match r {
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.get_database(db_name).await
                } else {
                    Err(e)
                }
            }
            Ok(db) => Ok(db),
        }
    }

    async fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>> {
        let res = self.read_only.get_table(db_name, table_name).await;
        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.get_table(db_name, table_name).await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn get_tables(&self, db_name: &str) -> common_exception::Result<Vec<Arc<dyn Table>>> {
        let r = self.read_only.get_tables(db_name).await;
        match r {
            Ok(x) => Ok(x),
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.get_tables(db_name).await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn get_table_meta_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        let res = self.read_only.get_table_meta_by_id(table_id).await;

        if let Ok(x) = res {
            Ok(x)
        } else {
            self.bottom.get_table_meta_by_id(table_id).await
        }
    }

    async fn create_table(&self, plan: CreateTablePlan) -> common_exception::Result<()> {
        self.bottom.create_table(plan).await
    }

    async fn drop_table(&self, plan: DropTablePlan) -> common_exception::Result<()> {
        self.bottom.drop_table(plan).await
    }

    fn build_table(&self, table_info: &TableInfo) -> common_exception::Result<Arc<dyn Table>> {
        let res = self.read_only.build_table(table_info);
        match res {
            Ok(t) => Ok(t),
            Err(e) => {
                if e.code() == ErrorCode::UnknownTable("").code() {
                    self.bottom.build_table(table_info)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn get_table_function(
        &self,
        func_name: &str,
        tbl_args: TableArgs,
    ) -> common_exception::Result<Arc<dyn TableFunction>> {
        let (id, factory) = self.func_engine_registry.get(func_name).ok_or_else(|| {
            ErrorCode::UnknownTable(format!("unknown table function {}", func_name))
        })?;

        // table function belongs to no/every database
        let func = factory.try_create("", func_name, *id, tbl_args)?;
        Ok(func)
    }

    async fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        table_option_key: String,
        table_option_value: String,
    ) -> common_exception::Result<UpsertTableOptionReply> {
        // upsert table option in BOTTOM layer only
        self.bottom
            .upsert_table_option(
                table_id,
                table_version,
                table_option_key,
                table_option_value,
            )
            .await
    }

    async fn create_database(
        &self,
        plan: CreateDatabasePlan,
    ) -> common_exception::Result<CreateDatabaseReply> {
        // create db in BOTTOM layer only
        self.bottom.create_database(plan).await
    }

    async fn drop_database(&self, plan: DropDatabasePlan) -> common_exception::Result<()> {
        // drop db in BOTTOM layer only
        self.bottom.drop_database(plan).await
    }
}
