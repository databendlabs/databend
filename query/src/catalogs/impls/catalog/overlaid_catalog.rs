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
use common_meta_types::TableInfo;
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

impl Catalog for OverlaidCatalog {
    fn get_databases(&self) -> common_exception::Result<Vec<Arc<dyn Database>>> {
        let mut dbs = self.read_only.get_databases()?;
        let mut other = self.bottom.get_databases()?;
        dbs.append(&mut other);
        Ok(dbs)
    }

    fn get_database(&self, db_name: &str) -> common_exception::Result<Arc<dyn Database>> {
        let r = self.read_only.get_database(db_name);
        match r {
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.get_database(db_name)
                } else {
                    Err(e)
                }
            }
            Ok(db) => Ok(db),
        }
    }

    fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>> {
        let res = self.read_only.get_table(db_name, table_name);
        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.get_table(db_name, table_name)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn get_tables(&self, db_name: &str) -> common_exception::Result<Vec<Arc<dyn Table>>> {
        let r = self.read_only.get_tables(db_name);
        match r {
            Ok(x) => Ok(x),
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.get_tables(db_name)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn create_table(&self, plan: CreateTablePlan) -> common_exception::Result<()> {
        self.bottom.create_table(plan)
    }

    fn drop_table(&self, plan: DropTablePlan) -> common_exception::Result<()> {
        self.bottom.drop_table(plan)
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

    fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        table_option_key: String,
        table_option_value: String,
    ) -> common_exception::Result<UpsertTableOptionReply> {
        // upsert table option in BOTTOM layer only
        self.bottom.upsert_table_option(
            table_id,
            table_version,
            table_option_key,
            table_option_value,
        )
    }

    fn create_database(
        &self,
        plan: CreateDatabasePlan,
    ) -> common_exception::Result<CreateDatabaseReply> {
        // create db in BOTTOM layer only
        self.bottom.create_database(plan)
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> common_exception::Result<()> {
        // drop db in BOTTOM layer only
        self.bottom.drop_database(plan)
    }
}
