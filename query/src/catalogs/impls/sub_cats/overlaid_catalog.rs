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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateDatabasePlan;
use common_planners::DropDatabasePlan;

use crate::catalogs::Catalog;
use crate::catalogs::Database;
use crate::catalogs::DatabaseEngine;
use crate::catalogs::TableFunctionMeta;
use crate::catalogs::TableMeta;
use crate::datasources::engines::database_factory_registry::EngineDescription;

/// Combine two catalogs together
/// - read/search like operations are always performed at
///   upper layer first, and bottom layer later(if necessary)  
/// - metadata are written to the bottom layer
pub struct OverlaidCatalog {
    /// the upper layer, read only
    read_only: Arc<dyn Catalog + Send + Sync>,
    /// bottom layer, writing goes here
    bottom: Arc<dyn Catalog + Send + Sync>,
}

impl OverlaidCatalog {
    pub fn create(
        upper_read_only: Arc<dyn Catalog + Send + Sync>,
        bottom: Arc<dyn Catalog + Send + Sync>,
    ) -> Self {
        Self {
            read_only: upper_read_only,
            bottom,
        }
    }
}

impl Catalog for OverlaidCatalog {
    fn register_db_engine(
        &self,
        _engine_type: &str,
        _database_engine: Arc<dyn DatabaseEngine>,
    ) -> common_exception::Result<()> {
        todo!()
    }

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

    fn exists_database(&self, db_name: &str) -> common_exception::Result<bool> {
        if !self.read_only.exists_database(db_name)? {
            self.bottom.exists_database(db_name)
        } else {
            Ok(true)
        }
    }

    fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<TableMeta>> {
        if self.read_only.exists_database(db_name)? {
            self.read_only.get_table(db_name, table_name)
        } else {
            self.bottom.get_table(db_name, table_name)
        }
    }

    fn get_table_by_id(
        &self,
        db_name: &str,
        table_id: MetaId,
        table_version: Option<MetaVersion>,
    ) -> common_exception::Result<Arc<TableMeta>> {
        self.read_only
            .get_table_by_id(db_name, table_id, table_version)
            .or_else(|_e| {
                self.bottom
                    .get_table_by_id(db_name, table_id, table_version)
            })
    }

    fn get_table_function(
        &self,
        func_name: &str,
    ) -> common_exception::Result<Arc<TableFunctionMeta>> {
        self.read_only
            .get_table_function(func_name)
            .or_else(|_e| self.bottom.get_table_function(func_name))
    }

    fn create_database(&self, plan: CreateDatabasePlan) -> common_exception::Result<()> {
        // create db in BOTTOM layer only
        self.bottom.create_database(plan)
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> common_exception::Result<()> {
        // drop db in BOTTOM layer only
        self.bottom.drop_database(plan)
    }

    fn get_db_engines(&self) -> common_exception::Result<Vec<EngineDescription>> {
        let mut dbs = self.read_only.get_db_engines()?;
        let mut other = self.bottom.get_db_engines()?;
        dbs.append(&mut other);
        Ok(dbs)
    }
}
