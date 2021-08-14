// Copyright 2020 Datafuse Labs.
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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::CreateTablePlan;
use common_planners::DropTablePlan;
use common_planners::TableEngineType;

use crate::catalogs::impls::database_catalog::LOCAL_TBL_ID_BEGIN;
use crate::catalogs::utils::InMemoryMetas;
use crate::catalogs::utils::TableFunctionMeta;
use crate::catalogs::utils::TableMeta;
use crate::datasources::local::CsvTable;
use crate::datasources::local::MemoryTable;
use crate::datasources::local::NullTable;
use crate::datasources::local::ParquetTable;
use crate::datasources::Database;

pub struct LocalDatabase {
    tables: RwLock<InMemoryMetas>,
    tbl_id_seq: AtomicU64,
}

impl LocalDatabase {
    pub fn create() -> Self {
        LocalDatabase {
            tables: RwLock::new(InMemoryMetas::new()),
            tbl_id_seq: AtomicU64::new(LOCAL_TBL_ID_BEGIN),
        }
    }
    fn next_db_id(&self) -> u64 {
        // `fetch_add` wraps around on overflow, but as LOCAL_TBL_ID_BEGIN
        // is defined as (1 << 62) + 10000, there are about 13 quintillion ids are reserved
        // for local tables, we do not check overflow here.
        self.tbl_id_seq.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Database for LocalDatabase {
    fn name(&self) -> &str {
        "local"
    }

    fn engine(&self) -> &str {
        "local"
    }

    fn is_local(&self) -> bool {
        true
    }

    fn get_table(&self, table_name: &str) -> Result<Arc<TableMeta>> {
        let tables = self.tables.read();
        let table = tables
            .name2meta
            .get(table_name)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table: '{}'", table_name)))?;
        Ok(table.clone())
    }

    fn get_table_by_id(
        &self,
        table_id: MetaId,
        _table_version: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        let tables = self.tables.read();
        let table = tables
            .id2meta
            .get(&table_id)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id)))?;
        Ok(table.clone())
    }

    fn get_tables(&self) -> Result<Vec<Arc<TableMeta>>> {
        Ok(self.tables.read().name2meta.values().cloned().collect())
    }

    fn get_table_functions(&self) -> Result<Vec<Arc<TableFunctionMeta>>> {
        Ok(vec![])
    }

    async fn create_table(&self, plan: CreateTablePlan) -> Result<()> {
        let clone = plan.clone();
        let db_name = clone.db.as_str();
        let table_name = clone.table.as_str();
        if self.tables.read().name2meta.get(table_name).is_some() {
            return if plan.if_not_exists {
                Ok(())
            } else {
                return Err(ErrorCode::UnImplement(format!(
                    "Table: '{}.{}' already exists.",
                    db_name, table_name,
                )));
            };
        }

        let table = match &plan.engine {
            TableEngineType::Parquet => {
                ParquetTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            TableEngineType::Csv => {
                CsvTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            TableEngineType::Null => {
                NullTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            TableEngineType::Memory => {
                MemoryTable::try_create(plan.db, plan.table, plan.schema, plan.options)?
            }
            _ => {
                return Result::Err(ErrorCode::UnImplement(format!(
                    "Local database does not support '{:?}' table engine",
                    plan.engine
                )));
            }
        };

        let mut tables = self.tables.write();
        let table_meta = TableMeta::new(Arc::from(table), self.next_db_id());
        tables.insert(table_meta);
        Ok(())
    }

    async fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let table_name = plan.table.as_str();
        let tbl_id = {
            let tables = self.tables.read();
            let by_name = tables.name2meta.get(table_name);
            match by_name {
                None => {
                    if plan.if_exists {
                        return Ok(());
                    } else {
                        return Err(ErrorCode::UnknownTable(format!(
                            "Unknown table: '{}.{}'",
                            plan.db, plan.table
                        )));
                    }
                }
                Some(tbl) => tbl.meta_id(),
            }
        };

        let mut tables = self.tables.write();
        tables.name2meta.remove(table_name);
        tables.id2meta.remove(&tbl_id);
        Ok(())
    }
}
