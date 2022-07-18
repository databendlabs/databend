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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::catalogs::Catalog;
use crate::catalogs::CATALOG_DEFAULT;
use crate::sessions::query_ctx::TableContext;
use crate::storages::system::table::AsyncOneBlockSystemTable;
use crate::storages::system::table::AsyncSystemTable;
use crate::storages::Table;

pub struct TablesTable<const WITH_HISTROY: bool> {
    table_info: TableInfo,
}

pub type TablesTableWithHistory = TablesTable<true>;
pub type TablesTableWithoutHistory = TablesTable<false>;

#[async_trait::async_trait]
pub trait HistoryAware {
    const TABLE_NAME: &'static str;
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>>;
}

#[async_trait::async_trait]
impl HistoryAware for TablesTable<true> {
    const TABLE_NAME: &'static str = "tables_with_history";
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        database_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        catalog.list_tables_history(tenant, database_name).await
    }
}

#[async_trait::async_trait]
impl HistoryAware for TablesTable<false> {
    const TABLE_NAME: &'static str = "tables";
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        database_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        catalog.list_tables(tenant, database_name).await
    }
}

#[async_trait::async_trait]
impl<const T: bool> AsyncSystemTable for TablesTable<T>
where TablesTable<T>: HistoryAware
{
    const NAME: &'static str = Self::TABLE_NAME;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(CATALOG_DEFAULT)?;
        let databases = catalog.list_databases(tenant.as_str()).await?;

        let mut database_tables = vec![];
        for database in databases {
            let name = database.name();
            let tables = Self::list_tables(&catalog, tenant.as_str(), name).await?;
            for table in tables {
                database_tables.push((name.to_string(), table));
            }
        }

        let mut num_rows: Vec<Option<u64>> = Vec::new();
        let mut data_size: Vec<Option<u64>> = Vec::new();
        let mut data_compressed_size: Vec<Option<u64>> = Vec::new();
        let mut index_size: Vec<Option<u64>> = Vec::new();

        for (_, tbl) in &database_tables {
            let stats = tbl.statistics(ctx.clone()).await?;
            num_rows.push(stats.as_ref().and_then(|v| v.num_rows));
            data_size.push(stats.as_ref().and_then(|v| v.data_size));
            data_compressed_size.push(stats.and_then(|v| v.data_size_compressed));
            index_size.push(None);
        }

        let databases: Vec<&[u8]> = database_tables.iter().map(|(d, _)| d.as_bytes()).collect();
        let names: Vec<&[u8]> = database_tables
            .iter()
            .map(|(_, v)| v.name().as_bytes())
            .collect();
        let engines: Vec<&[u8]> = database_tables
            .iter()
            .map(|(_, v)| v.engine().as_bytes())
            .collect();
        let created_ons: Vec<String> = database_tables
            .iter()
            .map(|(_, v)| {
                v.get_table_info()
                    .meta
                    .created_on
                    .format("%Y-%m-%d %H:%M:%S.%3f %z")
                    .to_string()
            })
            .collect();
        let dropped_ons: Vec<String> = database_tables
            .iter()
            .map(|(_, v)| {
                v.get_table_info()
                    .meta
                    .drop_on
                    .map(|v| v.format("%Y-%m-%d %H:%M:%S.%3f %z").to_string())
                    .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        let created_ons: Vec<&[u8]> = created_ons.iter().map(|s| s.as_bytes()).collect();
        let cluster_bys: Vec<String> = database_tables
            .iter()
            .map(|(_, v)| {
                v.get_table_info()
                    .meta
                    .default_cluster_key
                    .clone()
                    .unwrap_or_else(|| "".to_owned())
            })
            .collect();

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(databases),
            Series::from_data(names),
            Series::from_data(engines),
            Series::from_data(cluster_bys),
            Series::from_data(created_ons),
            Series::from_data(dropped_ons),
            Series::from_data(num_rows),
            Series::from_data(data_size),
            Series::from_data(data_compressed_size),
            Series::from_data(index_size),
        ]))
    }
}

impl<const T: bool> TablesTable<T>
where TablesTable<T>: HistoryAware
{
    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("database", Vu8::to_data_type()),
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("engine", Vu8::to_data_type()),
            DataField::new("cluster_by", Vu8::to_data_type()),
            DataField::new("created_on", Vu8::to_data_type()),
            DataField::new("dropped_on", Vu8::to_data_type()),
            DataField::new_nullable("num_rows", u64::to_data_type()),
            DataField::new_nullable("data_size", u64::to_data_type()),
            DataField::new_nullable("data_compressed_size", u64::to_data_type()),
            DataField::new_nullable("index_size", u64::to_data_type()),
        ])
    }

    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let name = Self::TABLE_NAME;
        let table_info = TableInfo {
            desc: format!("'system'.'{name}'"),
            name: Self::NAME.to_owned(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: TablesTable::<T>::schema(),
                engine: "SystemTables".to_string(),

                ..Default::default()
            },
        };

        AsyncOneBlockSystemTable::create(TablesTable::<T> { table_info })
    }
}
