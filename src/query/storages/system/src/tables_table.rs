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

use common_catalog::catalog::Catalog;
use common_catalog::catalog::CatalogManager;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

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
        let catalog_mgr = CatalogManager::instance();
        let ctls: Vec<(String, Arc<dyn Catalog>)> = catalog_mgr
            .catalogs
            .iter()
            .map(|e| (e.key().to_string(), e.value().clone()))
            .collect();
        let mut catalogs = MutableStringColumn::default(); // capacity unknown
        let mut databases = MutableStringColumn::default();
        let mut names = MutableStringColumn::default();
        let mut engines = MutableStringColumn::default();
        let mut cluster_bys = vec![];
        let mut created_ons = vec![];
        let mut dropped_ons = vec![];
        let mut num_rows: Vec<Option<u64>> = Vec::new();
        let mut data_size: Vec<Option<u64>> = Vec::new();
        let mut data_compressed_size: Vec<Option<u64>> = Vec::new();
        let mut index_size: Vec<Option<u64>> = Vec::new();

        for (ctl_name, ctl) in ctls.into_iter() {
            let dbs = ctl.list_databases(tenant.as_str()).await?;
            let ctl_name: &str = Box::leak(ctl_name.into_boxed_str());

            let mut database_tables = vec![];
            for database in dbs {
                let name = database.name().to_string().into_boxed_str();
                let name: &str = Box::leak(name);
                let tables = Self::list_tables(&ctl, tenant.as_str(), name).await?;
                for table in tables {
                    catalogs.append_value(ctl_name);
                    databases.append_value(name);
                    database_tables.push(table);
                }
            }

            for tbl in &database_tables {
                let stats = tbl.table_statistics()?;
                num_rows.push(stats.as_ref().and_then(|v| v.num_rows));
                data_size.push(stats.as_ref().and_then(|v| v.data_size));
                data_compressed_size.push(stats.as_ref().and_then(|v| v.data_size_compressed));
                index_size.push(stats.and_then(|v| v.index_size));

                names.append_value(tbl.name());
                engines.append_value(tbl.engine());

                let created_on = tbl
                    .get_table_info()
                    .meta
                    .created_on
                    .format("%Y-%m-%d %H:%M:%S.%3f %z")
                    .to_string();
                let dropped_on = tbl
                    .get_table_info()
                    .meta
                    .drop_on
                    .map(|v| v.format("%Y-%m-%d %H:%M:%S.%3f %z").to_string())
                    .unwrap_or_else(|| "NULL".to_owned());
                let cluster_by = tbl
                    .get_table_info()
                    .meta
                    .default_cluster_key
                    .as_ref()
                    .map(|s| s.clone().into_bytes())
                    .unwrap_or_else(Vec::new);
                created_ons.push(created_on);
                dropped_ons.push(dropped_on);
                cluster_bys.push(cluster_by);
            }
        }

        Ok(DataBlock::create(self.table_info.schema(), vec![
            catalogs.to_column(),
            databases.to_column(),
            names.to_column(),
            engines.to_column(),
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
            DataField::new("catalog", Vu8::to_data_type()),
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
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(TablesTable::<T> { table_info })
    }
}
