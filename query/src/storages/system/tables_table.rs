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
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;

use crate::catalogs::CATALOG_DEFAULT;
use crate::sessions::QueryContext;
use crate::storages::system::table::AsyncOneBlockSystemTable;
use crate::storages::system::table::AsyncSystemTable;
use crate::storages::Table;

pub struct TablesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for TablesTable {
    const NAME: &'static str = "system.tables";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        // TODO pass catalog in or embed catalog in table info?
        let catalog = ctx.get_catalog(CATALOG_DEFAULT)?;
        let databases = catalog.list_databases(tenant.as_str()).await?;

        let mut database_tables = vec![];
        for database in databases {
            let name = database.name();
            for table in catalog.list_tables_history(tenant.as_str(), name).await? {
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

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(databases),
            Series::from_data(names),
            Series::from_data(engines),
            Series::from_data(created_ons),
            Series::from_data(dropped_ons),
            Series::from_data(num_rows),
            Series::from_data(data_size),
            Series::from_data(data_compressed_size),
            Series::from_data(index_size),
        ]))
    }
}

impl TablesTable {
    pub fn schema() -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("database", Vu8::to_data_type()),
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("engine", Vu8::to_data_type()),
            DataField::new("created_on", Vu8::to_data_type()),
            DataField::new("dropped_on", Vu8::to_data_type()),
            DataField::new_nullable("num_rows", u64::to_data_type()),
            DataField::new_nullable("data_size", u64::to_data_type()),
            DataField::new_nullable("data_compressed_size", u64::to_data_type()),
            DataField::new_nullable("index_size", u64::to_data_type()),
        ])
    }

    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let table_info = TableInfo {
            desc: "'system'.'tables'".to_string(),
            name: "tables".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: TablesTable::schema(),
                engine: "SystemTables".to_string(),

                ..Default::default()
            },
        };

        AsyncOneBlockSystemTable::create(TablesTable { table_info })
    }
}
