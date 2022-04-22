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

use crate::sessions::QueryContext;
use crate::storages::system::table::AsyncOneBlockSystemTable;
use crate::storages::system::table::AsyncSystemTable;
use crate::storages::Table;

pub struct ColumnsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for ColumnsTable {
    const NAME: &'static str = "system.columns";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let rows = self.dump_table_columns(ctx).await?;
        let mut names: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut tables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut databases: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut data_types: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut is_nullables: Vec<bool> = Vec::with_capacity(rows.len());
        for (database_name, table_name, field) in rows.into_iter() {
            names.push(field.name().clone().into_bytes());
            tables.push(table_name.into_bytes());
            databases.push(database_name.into_bytes());

            let non_null_type = remove_nullable(field.data_type());
            let data_type = format_data_type_sql(&non_null_type);
            data_types.push(data_type.into_bytes());
            is_nullables.push(field.is_nullable());
        }

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(databases),
            Series::from_data(tables),
            Series::from_data(data_types),
            Series::from_data(is_nullables),
        ]))
    }
}

impl ColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("database", Vu8::to_data_type()),
            DataField::new("table", Vu8::to_data_type()),
            DataField::new("data_type", Vu8::to_data_type()),
            DataField::new("is_nullable", bool::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'columns'".to_string(),
            name: "columns".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemColumns".to_string(),
                ..Default::default()
            },
        };

        AsyncOneBlockSystemTable::create(ColumnsTable { table_info })
    }

    async fn dump_table_columns(
        &self,
        ctx: Arc<QueryContext>,
    ) -> Result<Vec<(String, String, DataField)>> {
        let tenant = ctx.get_tenant();
        // TODO replace default with real cat
        let catalog = ctx.get_catalog("default")?;
        let databases = catalog.list_databases(tenant.as_str()).await?;

        let mut rows: Vec<(String, String, DataField)> = vec![];
        for database in databases {
            for table in catalog
                .list_tables(tenant.as_str(), database.name())
                .await?
            {
                for field in table.schema().fields() {
                    rows.push((database.name().into(), table.name().into(), field.clone()))
                }
            }
        }

        Ok(rows)
    }
}
