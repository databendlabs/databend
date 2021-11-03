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

use std::any::Any;
use std::sync::Arc;

use common_context::IOContext;
use common_context::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::series::Series;
use common_datavalues::series::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::sessions::DatabendQueryContext;

pub struct ColumnsTable {
    table_info: TableInfo,
}

impl ColumnsTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String, false),
            DataField::new("database", DataType::String, false),
            DataField::new("table", DataType::String, false),
            DataField::new("data_type", DataType::String, false),
            DataField::new("is_nullable", DataType::Boolean, false),
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

        Self { table_info }
    }

    pub async fn dump_table_columns(
        &self,
        ctx: Arc<DatabendQueryContext>,
    ) -> Result<Vec<(String, String, DataField)>> {
        let catalog = ctx.get_catalog();
        let databases = catalog.get_databases().await?;

        let mut rows: Vec<(String, String, DataField)> = vec![];
        for database in databases {
            for table in catalog.get_tables(database.name()).await? {
                for field in table.schema().fields() {
                    rows.push((database.name().into(), table.name().into(), field.clone()))
                }
            }
        }

        Ok(rows)
    }
}

#[async_trait::async_trait]
impl Table for ColumnsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");

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
            data_types.push(field.data_type().to_string().into_bytes());
            is_nullables.push(field.is_nullable());
        }

        let block = DataBlock::create_by_array(self.table_info.schema(), vec![
            Series::new(names),
            Series::new(databases),
            Series::new(tables),
            Series::new(data_types),
            Series::new(is_nullables),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
