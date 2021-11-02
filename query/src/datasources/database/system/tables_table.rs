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
use common_datavalues::prelude::*;
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

pub struct TablesTable {
    table_info: TableInfo,
}

impl TablesTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("database", DataType::String, false),
            DataField::new("name", DataType::String, false),
            DataField::new("engine", DataType::String, false),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'tables'".to_string(),
            name: "tables".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTables".to_string(),

                ..Default::default()
            },
        };

        TablesTable { table_info }
    }
}

#[async_trait::async_trait]
impl Table for TablesTable {
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

        let cata = ctx.get_catalog();
        let databases = cata.get_databases().await?;

        let mut database_tables = vec![];
        for database in databases {
            let name = database.name();
            for table in cata.get_tables(name).await? {
                database_tables.push((name.to_string(), table));
            }
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

        let block = DataBlock::create_by_array(self.table_info.schema(), vec![
            Series::new(databases),
            Series::new(names),
            Series::new(engines),
        ]);

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
