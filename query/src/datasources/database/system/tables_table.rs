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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::sessions::DatabendQueryContextRef;

pub struct TablesTable {
    schema: DataSchemaRef,
}

impl TablesTable {
    pub fn create() -> Self {
        TablesTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("database", DataType::String, false),
                DataField::new("name", DataType::String, false),
                DataField::new("engine", DataType::String, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for TablesTable {
    fn name(&self) -> &str {
        "tables"
    }

    fn engine(&self) -> &str {
        "SystemTables"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        _ctx: DatabendQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            table_id: scan.table_id,
            table_version: scan.table_version,
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.functions table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let databases = ctx.get_catalog().get_databases()?;
        let mut database_tables = vec![];
        for database in databases {
            for table in database.get_tables()? {
                let name = database.name().to_string();
                database_tables.push((name, table));
            }
        }

        let databases: Vec<&[u8]> = database_tables.iter().map(|(d, _)| d.as_bytes()).collect();
        let names: Vec<&[u8]> = database_tables
            .iter()
            .map(|(_, v)| v.raw().name().as_bytes())
            .collect();
        let engines: Vec<&[u8]> = database_tables
            .iter()
            .map(|(_, v)| v.raw().engine().as_bytes())
            .collect();

        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Series::new(databases),
            Series::new(names),
            Series::new(engines),
        ]);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
