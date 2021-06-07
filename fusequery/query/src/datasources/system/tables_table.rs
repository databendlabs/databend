// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::StringArray;
use common_exception::Result;
use common_planners::Partition;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

pub struct TablesTable {
    schema: DataSchemaRef,
}

impl TablesTable {
    pub fn create() -> Self {
        TablesTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("database", DataType::Utf8, false),
                DataField::new("name", DataType::Utf8, false),
                DataField::new("engine", DataType::Utf8, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl ITable for TablesTable {
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
        _ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: vec![Partition {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.functions table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        let database_tables = ctx.get_datasource().get_all_tables()?;

        let databases: Vec<&str> = database_tables.iter().map(|(d, _)| d.as_str()).collect();
        let names: Vec<&str> = database_tables.iter().map(|(_, v)| v.name()).collect();
        let engines: Vec<&str> = database_tables.iter().map(|(_, v)| v.engine()).collect();

        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Arc::new(StringArray::from(databases)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(engines)),
        ]);

        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
