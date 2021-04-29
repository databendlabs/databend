// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::StringArray;
use common_datavalues::UInt32Array;
use common_exception::Result;
use common_planners::Partition;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

pub struct ClustersTable {
    schema: DataSchemaRef
}

impl ClustersTable {
    pub fn create() -> Self {
        ClustersTable {
            schema: Arc::new(DataSchema::new(vec![
                DataField::new("name", DataType::Utf8, false),
                DataField::new("address", DataType::Utf8, false),
                DataField::new("cpus", DataType::Int32, false),
            ]))
        }
    }
}

#[async_trait::async_trait]
impl ITable for ClustersTable {
    fn name(&self) -> &str {
        "clusters"
    }

    fn engine(&self) -> &str {
        "SystemClusters"
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

    fn read_plan(&self, _ctx: FuseQueryContextRef, _scan: &ScanPlan) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            partitions: vec![Partition {
                name: "".to_string(),
                version: 0
            }],
            statistics: Statistics::default(),
            description: "(Read from system.clusters table)".to_string()
        })
    }

    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        let nodes = ctx.try_get_cluster()?.get_nodes()?;
        let names: Vec<&str> = nodes.iter().map(|x| x.name.as_str()).collect();
        let addresses: Vec<&str> = nodes.iter().map(|x| x.address.as_str()).collect();
        let cpus: Vec<u32> = nodes.iter().map(|x| x.cpus as u32).collect();
        let block = DataBlock::create(self.schema.clone(), vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(addresses)),
            Arc::new(UInt32Array::from(cpus)),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block]
        )))
    }
}
