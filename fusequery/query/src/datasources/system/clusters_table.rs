// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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

use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

pub struct ClustersTable {
    schema: DataSchemaRef,
}

impl ClustersTable {
    pub fn create() -> Self {
        ClustersTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("name", DataType::Utf8, false),
                DataField::new("host", DataType::Utf8, false),
                DataField::new("port", DataType::UInt16, false),
                DataField::new("priority", DataType::UInt8, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for ClustersTable {
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
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.clusters table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let executors = ctx.try_get_executors().await?;
        let names: Vec<&str> = executors.iter().map(|x| x.name.as_str()).collect();
        let hosts = executors
            .iter()
            .map(|x| x.address.hostname())
            .collect::<Vec<_>>();
        let hostnames = hosts.iter().map(|x| x.as_str()).collect::<Vec<&str>>();
        let ports: Vec<u16> = executors.iter().map(|x| x.address.port()).collect();
        let priorities: Vec<u8> = executors.iter().map(|x| x.priority).collect();
        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Series::new(names),
            Series::new(hostnames),
            Series::new(ports),
            Series::new(priorities),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
