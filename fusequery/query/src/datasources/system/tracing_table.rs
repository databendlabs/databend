// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Partition;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use walkdir::WalkDir;

use crate::datasources::system::TracingTableStream;
use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

pub struct TracingTable {
    schema: DataSchemaRef,
}

impl TracingTable {
    pub fn create() -> Self {
        // {"v":0,"name":"fuse-query","msg":"Group by partial cost: 9.071158ms","level":20,"hostname":"datafuse","pid":56776,"time":"2021-06-24T02:17:28.679642889+00:00"}
        TracingTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("v", DataType::Int64, false),
                DataField::new("name", DataType::Utf8, false),
                DataField::new("msg", DataType::Utf8, false),
                DataField::new("level", DataType::Int8, false),
                DataField::new("hostname", DataType::Utf8, false),
                DataField::new("pid", DataType::Int64, false),
                DataField::new("time", DataType::Utf8, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for TracingTable {
    fn name(&self) -> &str {
        "tracing"
    }

    fn engine(&self) -> &str {
        "SystemTracing"
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
            description: "(Read from system.tracing table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let mut log_files = vec![];

        for entry in WalkDir::new(ctx.get_config().log_dir.as_str())
            .sort_by_key(|file| file.file_name().to_owned())
        {
            let entry = entry.map_err(|e| ErrorCode::UnknownException(format!("{}", e)))?;
            if !entry.path().is_dir() {
                log_files.push(entry.path().display().to_string());
            }
        }

        // Default limit.
        let mut limit = 100000000_usize;
        let extras = source_plan.get_push_downs();
        tracing::debug!("read extras:{:?}", extras);

        if let Some(limit_push_down) = extras.limit {
            limit = limit_push_down;
        }

        Ok(Box::pin(TracingTableStream::try_create(
            self.schema.clone(),
            log_files,
            limit,
        )?))
    }
}
