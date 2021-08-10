// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::series::Series;
use common_datavalues::series::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::Table;
use crate::sessions::DatafuseQueryContextRef;
use crate::sessions::ProcessInfo;

pub struct ProcessesTable {
    schema: DataSchemaRef,
}

impl ProcessesTable {
    pub fn create() -> Self {
        ProcessesTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("id", DataType::Utf8, false),
                DataField::new("host", DataType::Utf8, true),
                DataField::new("state", DataType::Utf8, false),
                DataField::new("database", DataType::Utf8, false),
                DataField::new("extra_info", DataType::Utf8, true),
            ]),
        }
    }

    fn process_host(process_info: &ProcessInfo) -> Option<String> {
        process_info
            .client_address
            .map(|socket_address| socket_address.to_string())
    }

    fn process_extra_info(process_info: &ProcessInfo) -> Option<String> {
        process_info.session_extra_info.clone()
    }
}

#[async_trait::async_trait]
impl Table for ProcessesTable {
    fn name(&self) -> &str {
        "processes"
    }

    fn engine(&self) -> &str {
        "SystemProcesses"
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
        _ctx: DatafuseQueryContextRef,
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
            description: "(Read from system.processes table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: DatafuseQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let sessions_manager = ctx.get_sessions_manager();
        let processes_info = sessions_manager.processes_info();

        let mut processes_id = Vec::with_capacity(processes_info.len());
        let mut processes_host = Vec::with_capacity(processes_info.len());
        let mut processes_state = Vec::with_capacity(processes_info.len());
        let mut processes_database = Vec::with_capacity(processes_info.len());
        let mut processes_extra_info = Vec::with_capacity(processes_info.len());

        for process_info in &processes_info {
            processes_id.push(process_info.id.clone());
            processes_state.push(process_info.state.clone());
            processes_database.push(process_info.database.clone());
            processes_host.push(ProcessesTable::process_host(process_info));
            processes_extra_info.push(ProcessesTable::process_extra_info(process_info));
        }

        let schema = self.schema.clone();
        let block = DataBlock::create_by_array(schema.clone(), vec![
            Series::new(processes_id),
            Series::new(processes_host),
            Series::new(processes_state),
            Series::new(processes_database),
            Series::new(processes_extra_info),
        ]);

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
