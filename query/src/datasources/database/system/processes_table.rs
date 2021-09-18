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

use crate::catalogs::Table;
use crate::sessions::DatabendQueryContextRef;
use crate::sessions::ProcessInfo;

pub struct ProcessesTable {
    schema: DataSchemaRef,
}

impl ProcessesTable {
    pub fn create() -> Self {
        ProcessesTable {
            schema: DataSchemaRefExt::create(vec![
                DataField::new("id", DataType::String, false),
                DataField::new("type", DataType::String, false),
                DataField::new("host", DataType::String, true),
                DataField::new("state", DataType::String, false),
                DataField::new("database", DataType::String, false),
                DataField::new("extra_info", DataType::String, true),
            ]),
        }
    }

    fn process_host(process_info: &ProcessInfo) -> Option<Vec<u8>> {
        let client_address = process_info.client_address;
        client_address.as_ref().map(|s| s.to_string().into_bytes())
    }

    fn process_extra_info(process_info: &ProcessInfo) -> Option<Vec<u8>> {
        process_info
            .session_extra_info
            .clone()
            .map(|s| s.into_bytes())
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
            description: "(Read from system.processes table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let sessions_manager = ctx.get_sessions_manager();
        let processes_info = sessions_manager.processes_info();

        let mut processes_id = Vec::with_capacity(processes_info.len());
        let mut processes_type = Vec::with_capacity(processes_info.len());
        let mut processes_host = Vec::with_capacity(processes_info.len());
        let mut processes_state = Vec::with_capacity(processes_info.len());
        let mut processes_database = Vec::with_capacity(processes_info.len());
        let mut processes_extra_info = Vec::with_capacity(processes_info.len());

        for process_info in &processes_info {
            processes_id.push(process_info.id.clone().into_bytes());
            processes_type.push(process_info.typ.clone().into_bytes());
            processes_state.push(process_info.state.clone().into_bytes());
            processes_database.push(process_info.database.clone().into_bytes());
            processes_host.push(ProcessesTable::process_host(process_info));
            processes_extra_info.push(ProcessesTable::process_extra_info(process_info));
        }

        let schema = self.schema.clone();
        let block = DataBlock::create_by_array(schema.clone(), vec![
            Series::new(processes_id),
            Series::new(processes_type),
            Series::new(processes_host),
            Series::new(processes_state),
            Series::new(processes_database),
            Series::new(processes_extra_info),
        ]);

        Ok(Box::pin(DataBlockStream::create(schema, None, vec![block])))
    }
}
