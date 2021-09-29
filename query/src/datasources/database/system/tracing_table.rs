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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Extras;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use walkdir::WalkDir;

use crate::catalogs::Table;
use crate::datasources::database::system::TracingTableStream;
use crate::sessions::DatabendQueryContextRef;

pub struct TracingTable {
    table_id: u64,
    db_name: String,
    schema: DataSchemaRef,
}

impl TracingTable {
    pub fn create(table_id: u64, db_name: &str) -> Self {
        // {"v":0,"name":"databend-query","msg":"Group by partial cost: 9.071158ms","level":20,"hostname":"databend","pid":56776,"time":"2021-06-24T02:17:28.679642889+00:00"}
        TracingTable {
            table_id,
            db_name: db_name.to_string(),
            schema: DataSchemaRefExt::create(vec![
                DataField::new("v", DataType::Int64, false),
                DataField::new("name", DataType::String, false),
                DataField::new("msg", DataType::String, false),
                DataField::new("level", DataType::Int8, false),
                DataField::new("hostname", DataType::String, false),
                DataField::new("pid", DataType::Int64, false),
                DataField::new("time", DataType::String, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for TracingTable {
    fn name(&self) -> &str {
        "tracing"
    }

    fn database(&self) -> &str {
        self.db_name.as_str()
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

    fn get_id(&self) -> u64 {
        self.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        _ctx: DatabendQueryContextRef,
        _push_downs: Option<Extras>,
        _partition_num_hint: Option<usize>,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            table_id: self.table_id,
            table_version: None,
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.tracing table)".to_string(),
            scan_plan: Default::default(),
            remote: false,
            tbl_args: None,
            push_downs: None,
        })
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let mut log_files = vec![];

        for entry in WalkDir::new(ctx.get_config().log.log_dir.as_str())
            .sort_by_key(|file| file.file_name().to_owned())
        {
            let entry = entry.map_err(|e| ErrorCode::UnknownException(format!("{}", e)))?;
            if !entry.path().is_dir() {
                log_files.push(entry.path().display().to_string());
            }
        }

        // Default limit.
        let mut limit = 100000000_usize;
        let push_downs = &source_plan.push_downs;
        tracing::debug!("read push_down:{:?}", push_downs);

        if let Some(extras) = push_downs {
            if let Some(limit_push_down) = extras.limit {
                limit = limit_push_down;
            }
        }

        Ok(Box::pin(TracingTableStream::try_create(
            self.schema.clone(),
            log_files,
            limit,
        )?))
    }
}
