// Copyright 2021 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use walkdir::WalkDir;

use crate::sessions::QueryContext;
use crate::storages::system::TracingTableStream;
use crate::storages::Table;

pub struct TracingTable {
    table_info: TableInfo,
}

impl TracingTable {
    pub fn create(table_id: u64) -> Self {
        // {"v":0,"name":"databend-query","msg":"Group by partial cost: 9.071158ms","level":20,"hostname":"databend","pid":56776,"time":"2021-06-24T02:17:28.679642889+00:00"}

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("v", i64::to_data_type()),
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("msg", Vu8::to_data_type()),
            DataField::new("level", i8::to_data_type()),
            DataField::new("hostname", Vu8::to_data_type()),
            DataField::new("pid", i64::to_data_type()),
            DataField::new("time", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'tracing'".to_string(),
            name: "tracing".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTracing".to_string(),
                ..Default::default()
            },
        };

        TracingTable { table_info }
    }
}

#[async_trait::async_trait]
impl Table for TracingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
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
        tracing::debug!("read push_down:{:?}", &plan.push_downs);

        if let Some(extras) = &plan.push_downs {
            if let Some(limit_push_down) = extras.limit {
                limit = limit_push_down;
            }
        }

        Ok(Box::pin(TracingTableStream::try_create(
            self.table_info.schema(),
            log_files,
            limit,
        )?))
    }
}
