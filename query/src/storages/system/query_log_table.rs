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
use std::collections::VecDeque;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct QueryLogTable {
    table_info: TableInfo,
    max_rows: i32,
    data: RwLock<VecDeque<DataBlock>>,
}

impl QueryLogTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            // Type.
            DataField::new("log_type", DataType::Int8, false),
            DataField::new("handler_type", DataType::String, false),
            // User.
            DataField::new("tenant_id", DataType::String, false),
            DataField::new("cluster_id", DataType::String, false),
            DataField::new("sql_user", DataType::String, false),
            DataField::new("sql_user_quota", DataType::String, false),
            DataField::new("sql_user_privileges", DataType::String, false),
            // Query.
            DataField::new("query_id", DataType::String, false),
            DataField::new("query_kind", DataType::String, false),
            DataField::new("query_text", DataType::String, false),
            DataField::new("event_date", DataType::Date32, false),
            DataField::new("event_time", DataType::DateTime64(3, None), false),
            // Schema.
            DataField::new("current_database", DataType::String, false),
            DataField::new("databases", DataType::String, false),
            DataField::new("tables", DataType::String, false),
            DataField::new("columns", DataType::String, false),
            DataField::new("projections", DataType::String, false),
            // Stats.
            DataField::new("written_rows", DataType::UInt64, false),
            DataField::new("written_bytes", DataType::UInt64, false),
            DataField::new("scan_rows", DataType::UInt64, false),
            DataField::new("scan_bytes", DataType::UInt64, false),
            DataField::new("scan_byte_cost_ms", DataType::UInt64, false),
            DataField::new("scan_seeks", DataType::UInt64, false),
            DataField::new("scan_seek_cost_ms", DataType::UInt64, false),
            DataField::new("scan_partitions", DataType::UInt64, false),
            DataField::new("result_rows", DataType::UInt64, false),
            DataField::new("result_bytes", DataType::UInt64, false),
            DataField::new("cpu_usage", DataType::UInt32, false),
            DataField::new("memory_usage", DataType::UInt64, false),
            // Client.
            DataField::new("client_info", DataType::String, false),
            DataField::new("client_address", DataType::String, false),
            // Exception.
            DataField::new("exception_code", DataType::Int32, false),
            DataField::new("exception_text", DataType::String, false),
            DataField::new("stack_trace", DataType::String, false),
            // Server.
            DataField::new("server_version", DataType::String, false),
            // Extra.
            DataField::new("extra", DataType::String, false),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_log'".to_string(),
            name: "query_log".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemQueryLog".to_string(),
                ..Default::default()
            },
        };
        QueryLogTable {
            table_info,
            max_rows: 200000,
            data: RwLock::new(VecDeque::new()),
        }
    }

    #[allow(dead_code)]
    pub fn set_max_rows(&mut self, max: i32) {
        self.max_rows = max;
    }
}

#[async_trait::async_trait]
impl Table for QueryLogTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let data = self.data.read().clone();
        let mut blocks = Vec::with_capacity(data.len());
        for block in data {
            blocks.push(block);
        }
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            blocks,
        )))
    }

    async fn append_data(
        &self,
        _ctx: Arc<QueryContext>,
        mut stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        while let Some(block) = stream.next().await {
            let block = block?;
            self.data.write().push_back(block);
        }

        // Check overflow.
        let over = self.data.read().len() as i32 - self.max_rows;
        if over > 0 {
            for _x in 0..over {
                self.data.write().pop_front();
            }
        }

        Ok(Box::pin(DataBlockStream::create(
            std::sync::Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }

    async fn truncate(
        &self,
        _ctx: Arc<QueryContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        let mut data = self.data.write();
        *data = VecDeque::new();
        Ok(())
    }
}
