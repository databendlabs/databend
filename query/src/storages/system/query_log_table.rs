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
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::SyncSource;
use crate::pipelines::new::processors::SyncSourcer;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct QueryLogTable {
    table_info: TableInfo,
    max_rows: i32,
    data: Arc<RwLock<VecDeque<DataBlock>>>,
}

impl QueryLogTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            // Type.
            DataField::new("log_type", i8::to_data_type()),
            DataField::new("handler_type", Vu8::to_data_type()),
            // User.
            DataField::new("tenant_id", Vu8::to_data_type()),
            DataField::new("cluster_id", Vu8::to_data_type()),
            DataField::new("sql_user", Vu8::to_data_type()),
            DataField::new("sql_user_quota", Vu8::to_data_type()),
            DataField::new("sql_user_privileges", Vu8::to_data_type()),
            // Query.
            DataField::new("query_id", Vu8::to_data_type()),
            DataField::new("query_kind", Vu8::to_data_type()),
            DataField::new("query_text", Vu8::to_data_type()),
            DataField::new("event_date", DateType::arc()),
            DataField::new("event_time", TimestampType::arc(3, None)),
            // Schema.
            DataField::new("current_database", Vu8::to_data_type()),
            DataField::new("databases", Vu8::to_data_type()),
            DataField::new("tables", Vu8::to_data_type()),
            DataField::new("columns", Vu8::to_data_type()),
            DataField::new("projections", Vu8::to_data_type()),
            // Stats.
            DataField::new("written_rows", u64::to_data_type()),
            DataField::new("written_bytes", u64::to_data_type()),
            DataField::new("written_io_bytes", u64::to_data_type()),
            DataField::new("written_io_bytes_cost_ms", u64::to_data_type()),
            DataField::new("scan_rows", u64::to_data_type()),
            DataField::new("scan_bytes", u64::to_data_type()),
            DataField::new("scan_io_bytes", u64::to_data_type()),
            DataField::new("scan_io_bytes_cost_ms", u64::to_data_type()),
            DataField::new("scan_partitions", u64::to_data_type()),
            DataField::new("total_partitions", u64::to_data_type()),
            DataField::new("result_rows", u64::to_data_type()),
            DataField::new("result_bytes", u64::to_data_type()),
            DataField::new("cpu_usage", u32::to_data_type()),
            DataField::new("memory_usage", u64::to_data_type()),
            // Client.
            DataField::new("client_info", Vu8::to_data_type()),
            DataField::new("client_address", Vu8::to_data_type()),
            // Exception.
            DataField::new("exception_code", i32::to_data_type()),
            DataField::new("exception_text", Vu8::to_data_type()),
            DataField::new("stack_trace", Vu8::to_data_type()),
            // Server.
            DataField::new("server_version", Vu8::to_data_type()),
            // Session settings
            DataField::new("session_settings", Vu8::to_data_type()),
            // Extra.
            DataField::new("extra", Vu8::to_data_type()),
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
            data: Arc::new(RwLock::new(VecDeque::new())),
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

    async fn read_partitions(
        &self,
        _ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
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

    fn read2(
        &self,
        ctx: Arc<QueryContext>,
        _: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        // TODO: split data for multiple threads
        let output = OutputPort::create();
        let mut source_builder = SourcePipeBuilder::create();

        source_builder.add_source(
            output.clone(),
            QueryLogSource::create(ctx, output, &self.data.read())?,
        );

        pipeline.add_pipe(source_builder.finalize());
        Ok(())
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

struct QueryLogSource {
    data: VecDeque<DataBlock>,
}

impl QueryLogSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        data: &VecDeque<DataBlock>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, QueryLogSource { data: data.clone() })
    }
}

impl SyncSource for QueryLogSource {
    const NAME: &'static str = "system.query_log";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        Ok(self.data.pop_front())
    }
}
