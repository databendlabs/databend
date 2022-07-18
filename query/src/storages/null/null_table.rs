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

use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing::info;
use futures::stream::StreamExt;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::EmptySink;
use crate::pipelines::processors::SyncSource;
use crate::pipelines::processors::SyncSourcer;
use crate::pipelines::Pipe;
use crate::pipelines::Pipeline;
use crate::pipelines::SinkPipeBuilder;
use crate::sessions::query_ctx::QryCtx;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::Table;

pub struct NullTable {
    table_info: TableInfo,
}

impl NullTable {
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(Self { table_info }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "NULL".to_string(),
            comment: "NULL Storage Engine".to_string(),
            ..Default::default()
        }
    }
}

#[async_trait::async_trait]
impl Table for NullTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn QryCtx>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    fn read2(
        &self,
        ctx: Arc<dyn QryCtx>,
        _: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        let schema = self.table_info.schema();
        pipeline.add_pipe(Pipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output.clone()],
            processors: vec![NullSource::create(ctx, output, schema)?],
        });

        Ok(())
    }

    async fn append_data(
        &self,
        _ctx: Arc<dyn QryCtx>,
        mut stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        while let Some(block) = stream.next().await {
            let block = block?;
            info!("Ignore one block rows: {}", block.num_rows())
        }
        Ok(Box::pin(DataBlockStream::create(
            std::sync::Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }

    fn append2(&self, _: Arc<dyn QryCtx>, pipeline: &mut Pipeline) -> Result<()> {
        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _ in 0..pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(input_port.clone(), EmptySink::create(input_port));
        }
        pipeline.add_pipe(sink_pipeline_builder.finalize());
        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: Arc<dyn QryCtx>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Ok(())
    }
}

struct NullSource {
    finish: bool,
    schema: DataSchemaRef,
}

impl NullSource {
    pub fn create(
        ctx: Arc<dyn QryCtx>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, NullSource {
            finish: false,
            schema,
        })
    }
}

impl SyncSource for NullSource {
    const NAME: &'static str = "NullSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        Ok(Some(DataBlock::empty_with_schema(self.schema.clone())))
    }
}
