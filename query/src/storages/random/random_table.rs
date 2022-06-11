//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::SyncSource;
use crate::pipelines::new::processors::SyncSourcer;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::Table;

pub struct RandomTable {
    table_info: TableInfo,
}

impl RandomTable {
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(Self { table_info }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "RANDOM".to_string(),
            comment: "RANDOM Storage Engine".to_string(),
            ..Default::default()
        }
    }
}

#[async_trait::async_trait]
impl Table for RandomTable {
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

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn read2(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        let mut output_schema = self.table_info.schema();
        let push_downs = plan.push_downs.clone();
        // If extras.push_downs is None or extras.push_down.limit is None,
        // set limit to `max_block_size`.
        let limit = match push_downs {
            Some(push_downs) => {
                if let Some(projection) = push_downs.projection {
                    // do projection on schema
                    output_schema = Arc::new(output_schema.project(projection));
                }
                match push_downs.limit {
                    Some(limit) => limit,
                    None => block_size,
                }
            }
            None => block_size,
        };

        let mut builder = SourcePipeBuilder::create();

        let max_threads = settings.get_max_threads()? as usize;
        // divide the whole limit by `max_threads`.
        let each_limit = limit / max_threads;
        let mut remain = limit % max_threads;

        for _index in 0..max_threads {
            // divide the whole limit by `max_threads`.
            let limit = if remain > 0 {
                remain -= 1;
                each_limit + 1
            } else {
                each_limit
            };
            let output = OutputPort::create();
            builder.add_source(
                output.clone(),
                RandomSource::create(
                    ctx.clone(),
                    output,
                    output_schema.clone(),
                    limit,
                    block_size,
                )?,
            );
        }

        pipeline.add_pipe(builder.finalize());
        Ok(())
    }
}

struct RandomSource {
    schema: DataSchemaRef,
    /// The number of rows needed to generate.
    limit: usize,
    /// record rows count.
    rows: usize,
    /// The max number of rows can one `generate` generate.
    block_size: usize,
}

impl RandomSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        limit: usize,
        block_size: usize,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, RandomSource {
            schema,
            limit,
            rows: 0,
            block_size,
        })
    }
}

impl SyncSource for RandomSource {
    const NAME: &'static str = "RandomTable";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.rows == self.limit {
            return Ok(None);
        }

        let rows = if self.limit - self.rows <= self.block_size {
            self.limit - self.rows
        } else {
            self.block_size
        };

        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_random_column(rows))
            .collect();

        self.rows += rows;

        Ok(Some(DataBlock::create(self.schema.clone(), columns)))
    }
}
