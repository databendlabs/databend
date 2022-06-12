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

use super::RandomPartInfo;
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

    pub fn generate_random_parts(workers: usize, total: usize) -> Partitions {
        let part_size = total / workers;
        let mut part_remain = total % workers;

        let mut partitions = Vec::with_capacity(workers);
        if part_size == 0 {
            partitions.push(RandomPartInfo::create(total));
        } else {
            for _ in 0..workers {
                let rows = if part_remain > 0 {
                    part_remain -= 1;
                    part_size + 1
                } else {
                    part_size
                };
                partitions.push(RandomPartInfo::create(rows));
            }
        }
        partitions
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
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        // If extras.push_downs is None or extras.push_down.limit is None,
        // set limit to `max_block_size`.
        let (schema, total_rows) = match push_downs {
            Some(push_downs) => {
                let mut schema = self.schema();
                if let Some(projection) = push_downs.projection {
                    // do projection on schema
                    schema = Arc::new(schema.project(projection));
                }
                let limit = match push_downs.limit {
                    Some(limit) => limit,
                    None => block_size,
                };
                (schema, limit)
            }
            None => (self.schema(), block_size),
        };

        // generate one row to estimate the bytes size.
        let one_row_bytes = schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_random_column(1))
            .collect::<Vec<_>>()
            .iter()
            .map(|col| col.memory_size())
            .sum::<usize>();
        let read_bytes = total_rows * one_row_bytes;
        let parts_num = (total_rows / block_size) + 1;
        let statistics = Statistics::new_exact(total_rows, read_bytes, parts_num, parts_num);

        let mut worker_num = settings.get_max_threads()? as usize;
        if worker_num > parts_num {
            worker_num = parts_num;
        }
        let parts = Self::generate_random_parts(worker_num, total_rows);

        Ok((statistics, parts))
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
        let mut output_schema = self.table_info.schema();
        let push_downs = plan.push_downs.clone();

        if let Some(extras) = push_downs {
            if let Some(projection) = extras.projection {
                // do projection on schema
                output_schema = Arc::new(output_schema.project(projection));
            }
        }

        let mut builder = SourcePipeBuilder::create();

        for index in 0..plan.parts.len() {
            let output = OutputPort::create();
            let parts = RandomPartInfo::from_part(&plan.parts[index])?;
            builder.add_source(
                output.clone(),
                RandomSource::create(ctx.clone(), output, output_schema.clone(), parts.rows)?,
            );
        }

        pipeline.add_pipe(builder.finalize());
        Ok(())
    }
}

struct RandomSource {
    schema: DataSchemaRef,
    /// how many rows are needed to generate
    rows: usize,
}

impl RandomSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        rows: usize,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, RandomSource { schema, rows })
    }
}

impl SyncSource for RandomSource {
    const NAME: &'static str = "RandomTable";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.rows == 0 {
            // No more row is needed to generate.
            return Ok(None);
        }

        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_random_column(self.rows))
            .collect();

        // The partition garantees the number of rows is less than or equal to `max_block_size`.
        // And we generate all the `self.rows` at once.
        self.rows = 0;

        Ok(Some(DataBlock::create(self.schema.clone(), columns)))
    }
}
