// Copyright 2021 Datafuse Labs
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

use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::SourcePipeBuilder;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use databend_storages_common_table_meta::table::OPT_KEY_RANDOM_SEED;

use crate::RandomPartInfo;

pub struct RandomTable {
    table_info: TableInfo,
    seed: Option<u64>,
}

impl RandomTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        let seed = match table_info.meta.options.get(OPT_KEY_RANDOM_SEED) {
            None => None,
            Some(seed_str) => Some(seed_str.parse::<u64>()?),
        };
        Ok(Box::new(Self { table_info, seed }))
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
        Partitions::create(PartitionsShuffleKind::Seq, partitions)
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

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        // If extras.push_downs is None or extras.push_down.limit is None,
        // set limit to `max_block_size`.
        let (schema, total_rows) = match push_downs {
            Some(push_downs) => {
                let mut schema = self.schema();
                if let Some(projection) = push_downs.projection {
                    // do projection on schema
                    schema = match projection {
                        Projection::Columns(indices) => Arc::new(schema.project(&indices)),
                        Projection::InnerColumns(path_indices) => {
                            Arc::new(schema.inner_project(&path_indices))
                        }
                    };
                }
                let limit = push_downs.limit.unwrap_or(block_size);
                (schema, limit)
            }
            None => (self.schema(), block_size),
        };

        // generate one row to estimate the bytes size.
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                let data_type: DataType = f.data_type().into();
                let column = Column::random(&data_type, 1, self.seed);
                BlockEntry::new(data_type.clone(), Value::Column(column))
            })
            .collect::<Vec<_>>();
        let block = DataBlock::new(columns, 1);
        let one_row_bytes = block.memory_size();
        let read_bytes = total_rows * one_row_bytes;
        let parts_num = (total_rows / block_size) + 1;
        let statistics = PartStatistics::new_exact(total_rows, read_bytes, parts_num, parts_num);

        let mut worker_num = settings.get_max_threads()? as usize;
        if worker_num > parts_num {
            worker_num = parts_num;
        }
        let parts = Self::generate_random_parts(worker_num, total_rows);

        Ok((statistics, parts))
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let mut output_schema = self.table_info.schema();
        let push_downs = plan.push_downs.clone();

        if let Some(extras) = push_downs {
            if let Some(projection) = extras.projection {
                // do projection on schema
                output_schema = match projection {
                    Projection::Columns(indices) => Arc::new(output_schema.project(&indices)),
                    Projection::InnerColumns(path_indices) => {
                        Arc::new(output_schema.inner_project(&path_indices))
                    }
                };
            }
        }

        let mut builder = SourcePipeBuilder::create();

        for index in 0..plan.parts.len() {
            let output = OutputPort::create();
            let parts = RandomPartInfo::from_part(&plan.parts.partitions[index])?;
            builder.add_source(
                output.clone(),
                RandomSource::create(
                    ctx.clone(),
                    output,
                    output_schema.clone(),
                    parts.rows,
                    self.seed,
                )?,
            );
        }

        if plan.parts.is_empty() {
            let output = OutputPort::create();
            builder.add_source(
                output.clone(),
                RandomSource::create(ctx.clone(), output, output_schema, 0, self.seed)?,
            );
        }

        pipeline.add_pipe(builder.finalize());
        Ok(())
    }
}

struct RandomSource {
    schema: TableSchemaRef,
    /// how many rows are needed to generate
    rows: usize,
    seed: Option<u64>,
}

impl RandomSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        schema: TableSchemaRef,
        rows: usize,
        seed: Option<u64>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, RandomSource { schema, rows, seed })
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
            .map(|f| {
                let data_type = f.data_type().into();
                let value = Value::Column(Column::random(&data_type, self.rows, self.seed));
                BlockEntry::new(data_type, value)
            })
            .collect();

        // The partition guarantees the number of rows is less than or equal to `max_block_size`.
        // And we generate all the `self.rows` at once.
        let num_rows = self.rows;
        self.rows = 0;

        Ok(Some(DataBlock::new(columns, num_rows)))
    }
}
