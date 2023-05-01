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
use std::time::Instant;

use common_base::base::ProgressValues;
use common_base::runtime::GlobalIORuntime;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TableSchema;
use common_pipeline_core::processors::processor::ProcessorPtr;
use opendal::Operator;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::metrics::*;
use crate::operations::mutation::compact::CompactSourceMeta;
use crate::operations::mutation::CompactPartInfo;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::ClusterStatsGenerator;

pub struct CompactSource {
    ctx: Arc<dyn TableContext>,
    dal: Operator,

    block_reader: Arc<BlockReader>,
    block_builder: BlockBuilder,

    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    finished: bool,
}

impl CompactSource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        write_settings: WriteSettings,
        meta_locations: TableMetaLocationGenerator,
        source_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let block_builder = BlockBuilder {
            ctx: ctx.clone(),
            meta_locations,
            source_schema,
            write_settings,
            cluster_stats_gen: ClusterStatsGenerator::default(),
        };
        Ok(ProcessorPtr::create(Box::new(CompactSource {
            ctx,
            dal,
            block_reader,
            block_builder,
            output,
            output_data: None,
            finished: false,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for CompactSource {
    fn name(&self) -> String {
        "CompactSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            if !self.finished {
                return Ok(Event::Async);
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self.output_data.take() {
            self.output.push_data(Ok(block));
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.ctx.get_partition() {
            Some(part) => {
                let block_reader = self.block_reader.as_ref();
                let block_builder = self.block_builder.clone();

                // block read tasks.
                let mut task_futures = Vec::new();
                let part = CompactPartInfo::from_part(&part)?;
                let mut stats = Vec::with_capacity(part.blocks.len());
                for block in &part.blocks {
                    stats.push(block.col_stats.clone());

                    let settings = ReadSettings::from_ctx(&self.ctx)?;
                    let storage_format = block_builder.write_settings.storage_format;
                    // read block in parallel.
                    task_futures.push(async move {
                        // Perf
                        {
                            metrics_inc_compact_block_read_nums(1);
                            metrics_inc_compact_block_read_bytes(block.block_size);
                        }

                        block_reader
                            .read_by_meta(&settings, block.as_ref(), &storage_format)
                            .await
                    });
                }

                let start = Instant::now();

                let blocks = futures::future::try_join_all(task_futures).await?;
                // Perf.
                {
                    metrics_inc_compact_block_read_milliseconds(start.elapsed().as_millis() as u64);
                }

                // concat blocks.
                let new_block = if blocks.len() == 1 {
                    blocks[0].convert_to_full()
                } else {
                    DataBlock::concat(&blocks)?
                };
                // build block serialization.
                let serialized = GlobalIORuntime::instance()
                    .spawn_blocking(move || {
                        block_builder.build(new_block, |block, _| Ok((None, block)))
                    })
                    .await?;

                let start = Instant::now();

                // Perf.
                {
                    metrics_inc_compact_block_write_nums(1);
                    metrics_inc_compact_block_write_bytes(serialized.block_raw_data.len() as u64);
                }

                // write block data.
                write_data(
                    serialized.block_raw_data,
                    &self.dal,
                    &serialized.block_meta.location.0,
                )
                .await?;

                // write index data.
                if let Some(index_state) = serialized.bloom_index_state {
                    write_data(index_state.data, &self.dal, &index_state.location.0).await?;
                }

                // Perf
                {
                    metrics_inc_compact_block_write_milliseconds(start.elapsed().as_millis() as u64);
                }

                let progress_values = ProgressValues {
                    rows: serialized.block_meta.row_count as usize,
                    bytes: serialized.block_meta.block_size as usize,
                };
                self.ctx.get_write_progress().incr(&progress_values);

                self.output_data = Some(DataBlock::empty_with_meta(CompactSourceMeta::create(
                    part.index.clone(),
                    serialized.block_meta.into(),
                )));
            }
            None => self.finished = true,
        };

        Ok(())
    }
}
