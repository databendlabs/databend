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

use chrono::Utc;
use common_base::base::ProgressValues;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use opendal::Operator;
use storages_common_index::BloomIndex;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::metrics::metrics_inc_block_index_write_bytes;
use crate::metrics::metrics_inc_block_index_write_milliseconds;
use crate::metrics::metrics_inc_block_index_write_nums;
use crate::metrics::metrics_inc_block_write_bytes;
use crate::metrics::metrics_inc_block_write_milliseconds;
use crate::metrics::metrics_inc_block_write_nums;
use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::mutation::ClusterStatsGenType;
use crate::operations::mutation::SerializeDataMeta;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

enum State {
    Consume,
    NeedSerialize {
        block: DataBlock,
        stats_type: ClusterStatsGenType,
        index: Option<BlockMetaIndex>,
    },
    Serialized {
        // serialized: BlockSerialization,
        block: DataBlock,
        stats_type: ClusterStatsGenType,
        index: Option<BlockMetaIndex>,
    },
}

pub struct TransformSerializeBlock {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    block_builder: BlockBuilder,
    dal: Operator,
    table: FuseTable,
    prev_snapshot_time: Option<i64>,
}

impl TransformSerializeBlock {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
    ) -> Result<Self> {
        let source_schema = Arc::new(table.table_info.schema().remove_virtual_computed_fields());
        let bloom_columns_map = table
            .bloom_index_cols
            .bloom_index_fields(source_schema.clone(), BloomIndex::supported_type)?;
        let block_builder = BlockBuilder {
            ctx,
            meta_locations: table.meta_location_generator().clone(),
            source_schema,
            write_settings: table.get_write_settings(),
            cluster_stats_gen,
            bloom_columns_map,
        };
        Ok(TransformSerializeBlock {
            state: State::Consume,
            input,
            output,
            output_data: None,
            block_builder,
            dal: table.get_operator(),
            table: table.clone(),
            prev_snapshot_time: None,
        })
    }

    pub fn into_processor(self) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(self)))
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input.clone();
        let output = self.output.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output])
    }

    pub fn get_block_builder(&self) -> BlockBuilder {
        self.block_builder.clone()
    }

    fn mutation_logs(entry: MutationLogEntry) -> DataBlock {
        let meta = MutationLogs {
            entries: vec![entry],
        };
        DataBlock::empty_with_meta(Box::new(meta))
    }
}

#[async_trait::async_trait]
impl Processor for TransformSerializeBlock {
    fn name(&self) -> String {
        "TransformSerializeBlock".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::NeedSerialize { .. }) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::Serialized { .. }) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let mut input_data = self.input.pull_data().unwrap()?;
        let meta = input_data.take_meta();
        if let Some(meta) = meta {
            let meta =
                SerializeDataMeta::downcast_from(meta).ok_or(ErrorCode::Internal("It's a bug"))?;
            if let Some(deleted_segment) = meta.deleted_segment {
                // delete a whole segment, segment level
                let data_block =
                    Self::mutation_logs(MutationLogEntry::DeletedSegment { deleted_segment });
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            } else if input_data.is_empty() {
                // delete a whole block, block level
                let data_block =
                    Self::mutation_logs(MutationLogEntry::DeletedBlock { index: meta.index });
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            } else {
                // replace the old block
                self.state = State::NeedSerialize {
                    block: input_data,
                    stats_type: meta.stats_type,
                    index: Some(meta.index),
                };
                Ok(Event::Sync)
            }
        } else if input_data.is_empty() {
            // do nothing
            let data_block = Self::mutation_logs(MutationLogEntry::DoNothing);
            self.output.push_data(Ok(data_block));
            Ok(Event::NeedConsume)
        } else {
            // append block
            self.state = State::NeedSerialize {
                block: input_data,
                stats_type: ClusterStatsGenType::Generally,
                index: None,
            };
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::NeedSerialize {
                block,
                stats_type,
                index,
            } => {
                self.state = State::Serialized {
                    block,
                    stats_type,
                    index,
                };
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Serialized {
                block,
                stats_type,
                index,
            } => {
                let prev_snapshot_time = match self.prev_snapshot_time {
                    None => {
                        let prev_snapshot_time = match self.table.read_table_snapshot().await? {
                            Some(snapshot) => snapshot
                                .timestamp
                                .map_or(Utc::now().timestamp(), |timestamp| timestamp.timestamp()),
                            None => Utc::now().timestamp(),
                        };
                        self.prev_snapshot_time = Some(prev_snapshot_time);
                        prev_snapshot_time
                    }
                    Some(prev_snapshot_time) => prev_snapshot_time,
                };
                let serialized = self
                    .block_builder
                    .build(
                        block,
                        prev_snapshot_time,
                        |block, generator| match &stats_type {
                            ClusterStatsGenType::Generally => generator.gen_stats_for_append(block),
                            ClusterStatsGenType::WithOrigin(origin_stats) => {
                                let cluster_stats = generator
                                    .gen_with_origin_stats(&block, origin_stats.clone())?;
                                Ok((cluster_stats, block))
                            }
                        },
                    )
                    .await?;

                let start = Instant::now();
                // write block data.
                let raw_block_data = serialized.block_raw_data;
                let data_size = raw_block_data.len();
                let path = serialized.block_meta.location.0.as_str();
                write_data(raw_block_data, &self.dal, path).await?;

                // Perf.
                {
                    metrics_inc_block_write_nums(1);
                    metrics_inc_block_write_bytes(data_size as u64);
                    metrics_inc_block_write_milliseconds(start.elapsed().as_millis() as u64);
                }

                // write index data.
                let bloom_index_state = serialized.bloom_index_state;
                if let Some(bloom_index_state) = bloom_index_state {
                    let index_size = bloom_index_state.data.len();
                    write_data(
                        bloom_index_state.data,
                        &self.dal,
                        &bloom_index_state.location.0,
                    )
                    .await?;
                    // Perf.
                    {
                        metrics_inc_block_index_write_nums(1);
                        metrics_inc_block_index_write_bytes(index_size as u64);
                        metrics_inc_block_index_write_milliseconds(
                            start.elapsed().as_millis() as u64
                        );
                    }
                }

                let data_block = if let Some(index) = index {
                    Self::mutation_logs(MutationLogEntry::ReplacedBlock {
                        index,
                        block_meta: Arc::new(serialized.block_meta),
                    })
                } else {
                    let progress_values = ProgressValues {
                        rows: serialized.block_meta.row_count as usize,
                        bytes: serialized.block_meta.block_size as usize,
                    };
                    self.block_builder
                        .ctx
                        .get_write_progress()
                        .incr(&progress_values);

                    DataBlock::empty_with_meta(Box::new(serialized.block_meta))
                };
                self.output_data = Some(data_block);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
