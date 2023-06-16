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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use opendal::Operator;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockSerialization;
use crate::metrics::metrics_inc_block_index_write_bytes;
use crate::metrics::metrics_inc_block_index_write_milliseconds;
use crate::metrics::metrics_inc_block_index_write_nums;
use crate::metrics::metrics_inc_block_write_bytes;
use crate::metrics::metrics_inc_block_write_milliseconds;
use crate::metrics::metrics_inc_block_write_nums;
use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::common::Replacement;
use crate::operations::common::ReplacementLogEntry;
use crate::operations::mutation::mutation_meta::ClusterStatsGenType;
use crate::operations::mutation::SerializeDataMeta;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

enum State {
    Consume,
    NeedSerialize(DataBlock, ClusterStatsGenType),
    Serialized(BlockSerialization),
    Output(Replacement),
}

pub struct SerializeDataTransform {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    block_builder: BlockBuilder,

    dal: Operator,

    index: BlockMetaIndex,
}

impl SerializeDataTransform {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
    ) -> Result<ProcessorPtr> {
        let source_schema = Arc::new(table.table_info.schema().remove_virtual_computed_fields());
        let block_builder = BlockBuilder {
            ctx,
            meta_locations: table.meta_location_generator().clone(),
            source_schema,
            write_settings: table.get_write_settings(),
            cluster_stats_gen,
        };
        Ok(ProcessorPtr::create(Box::new(SerializeDataTransform {
            state: State::Consume,
            input,
            output,
            output_data: None,
            block_builder,
            dal: table.get_operator(),
            index: BlockMetaIndex::default(),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for SerializeDataTransform {
    fn name(&self) -> String {
        "SerializeDataTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::NeedSerialize(_, _) | State::Output(_)) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::Serialized(_)) {
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
            let meta = SerializeDataMeta::downcast_ref_from(&meta).unwrap();
            self.index = meta.index.clone();
            if input_data.is_empty() {
                self.state = State::Output(Replacement::Deleted);
            } else {
                self.state = State::NeedSerialize(input_data, meta.stats_type.clone());
            }
        } else {
            self.state = State::Output(Replacement::DoNothing);
        }
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::NeedSerialize(block, stats_type) => {
                let serialized =
                    self.block_builder
                        .build(block, |block, generator| match &stats_type {
                            ClusterStatsGenType::Generally => generator.gen_stats_for_append(block),
                            ClusterStatsGenType::WithOrigin(origin_stats) => {
                                let cluster_stats = generator
                                    .gen_with_origin_stats(&block, origin_stats.clone())?;
                                Ok((cluster_stats, block))
                            }
                        })?;

                self.state = State::Serialized(serialized);
            }
            State::Output(op) => {
                let entry = ReplacementLogEntry {
                    index: self.index.clone(),
                    op,
                };
                let meta = MutationLogs {
                    entries: vec![MutationLogEntry::Replacement(entry)],
                };
                self.output_data = Some(DataBlock::empty_with_meta(Box::new(meta)));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Serialized(serialized) => {
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
                let block_meta = Arc::new(serialized.block_meta);
                self.state = State::Output(Replacement::Replaced(block_meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
