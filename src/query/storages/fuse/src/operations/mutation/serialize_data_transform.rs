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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use opendal::Operator;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::ClusterStatistics;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockSerialization;
use crate::operations::mutation::Mutation;
use crate::operations::mutation::MutationTransformMeta;
use crate::operations::mutation::SerializeDataMeta;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

enum State {
    Consume,
    NeedSerialize(DataBlock, Option<ClusterStatistics>),
    Serialized(BlockSerialization),
    Output(Mutation),
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
        let block_builder = BlockBuilder {
            ctx,
            meta_locations: table.meta_location_generator().clone(),
            source_schema: table.schema(),
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
            let meta = SerializeDataMeta::from_meta(&meta)?;
            self.index = meta.index.clone();
            if input_data.is_empty() {
                self.state = State::Output(Mutation::Deleted);
            } else {
                self.state = State::NeedSerialize(input_data, meta.cluster_stats.clone());
            }
        } else {
            self.state = State::Output(Mutation::DoNothing);
        }
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::NeedSerialize(block, origin_stats) => {
                let serialized = self.block_builder.build(block, |block, generator| {
                    let cluster_stats =
                        generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                    Ok((cluster_stats, block))
                })?;

                self.state = State::Serialized(serialized);
            }
            State::Output(op) => {
                let meta = MutationTransformMeta::create(self.index.clone(), op);
                self.output_data = Some(DataBlock::empty_with_meta(meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Serialized(serialized) => {
                // write block data.
                let raw_block_data = serialized.block_raw_data;
                let path = serialized.block_meta.location.0.as_str();
                write_data(raw_block_data, &self.dal, path).await?;

                // write index data.
                let bloom_index_state = serialized.bloom_index_state;
                if let Some(bloom_index_state) = bloom_index_state {
                    write_data(
                        bloom_index_state.data,
                        &self.dal,
                        &bloom_index_state.location.0,
                    )
                    .await?;
                }
                let block_meta = Arc::new(serialized.block_meta);
                self.state = State::Output(Mutation::Replaced(block_meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
