//  Copyright 2023 Datafuse Labs.
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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use opendal::Operator;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ClusterStatistics;
use storages_common_table_meta::table::TableCompression;

use crate::io::write_block;
use crate::io::write_data;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::operations::mutation::Mutation;
use crate::operations::mutation::MutationTransformMeta;
use crate::operations::mutation::SerializeDataMeta;
use crate::operations::BloomIndexState;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::gen_columns_statistics;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

pub struct SerializeState {
    pub block_data: Vec<u8>,
    pub block_location: String,
    pub index_data: Option<Vec<u8>>,
    pub index_location: Option<String>,
}

enum State {
    Consume,
    NeedSerialize(DataBlock),
    Serialized(SerializeState, Arc<BlockMeta>),
    Output(Mutation),
}

pub struct SerializeDataTransform {
    state: State,
    ctx: Arc<dyn TableContext>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    write_settings: WriteSettings,

    location_gen: TableMetaLocationGenerator,
    dal: Operator,
    cluster_stats_gen: ClusterStatsGenerator,

    schema: TableSchemaRef,
    index: BlockMetaIndex,
    origin_stats: Option<ClusterStatistics>,
    table_compression: TableCompression,
}

impl SerializeDataTransform {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(SerializeDataTransform {
            state: State::Consume,
            ctx: ctx.clone(),
            input,
            output,
            output_data: None,
            location_gen: table.meta_location_generator().clone(),
            dal: table.get_operator(),
            write_settings: table.get_write_settings(),
            cluster_stats_gen,
            schema: table.schema(),
            index: BlockMetaIndex::default(),
            origin_stats: None,
            table_compression: table.table_compression,
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
        if matches!(self.state, State::NeedSerialize(_) | State::Output(_)) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::Serialized(_, _)) {
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
            self.origin_stats = meta.cluster_stats.clone();
            if input_data.is_empty() {
                self.state = State::Output(Mutation::Deleted);
            } else {
                self.state = State::NeedSerialize(input_data);
            }
        } else {
            self.state = State::Output(Mutation::DoNothing);
        }
        Ok(Event::Sync)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::NeedSerialize(block) => {
                let cluster_stats = self
                    .cluster_stats_gen
                    .gen_with_origin_stats(&block, std::mem::take(&mut self.origin_stats))?;

                let row_count = block.num_rows() as u64;
                let block_size = block.memory_size() as u64;
                let (block_location, block_id) = self.location_gen.gen_block_location();

                // build block index.
                let location = self.location_gen.block_bloom_index_location(&block_id);
                let bloom_index_state = BloomIndexState::try_create(
                    self.ctx.clone(),
                    self.schema.clone(),
                    &block,
                    location,
                )?;
                let column_distinct_count = bloom_index_state
                    .as_ref()
                    .map(|i| i.column_distinct_count.clone());
                let col_stats =
                    gen_columns_statistics(&block, column_distinct_count, &self.schema)?;

                // serialize data block.
                let mut block_data = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
                let schema = self.schema.clone();

                let (file_size, col_metas) =
                    write_block(&self.write_settings, &schema, block, &mut block_data)?;

                let (index_data, index_location, index_size) =
                    if let Some(bloom_index_state) = bloom_index_state {
                        (
                            Some(bloom_index_state.data.clone()),
                            Some(bloom_index_state.location.clone()),
                            bloom_index_state.size,
                        )
                    } else {
                        (None, None, 0u64)
                    };

                // new block meta.
                let new_meta = Arc::new(BlockMeta::new(
                    row_count,
                    block_size,
                    file_size,
                    col_stats,
                    col_metas,
                    cluster_stats,
                    block_location.clone(),
                    index_location.clone(),
                    index_size,
                    self.table_compression.into(),
                ));

                self.state = State::Serialized(
                    SerializeState {
                        block_data,
                        block_location: block_location.0,
                        index_data,
                        index_location: index_location.map(|l| l.0),
                    },
                    new_meta,
                );
            }
            State::Output(op) => {
                let meta = MutationTransformMeta::create(self.index.clone(), op);
                self.output_data = Some(DataBlock::empty_with_meta(meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Serialized(serialize_state, block_meta) => {
                // write block data.
                write_data(
                    serialize_state.block_data,
                    &self.dal,
                    &serialize_state.block_location,
                )
                .await?;
                // write index data.
                if let (Some(index_data), Some(index_location)) =
                    (serialize_state.index_data, serialize_state.index_location)
                {
                    write_data(index_data, &self.dal, &index_location).await?;
                }

                self.state = State::Output(Mutation::Replaced(block_meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
