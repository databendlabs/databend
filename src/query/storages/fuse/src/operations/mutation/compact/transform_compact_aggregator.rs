// Copyright 2022 Datafuse Labs.
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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CacheManager;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use tracing::Instrument;

use super::compact_meta::CompactSourceMeta2;
use crate::io::try_join_futures;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::MutationSinkMeta;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

#[derive(Clone)]
struct SerializedSegment {
    location: String,
    segment: Arc<SegmentInfo>,
}

pub struct CompactAggregatorTransform {
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    // locations all the merged segments.
    pub merged_segments: BTreeMap<usize, Location>,
    // summarised statistics of all the merged segments
    pub merged_statistics: Statistics,
    merge_blocks: HashMap<usize, BTreeMap<usize, Arc<BlockMeta>>>,
    thresholds: BlockThresholds,
    abort_operation: AbortOperation,

    inputs: Vec<Arc<InputPort>>,

    cur_input_index: usize,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
}

impl CompactAggregatorTransform {
    fn get_current_input(&mut self) -> Option<Arc<InputPort>> {
        let mut finished = true;
        let mut index = self.cur_input_index;

        loop {
            let input = &self.inputs[index];

            if !input.is_finished() {
                finished = false;
                input.set_need_data();

                if input.has_data() {
                    self.cur_input_index = index;
                    return Some(input.clone());
                }
            }

            index += 1;
            if index == self.inputs.len() {
                index = 0;
            }

            if index == self.cur_input_index {
                return match finished {
                    true => Some(input.clone()),
                    false => None,
                };
            }
        }
    }

    async fn write_segment(dal: Operator, segment: SerializedSegment) -> Result<()> {
        dal.object(&segment.location)
            .write(serde_json::to_vec(&segment.segment)?)
            .await?;
        if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
            segment_cache.put(segment.location.clone(), segment.segment.clone());
        }
        Ok(())
    }

    async fn write_segments(&self, segments: &[SerializedSegment]) -> Result<()> {
        let mut iter = segments.iter();
        let tasks = std::iter::from_fn(move || {
            iter.next().map(|segment| {
                Self::write_segment(self.dal.clone(), segment.clone())
                    .instrument(tracing::debug_span!("write_segment"))
            })
        });

        try_join_futures(
            self.ctx.clone(),
            tasks,
            "compact-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for CompactAggregatorTransform {
    fn name(&self) -> String {
        "CompactAggregatorTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in &self.inputs {
                input.finish();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        let current_input = self.get_current_input();
        if let Some(cur_input) = current_input {
            if cur_input.is_finished() {
                return Ok(Event::Async);
            } else {
                let input_data = cur_input.pull_data().unwrap()?;
                if let Some(meta) = input_data
                    .get_meta()
                    .and_then(CompactSourceMeta2::downcast_ref_from)
                {
                    self.abort_operation.add_block(&meta.block);
                    self.merge_blocks
                        .entry(meta.index.segment_idx)
                        .and_modify(|v| {
                            v.insert(meta.index.block_idx, meta.block.clone());
                        })
                        .or_insert(BTreeMap::from([(meta.index.block_idx, meta.block.clone())]));
                }
                cur_input.set_need_data();
            }
        }
        Ok(Event::NeedData)
    }

    async fn async_process(&mut self) -> Result<()> {
        let mut serialized_segments = Vec::with_capacity(self.merge_blocks.len());
        for (segment_idx, block_map) in std::mem::take(&mut self.merge_blocks) {
            let blocks: Vec<_> = block_map.into_values().collect();
            let new_summary = reduce_block_metas(&blocks, self.thresholds)?;
            merge_statistics_mut(&mut self.merged_statistics, &new_summary)?;
            let new_segment = SegmentInfo::new(blocks, new_summary);
            let location = self.location_gen.gen_segment_info_location();
            self.abort_operation.add_segment(location.clone());
            self.merged_segments.insert(
                segment_idx,
                (location.clone(), new_segment.format_version()),
            );
            serialized_segments.push(SerializedSegment {
                location,
                segment: Arc::new(new_segment),
            });
        }

        let max_io_requests = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        for segments in serialized_segments.chunks(max_io_requests) {
            self.write_segments(segments).await?;
        }

        let merged_segments = std::mem::take(&mut self.merged_segments)
            .into_values()
            .collect();
        let meta = MutationSinkMeta::create(
            merged_segments,
            std::mem::take(&mut self.merged_statistics),
            std::mem::take(&mut self.abort_operation),
        );
        self.output_data = Some(DataBlock::empty_with_meta(meta));
        Ok(())
    }
}
