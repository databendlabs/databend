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
use std::sync::Arc;

use common_datablocks::BlockMetaInfos;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use common_storages_table_meta::meta::Versioned;
use itertools::Itertools;

use crate::operations::mutation::compact::CompactSinkMeta;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::BlockCompactMutator;
use crate::operations::mutation::MutationMeta;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::statistics::reducers::merge_statistics_mut;

pub struct MergeSegmentsTransform {
    // The order of the unchanged segments in snapshot.
    pub unchanged_segment_indices: Vec<usize>,
    // locations all the unchanged segments.
    pub unchanged_segment_locations: Vec<Location>,
    // summarised statistics of all the unchanged segments
    pub unchanged_segment_statistics: Statistics,
    abort_operation: AbortOperation,

    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    cur_input_index: usize,
    input_metas: BlockMetaInfos,
    output_data: Option<DataBlock>,
}

impl MergeSegmentsTransform {
    pub fn try_create(
        mutator: BlockCompactMutator,
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(MergeSegmentsTransform {
            unchanged_segment_indices: mutator.unchanged_segment_indices,
            unchanged_segment_locations: mutator.unchanged_segment_locations,
            unchanged_segment_statistics: mutator.unchanged_segment_statistics,
            abort_operation: AbortOperation::default(),
            inputs,
            output,
            cur_input_index: 0,
            input_metas: Vec::new(),
            output_data: None,
        })))
    }

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
}

#[async_trait::async_trait]
impl Processor for MergeSegmentsTransform {
    fn name(&self) -> String {
        "MergeSegmentsTransform".to_string()
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
                return Ok(Event::Sync);
            }

            let input_meta = cur_input
                .pull_data()
                .unwrap()?
                .get_meta()
                .cloned()
                .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;
            self.input_metas.push(input_meta);
            cur_input.set_need_data();
        }
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let metas = std::mem::take(&mut self.input_metas);
        let mut merged_segments = std::mem::take(&mut self.unchanged_segment_locations);
        let mut merged_statistics = std::mem::take(&mut self.unchanged_segment_statistics);
        let mut merged_indices = std::mem::take(&mut self.unchanged_segment_indices);
        for v in metas.iter() {
            let meta = CompactSinkMeta::from_meta(v)?;
            self.abort_operation.merge(&meta.abort_operation);
            merged_segments.push((meta.segment_location.clone(), SegmentInfo::VERSION));
            merged_indices.push(meta.order);
            merge_statistics_mut(&mut merged_statistics, &meta.segment_info.summary)?;
        }
        merged_segments = merged_segments
            .into_iter()
            .zip(merged_indices.iter())
            .sorted_by_key(|&(_, r)| *r)
            .map(|(l, _)| l)
            .collect();

        let meta = MutationMeta::create(
            merged_segments,
            merged_statistics,
            std::mem::take(&mut self.abort_operation),
        );
        self.output_data = Some(DataBlock::empty_with_meta(meta));
        Ok(())
    }
}
