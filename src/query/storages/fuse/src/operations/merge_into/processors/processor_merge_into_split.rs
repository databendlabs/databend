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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;

use crate::operations::merge_into::mutator::MergeIntoSplitMutator;
use crate::operations::BlockMetaIndex;

// There are two kinds of usage for this processor:
// 1. we will receive a probed datablock from join, and split it by rowid into matched block and unmatched block
// 2. we will receive a unmatched datablock, but this is an optimization for target table as build side. The unmatched
// datablock is a physical block's partial unmodified block. And its meta is a prefix(segment_id_block_id).
// we use the meta to distinct 1 and 2.
pub struct MergeIntoSplitProcessor {
    input_port: Arc<InputPort>,
    output_port_matched: Arc<OutputPort>,
    output_port_not_matched: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data_matched_data: Option<DataBlock>,
    output_data_not_matched_data: Option<DataBlock>,
    merge_into_split_mutator: MergeIntoSplitMutator,
}

impl MergeIntoSplitProcessor {
    pub fn create(split_idx: u32) -> Result<Self> {
        let merge_into_split_mutator = MergeIntoSplitMutator::try_create(split_idx);
        let input_port = InputPort::create();
        let output_port_matched = OutputPort::create();
        let output_port_not_matched = OutputPort::create();
        Ok(Self {
            input_port,
            output_port_matched,
            output_port_not_matched,
            input_data: None,
            output_data_matched_data: None,
            output_data_not_matched_data: None,
            merge_into_split_mutator,
        })
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let output_port_matched = self.output_port_matched.clone();
        let output_port_not_matched = self.output_port_not_matched.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![
            output_port_matched,
            output_port_not_matched,
        ])
    }
}

impl Processor for MergeIntoSplitProcessor {
    fn name(&self) -> String {
        "MergeIntoSplit".to_owned()
    }

    #[doc = " Reference used for downcast."]
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // 1. if there is no data and input_port is finished, this processor has finished
        // it's work
        let finished = self.input_port.is_finished()
            && self.output_data_matched_data.is_none()
            && self.output_data_not_matched_data.is_none();
        if finished {
            self.output_port_matched.finish();
            self.output_port_not_matched.finish();
            return Ok(Event::Finished);
        }

        let mut pushed_something = false;

        // 2. process data stage here
        if self.output_port_matched.can_push() {
            if let Some(matched_data) = self.output_data_matched_data.take() {
                self.output_port_matched.push_data(Ok(matched_data));
                pushed_something = true
            }
        }

        if self.output_port_not_matched.can_push() {
            if let Some(not_matched_data) = self.output_data_not_matched_data.take() {
                self.output_port_not_matched.push_data(Ok(not_matched_data));
                pushed_something = true
            }
        }

        // 3. trigger down stream pipeItem to consume if we pushed data
        if pushed_something {
            Ok(Event::NeedConsume)
        } else {
            // 4. we can't pushed data ,so the down stream is not prepared or we have no data at all
            // we need to make sure only when the all out_pudt_data are empty ,and we start to split
            // datablock held by input_data
            if self.input_port.has_data() {
                if self.output_data_matched_data.is_none()
                    && self.output_data_not_matched_data.is_none()
                {
                    // no pending data (being sent to down streams)
                    self.input_data = Some(self.input_port.pull_data().unwrap()?);
                    Ok(Event::Sync)
                } else {
                    // data pending
                    Ok(Event::NeedConsume)
                }
            } else {
                self.input_port.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            //  we receive a partial unmodified block data. please see details at the top of this file.
            if data_block.get_meta().is_some() {
                let meta_index = BlockMetaIndex::downcast_ref_from(data_block.get_meta().unwrap());
                if meta_index.is_some() {
                    // we reserve the meta in data_block to avoid adding insert `merge_status` in `merge_into_not_matched` by mistake.
                    // if `is_empty`, it's a whole block matched, we need to delete.
                    if !data_block.is_empty() {
                        self.output_data_not_matched_data = Some(data_block.clone());
                    }
                    // if the downstream receive this, it should just treat this as a DeletedLog.
                    self.output_data_matched_data = Some(DataBlock::empty_with_meta(Box::new(
                        meta_index.unwrap().clone(),
                    )));
                    return Ok(());
                }
            }

            let start = Instant::now();
            let (matched_block, not_matched_block) = self
                .merge_into_split_mutator
                .split_data_block(&data_block)?;
            let elapsed_time = start.elapsed().as_millis() as u64;
            metrics_inc_merge_into_split_milliseconds(elapsed_time);

            if !matched_block.is_empty() {
                metrics_inc_merge_into_matched_rows(matched_block.num_rows() as u32);
                self.output_data_matched_data = Some(matched_block);
            }

            if !not_matched_block.is_empty() {
                metrics_inc_merge_into_unmatched_rows(not_matched_block.num_rows() as u32);
                self.output_data_not_matched_data = Some(not_matched_block);
            }
        }
        Ok(())
    }
}
