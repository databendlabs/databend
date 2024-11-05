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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;

use super::processor_merge_into_matched_and_split::SourceFullMatched;
use crate::operations::merge_into::processors::RowIdKind;

// for distributed merge into (source as build and it will be broadcast)
pub struct RowNumberAndLogSplitProcessor {
    input_port: Arc<InputPort>,
    output_port_row_number: Arc<OutputPort>,
    output_port_log: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data_row_number: Option<DataBlock>,
    output_data_log: Option<DataBlock>,
}

impl RowNumberAndLogSplitProcessor {
    pub fn create() -> Result<Self> {
        Ok(Self {
            input_port: InputPort::create(),
            output_port_log: OutputPort::create(),
            output_port_row_number: OutputPort::create(),
            input_data: None,
            output_data_log: None,
            output_data_row_number: None,
        })
    }

    pub fn into_pipe(self) -> Pipe {
        let pipe_item = self.into_pipe_item();
        Pipe::create(1, 2, vec![pipe_item])
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let output_port_row_number = self.output_port_row_number.clone();
        let output_port_log = self.output_port_log.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![
            output_port_row_number,
            output_port_log,
        ])
    }
}

// we will also use RowNumberAndLogSplitProcessor to
// split rowids and logs although it's named with 'RowNumber'
impl Processor for RowNumberAndLogSplitProcessor {
    fn name(&self) -> String {
        "RowNumberAndLogSplit".to_owned()
    }

    #[doc = " Reference used for downcast."]
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // 1. if there is no data and input_port is finished, this processor has finished
        // it's work
        let finished = self.input_port.is_finished()
            && self.output_data_row_number.is_none()
            && self.output_data_log.is_none();
        if finished {
            self.output_port_row_number.finish();
            self.output_port_log.finish();
            return Ok(Event::Finished);
        }

        let mut pushed_something = false;

        // 2. process data stage here
        if self.output_port_row_number.can_push() {
            if let Some(row_number_data) = self.output_data_row_number.take() {
                self.output_port_row_number.push_data(Ok(row_number_data));
                pushed_something = true
            }
        }

        if self.output_port_log.can_push() {
            if let Some(log_data) = self.output_data_log.take() {
                self.output_port_log.push_data(Ok(log_data));
                pushed_something = true
            }
        }

        // 3. trigger down stream pipeItem to consume if we pushed data
        if pushed_something {
            Ok(Event::NeedConsume)
        } else {
            // 4. we can't pushed data ,so the down stream is not prepared or we have no data at all
            // we need to make sure only when the all output_data are empty ,and we start to split
            // datablock held by input_data
            if self.input_port.has_data() {
                if self.output_data_row_number.is_none() && self.output_data_log.is_none() {
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
            if data_block.get_meta().is_some() {
                // distributed mode: source as build side
                if SourceFullMatched::downcast_ref_from(data_block.get_meta().unwrap()).is_some()
                    // distributed mode: target as build side
                    || RowIdKind::downcast_ref_from(data_block.get_meta().unwrap()).is_some()
                {
                    self.output_data_row_number = Some(data_block)
                } else {
                    // mutation logs
                    self.output_data_log = Some(data_block);
                }
            } else {
                self.output_data_row_number = Some(data_block)
            }
        }
        Ok(())
    }
}
