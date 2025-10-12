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

use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

pub struct TransformMetaDispatcher {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    queue: Vec<AggregateMeta>,
}

impl Processor for TransformMetaDispatcher {
    fn name(&self) -> &str {
        "TransformMetaDispatcher"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> databend_common_exception::Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedData);
        }

        if let Some(meta) = self.queue.pop() {
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(Box::new(meta))));
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(block_meta) = data_block
                .take_meta()
                .and_then(AggregateMeta::downcast_from)
            {
                let AggregateMeta::Partitioned { bucket, data } = block_meta else {
                    return Err(databend_common_exception::ErrorCode::Internal(
                        "TransformMetaDispatcher only support Partitioned AggregateMeta",
                    ));
                    // TODO:
                };

                self.queue = data;

                return if let Some(meta) = self.queue.pop() {
                    self.output
                        .push_data(Ok(DataBlock::empty_with_meta(Box::new(meta))));
                    Ok(Event::NeedConsume)
                } else {
                    Err(databend_common_exception::ErrorCode::Internal(
                        "Logic error",
                    ))
                };
            }
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }
}
