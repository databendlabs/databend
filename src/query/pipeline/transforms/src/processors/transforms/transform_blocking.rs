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
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

pub trait BlockingTransform: Send {
    const NAME: &'static str;

    fn consume(&mut self, block: DataBlock) -> Result<()>;

    fn transform(&mut self) -> Result<Option<DataBlock>>;
}

/// A transform may be blocked on a certain input.
///
/// This transform will not pull new data from the input until the inner transform returns [None].
pub struct BlockingTransformer<T: BlockingTransform + 'static> {
    inner: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    need_data: bool,
}

impl<T: BlockingTransform + 'static> BlockingTransformer<T> {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(Self {
            inner,
            input,
            output,
            input_data: None,
            output_data: None,
            need_data: true,
        })
    }
}

#[async_trait::async_trait]
impl<T: BlockingTransform + 'static> Processor for BlockingTransformer<T> {
    fn name(&self) -> String {
        String::from(T::NAME)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output) = self.output_data.take() {
            self.output.push_data(Ok(output));
            return Ok(Event::NeedConsume);
        }

        if !self.need_data {
            // There is data needed to be transformed.
            return Ok(Event::Sync);
        }

        // The data is fully consumed, we can begin to consume new data.
        if self.input.has_data() {
            let data = self.input.pull_data().unwrap()?;
            self.input_data = Some(data);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(input) = self.input_data.take() {
            debug_assert!(self.need_data);
            self.inner.consume(input)?;
        }

        if let Some(block) = self.inner.transform()? {
            self.output_data = Some(block);
            self.need_data = false;
        } else {
            self.need_data = true;
        }

        Ok(())
    }
}
