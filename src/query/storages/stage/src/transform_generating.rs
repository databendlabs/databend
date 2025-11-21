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
use std::mem;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
pub type DataBlockIterator = Box<dyn Iterator<Item = Result<DataBlock>> + Send>;

// type DataBlockIteratorBuilder = Box<dyn Fn(DataBlock) -> Result<DataBlockIterator> + Send>;

pub(crate) trait DataBlockIteratorBuilder: Send + 'static {
    const NAME: &'static str;
    fn to_iter(&self, block: DataBlock) -> Result<DataBlockIterator>;
}

pub struct GeneratingTransformer<T> {
    block_iterator_builder: T,
    block_iter: Option<DataBlockIterator>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
}

impl<T: DataBlockIteratorBuilder> GeneratingTransformer<T> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_iterator_builder: T,
    ) -> Self {
        Self {
            block_iterator_builder,
            block_iter: None,
            input,
            output,
            input_data: None,
            output_data: None,
        }
    }
}

#[async_trait::async_trait]
impl<T> Processor for GeneratingTransformer<T>
where T: DataBlockIteratorBuilder
{
    fn name(&self) -> String {
        T::NAME.to_string()
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

        if let Some(data_block) = mem::take(&mut self.output_data) {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.block_iter.is_some() || self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
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
        if self.block_iter.is_none() {
            if let Some(data_block) = self.input_data.take() {
                self.block_iter = Some(self.block_iterator_builder.to_iter(data_block)?);
            }
        }
        if let Some(block_iter) = self.block_iter.as_mut() {
            if let Some(block) = block_iter.next() {
                self.output_data = Some(block?);
            } else {
                self.block_iter = None;
            }
        };
        Ok(())
    }
}
