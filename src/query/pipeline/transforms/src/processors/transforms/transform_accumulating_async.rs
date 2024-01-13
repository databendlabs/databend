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

#[async_trait::async_trait]
pub trait AsyncAccumulatingTransform: Send {
    const NAME: &'static str;

    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>>;

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        Ok(None)
    }
}

pub struct AsyncAccumulatingTransformer<T: AsyncAccumulatingTransform + 'static> {
    inner: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    called_on_finish: bool,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
}

impl<T: AsyncAccumulatingTransform + 'static> AsyncAccumulatingTransformer<T> {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(Self {
            inner,
            input,
            output,
            input_data: None,
            output_data: None,
            called_on_finish: false,
        })
    }
}

#[async_trait::async_trait]
impl<T: AsyncAccumulatingTransform + 'static> Processor for AsyncAccumulatingTransformer<T> {
    fn name(&self) -> String {
        String::from(T::NAME)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            if !self.called_on_finish {
                return Ok(Event::Async);
            }

            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            return match !self.called_on_finish {
                true => Ok(Event::Async),
                false => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            self.output_data = self.inner.transform(data_block).await?;
            return Ok(());
        }

        if !self.called_on_finish {
            self.called_on_finish = true;
            self.output_data = self.inner.on_finish(true).await?;
        }

        Ok(())
    }
}
