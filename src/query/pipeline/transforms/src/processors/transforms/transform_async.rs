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
pub trait AsyncTransform: Send {
    const NAME: &'static str;

    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock>;

    fn name(&self) -> String {
        Self::NAME.to_string()
    }

    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct AsyncTransformer<T: AsyncTransform + 'static> {
    transform: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    called_on_start: bool,
    called_on_finish: bool,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
}

impl<T: AsyncTransform + 'static> AsyncTransformer<T> {
    pub fn create(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(Self {
            input,
            output,
            transform: inner,
            input_data: None,
            output_data: None,
            called_on_start: false,
            called_on_finish: false,
        })
    }
}

#[async_trait::async_trait]
impl<T: AsyncTransform + 'static> Processor for AsyncTransformer<T> {
    fn name(&self) -> String {
        AsyncTransform::name(&self.transform)
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.called_on_start {
            return Ok(Event::Async);
        }

        match self.output.is_finished() {
            true => self.finish_input(),
            false if !self.output.can_push() => self.not_need_data(),
            false => {
                if let Some(data) = self.output_data.take() {
                    self.output.push_data(Ok(data));
                }

                if self.input_data.is_some() {
                    return Ok(Event::Async);
                }

                self.pull_data()
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.transform.on_start().await?;
            return Ok(());
        }

        if let Some(data_block) = self.input_data.take() {
            let data_block = self.transform.transform(data_block).await?;
            self.output_data = Some(data_block);
            return Ok(());
        }

        if !self.called_on_finish {
            self.called_on_finish = true;
            self.transform.on_finish().await?;
        }

        Ok(())
    }
}

impl<T: AsyncTransform> AsyncTransformer<T> {
    fn pull_data(&mut self) -> Result<Event> {
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

    fn not_need_data(&mut self) -> Result<Event> {
        self.input.set_not_need_data();
        Ok(Event::NeedConsume)
    }

    fn finish_input(&mut self) -> Result<Event> {
        match !self.called_on_finish {
            true => Ok(Event::Async),
            false => {
                self.input.finish();
                Ok(Event::Finished)
            }
        }
    }
}
