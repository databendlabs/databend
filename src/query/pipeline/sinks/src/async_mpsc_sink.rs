// Copyright 2023 Datafuse Labs.
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

use async_trait::async_trait;
use async_trait::unboxed_simple;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

/// Sink with multiple inputs.
#[async_trait]
pub trait AsyncMpscSink: Send {
    const NAME: &'static str;

    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

    #[unboxed_simple]
    async fn consume(&mut self, data_block: Vec<DataBlock>) -> Result<bool>;
}

pub struct AsyncMpscSinker<T: AsyncMpscSink + 'static> {
    inner: T,
    finished: bool,
    inputs: Vec<Arc<InputPort>>,
    inputs_data: Option<Vec<DataBlock>>,
    called_on_start: bool,
    called_on_finish: bool,
}

impl<T: AsyncMpscSink + 'static> AsyncMpscSinker<T> {
    pub fn create(inputs: Vec<Arc<InputPort>>, inner: T) -> Box<dyn Processor> {
        Box::new(AsyncMpscSinker {
            inner,
            inputs,
            finished: false,
            inputs_data: None,
            called_on_start: false,
            called_on_finish: false,
        })
    }

    fn get_ready_inputs(&self) -> Option<Vec<Arc<InputPort>>> {
        let mut ready = Vec::with_capacity(self.inputs.len());
        let mut all_finished = true;
        for input in self.inputs.iter() {
            if !input.is_finished() {
                all_finished = false;
                input.set_need_data();
                if input.has_data() {
                    ready.push(input.clone());
                }
            }
        }
        if all_finished { None } else { Some(ready) }
    }

    fn finish_inputs(&mut self) {
        for input in &self.inputs {
            input.finish();
        }
    }
}

#[async_trait::async_trait]
impl<T: AsyncMpscSink + 'static> Processor for AsyncMpscSinker<T> {
    fn name(&self) -> String {
        T::NAME.to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.called_on_start {
            return Ok(Event::Async);
        }

        if self.inputs_data.is_some() {
            return Ok(Event::Async);
        }

        if self.finished {
            if !self.called_on_finish {
                return Ok(Event::Async);
            }

            self.finish_inputs();
            return Ok(Event::Finished);
        }

        match self.get_ready_inputs() {
            Some(inputs) if !inputs.is_empty() => {
                let blocks = inputs
                    .iter()
                    .map(|input| input.pull_data().unwrap())
                    .collect::<Result<Vec<_>>>()?;
                self.inputs_data = Some(blocks);
                Ok(Event::Async)
            }
            Some(_) => Ok(Event::NeedData),
            None => match !self.called_on_finish {
                // All finished
                true => Ok(Event::Async),
                false => Ok(Event::Finished),
            },
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.inner.on_start().await?;
        } else if let Some(data_block) = self.inputs_data.take() {
            self.finished = self.inner.consume(data_block).await?;
        } else if !self.called_on_finish {
            self.called_on_finish = true;
            self.inner.on_finish().await?;
        }

        Ok(())
    }
}
