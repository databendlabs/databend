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

use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_exception::Result;
use futures::Future;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;

#[async_trait]
pub trait AsyncSink: Send {
    const NAME: &'static str;

    type ConsumeFuture<'a>: Future<Output = Result<()>> + Send
    where Self: 'a;

    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

    /// We don't use async_trait for consume method, using GAT instead to make it more static dispatchable.
    fn consume(&mut self, data_block: DataBlock) -> Self::ConsumeFuture<'_>;
}

pub struct AsyncSinker<T: AsyncSink + 'static> {
    inner: T,
    input: Arc<InputPort>,
    input_data: Option<DataBlock>,
    called_on_start: bool,
    called_on_finish: bool,
}

impl<T: AsyncSink + 'static> AsyncSinker<T> {
    pub fn create(input: Arc<InputPort>, inner: T) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(AsyncSinker {
            inner,
            input,
            input_data: None,
            called_on_start: false,
            called_on_finish: false,
        }))
    }
}

#[async_trait::async_trait]
impl<T: AsyncSink + 'static> Processor for AsyncSinker<T> {
    fn name(&self) -> &'static str {
        T::NAME
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.called_on_start {
            return Ok(Event::Async);
        }

        if self.input_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            return match !self.called_on_finish {
                true => Ok(Event::Async),
                false => Ok(Event::Finished),
            };
        }

        match self.input.has_data() {
            true => {
                self.input_data = Some(self.input.pull_data().unwrap()?);
                Ok(Event::Async)
            }
            false => {
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.inner.on_start().await?;
        } else if let Some(data_block) = self.input_data.take() {
            self.inner.consume(data_block).await?;
        } else if !self.called_on_finish {
            self.called_on_finish = true;
            self.inner.on_finish().await?;
        }

        Ok(())
    }
}
