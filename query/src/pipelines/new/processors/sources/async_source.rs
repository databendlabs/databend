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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::Result;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;

#[async_trait::async_trait]
pub trait AsyncSource: Send {
    const NAME: &'static str;

    async fn generate(&mut self) -> Result<Option<DataBlock>>;
}

// TODO: This can be refactored using proc macros
// TODO: Most of its current code is consistent with sync. We need refactor this with better async
// scheduling after supported expand processors. It will be implemented using a similar dynamic window.
pub struct AsyncSourcer<T: 'static + AsyncSource> {
    is_finish: bool,

    inner: T,
    output: Arc<OutputPort>,
    generated_data: Option<DataBlock>,
}

impl<T: 'static + AsyncSource> AsyncSourcer<T> {
    pub fn create(output: Arc<OutputPort>, inner: T) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            inner,
            output,
            is_finish: false,
            generated_data: None,
        })))
    }

    #[inline(always)]
    fn push_data(&mut self) -> Event {
        if let Some(generated_data) = self.generated_data.take() {
            self.generated_data = None;
            self.output.push_data(Ok(generated_data));
        }

        Event::NeedConsume
    }

    #[inline(always)]
    fn close_output(&mut self) -> Event {
        if !self.is_finish {
            self.is_finish = true;
        }

        if !self.output.is_finished() {
            self.output.finish();
        }

        Event::Finished
    }
}

#[async_trait::async_trait]
impl<T: 'static + AsyncSource> Processor for AsyncSourcer<T> {
    fn name(&self) -> &'static str {
        T::NAME
    }

    fn event(&mut self) -> Result<Event> {
        Ok(match &self.generated_data {
            None if self.is_finish => self.close_output(),
            None => Event::Async,
            Some(_) if self.output.can_push() => self.push_data(),
            Some(_) if self.output.is_finished() => self.close_output(),
            Some(_) => Event::NeedConsume,
        })
    }

    async fn async_process(&mut self) -> Result<()> {
        match self.inner.generate().await? {
            None => self.is_finish = true,
            Some(data_block) => self.generated_data = Some(data_block),
        };

        Ok(())
    }
}
