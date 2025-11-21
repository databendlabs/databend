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
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

#[async_trait::async_trait]
pub trait HookTransform: Send + 'static {
    const NAME: &'static str;

    fn on_input(&mut self, data: DataBlock) -> Result<()>;

    fn on_output(&mut self) -> Result<Option<DataBlock>>;

    fn need_process(&self, input_finished: bool) -> Option<Event>;

    fn process(&mut self) -> Result<()> {
        unimplemented!()
    }

    async fn async_process(&mut self) -> Result<()> {
        unimplemented!()
    }
}

pub struct HookTransformer<T: HookTransform> {
    inner: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
}

impl<T: HookTransform> HookTransformer<T> {
    pub fn new(input: Arc<InputPort>, output: Arc<OutputPort>, inner: T) -> Self {
        Self {
            inner,
            input,
            output,
        }
    }
}

#[async_trait::async_trait]
impl<T: HookTransform> Processor for HookTransformer<T> {
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

        if let Some(output) = self.inner.on_output()? {
            self.output.push_data(Ok(output));
            return Ok(Event::NeedConsume);
        }

        if let Some(event) = self.inner.need_process(self.input.is_finished()) {
            return Ok(event);
        }

        if self.input.has_data() {
            let data = self.input.pull_data().unwrap()?;
            self.inner.on_input(data)?;
        }

        if let Some(event) = self.inner.need_process(self.input.is_finished()) {
            return Ok(event);
        }

        if self.input.is_finished() {
            self.output.finish();
            Ok(Event::Finished)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        self.inner.process()
    }

    async fn async_process(&mut self) -> Result<()> {
        self.inner.async_process().await
    }
}
