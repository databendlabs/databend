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

use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;

pub trait Sink: Send {
    const NAME: &'static str;

    fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

    fn interrupt(&self) {}

    fn consume(&mut self, data_block: DataBlock) -> Result<()>;
}

pub struct Sinker<T: Sink + 'static> {
    inner: T,
    input: Arc<InputPort>,
    input_data: Option<DataBlock>,
    called_on_start: bool,
    called_on_finish: bool,
}

impl<T: Sink + 'static> Drop for Sinker<T> {
    fn drop(&mut self) {
        drop_guard(move || {
            if !self.called_on_finish {
                self.called_on_finish = true;
                let _ = self.inner.on_finish();
            }
        })
    }
}

impl<T: Sink + 'static> Sinker<T> {
    pub fn create(input: Arc<InputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(Sinker {
            inner,
            input,
            input_data: None,
            called_on_start: false,
            called_on_finish: false,
        })
    }
}

#[async_trait::async_trait]
impl<T: Sink + 'static> Processor for Sinker<T> {
    fn name(&self) -> String {
        T::NAME.to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.called_on_start {
            return Ok(Event::Sync);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            return match !self.called_on_finish {
                true => Ok(Event::Sync),
                false => Ok(Event::Finished),
            };
        }

        match self.input.has_data() {
            true => {
                let data = self.input.pull_data().ok_or_else(|| {
                    databend_common_exception::ErrorCode::Internal(
                        "Failed to pull data from input port in sync sink",
                    )
                })??;
                self.input_data = Some(data);
                Ok(Event::Sync)
            }
            false => {
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn interrupt(&self) {
        self.inner.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.inner.on_start()?;
        } else if let Some(data_block) = self.input_data.take() {
            self.inner.consume(data_block)?;
        } else if !self.called_on_finish {
            self.called_on_finish = true;
            self.inner.on_finish()?;
        }

        Ok(())
    }
}
