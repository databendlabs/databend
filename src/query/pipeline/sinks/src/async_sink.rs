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

use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;

#[async_trait]
pub trait AsyncSink: Send {
    const NAME: &'static str;

    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

    #[unboxed_simple]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool>;

    fn details_status(&self) -> Option<String> {
        None
    }
}

pub struct AsyncSinker<T: AsyncSink + 'static> {
    inner: Option<T>,
    finished: bool,
    input: Arc<InputPort>,
    input_data: Option<DataBlock>,
    called_on_start: bool,
    called_on_finish: bool,
}

impl<T: AsyncSink + 'static> AsyncSinker<T> {
    pub fn create(input: Arc<InputPort>, inner: T) -> Box<dyn Processor> {
        Box::new(AsyncSinker {
            input,
            finished: false,
            input_data: None,
            inner: Some(inner),
            called_on_start: false,
            called_on_finish: false,
        })
    }
}

impl<T: AsyncSink + 'static> Drop for AsyncSinker<T> {
    fn drop(&mut self) {
        drop_guard(move || {
            if !self.called_on_start || !self.called_on_finish {
                if let Some(mut inner) = self.inner.take() {
                    GlobalIORuntime::instance().spawn({
                        let called_on_start = self.called_on_start;
                        let called_on_finish = self.called_on_finish;
                        async move {
                            if !called_on_start {
                                let _ = inner.on_start().await;
                            }

                            if !called_on_finish {
                                let _ = inner.on_finish().await;
                            }
                        }
                    });
                }
            }
        })
    }
}

#[async_trait::async_trait]
impl<T: AsyncSink + 'static> Processor for AsyncSinker<T> {
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

        if self.input_data.is_some() {
            // Wake up upstream while executing async work
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Async);
        }

        if self.finished {
            if !self.called_on_finish {
                return Ok(Event::Async);
            }

            self.input.finish();
            return Ok(Event::Finished);
        }

        if self.input.is_finished() {
            return match !self.called_on_finish {
                true => Ok(Event::Async),
                false => Ok(Event::Finished),
            };
        }

        match self.input.has_data() {
            true => {
                // Wake up upstream while executing async work
                self.input_data = Some(self.input.pull_data().unwrap()?);
                self.input.set_need_data();
                Ok(Event::Async)
            }
            false => {
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.inner.as_mut().unwrap().on_start().await?;
        } else if let Some(data_block) = self.input_data.take() {
            self.finished = self.inner.as_mut().unwrap().consume(data_block).await?;
        } else if !self.called_on_finish {
            self.called_on_finish = true;
            self.inner.as_mut().unwrap().on_finish().await?;
        }

        Ok(())
    }

    fn details_status(&self) -> Option<String> {
        self.inner.as_ref().and_then(|x| x.details_status())
    }
}
