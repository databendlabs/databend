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
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::core::Event;
use crate::core::InputPort;
use crate::core::Processor;

#[async_trait]
pub trait AsyncSink: Send {
    const NAME: &'static str;
    const CALL_ON_START_ON_ERROR: bool = true;
    const CALL_ON_FINISH_ON_ERROR: bool = true;

    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

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
                            if !called_on_start && T::CALL_ON_START_ON_ERROR {
                                let _ = inner.on_start().await;
                            }

                            if !called_on_finish && T::CALL_ON_FINISH_ON_ERROR {
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
                let data = self.input.pull_data().ok_or_else(|| {
                    databend_common_exception::ErrorCode::Internal(
                        "Failed to pull data from input port in async sink",
                    )
                })??;
                self.input_data = Some(data);
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
            self.inner
                .as_mut()
                .ok_or_else(|| {
                    databend_common_exception::ErrorCode::Internal(
                        "AsyncSink inner is None when calling on_start",
                    )
                })?
                .on_start()
                .await?;
        } else if let Some(data_block) = self.input_data.take() {
            self.finished = self
                .inner
                .as_mut()
                .ok_or_else(|| {
                    databend_common_exception::ErrorCode::Internal(
                        "AsyncSink inner is None when consuming data",
                    )
                })?
                .consume(data_block)
                .await?;
        } else if !self.called_on_finish {
            let result = self
                .inner
                .as_mut()
                .ok_or_else(|| {
                    databend_common_exception::ErrorCode::Internal(
                        "AsyncSink inner is None when calling on_finish",
                    )
                })?
                .on_finish()
                .await;
            self.called_on_finish = true;
            result?;
        }

        Ok(())
    }

    fn details_status(&self) -> Option<String> {
        self.inner.as_ref().and_then(|x| x.details_status())
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;
    use databend_common_expression::DataBlock;
    use tokio::sync::oneshot;

    use super::AsyncSink;
    use super::AsyncSinker;
    use crate::core::InputPort;
    use crate::core::Processor;

    struct BlockingFinishSink {
        finish_entered_tx: Option<oneshot::Sender<()>>,
        finish_release_rx: Option<oneshot::Receiver<()>>,
    }

    #[async_trait]
    impl AsyncSink for BlockingFinishSink {
        const NAME: &'static str = "BlockingFinishSink";

        async fn on_finish(&mut self) -> Result<()> {
            if let Some(tx) = self.finish_entered_tx.take() {
                let _ = tx.send(());
            }

            if let Some(rx) = self.finish_release_rx.take() {
                let _ = rx.await;
            }

            Ok(())
        }

        async fn consume(&mut self, _data_block: DataBlock) -> Result<bool> {
            Ok(false)
        }
    }

    struct ErrorFinishSink;

    #[async_trait]
    impl AsyncSink for ErrorFinishSink {
        const NAME: &'static str = "ErrorFinishSink";

        async fn on_finish(&mut self) -> Result<()> {
            Err(ErrorCode::Internal("finish failed"))
        }

        async fn consume(&mut self, _data_block: DataBlock) -> Result<bool> {
            Ok(false)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_cancel_pending_on_finish_does_not_mark_finish_called() -> Result<()> {
        let (finish_entered_tx, finish_entered_rx) = oneshot::channel();
        let (_finish_release_tx, finish_release_rx) = oneshot::channel();

        let sink = Box::leak(Box::new(AsyncSinker {
            inner: Some(BlockingFinishSink {
                finish_entered_tx: Some(finish_entered_tx),
                finish_release_rx: Some(finish_release_rx),
            }),
            finished: false,
            input: InputPort::create(),
            input_data: None,
            called_on_start: true,
            called_on_finish: false,
        }));

        let mut finish_future = Box::pin(sink.async_process());

        tokio::select! {
            entered = finish_entered_rx => {
                entered.expect("on_finish should be entered");
            }
            result = &mut finish_future => {
                panic!("on_finish should remain pending, got {result:?}");
            }
        }

        drop(finish_future);

        assert!(
            !sink.called_on_finish,
            "pending on_finish must not be recorded as completed"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_on_finish_error_marks_finish_called() -> Result<()> {
        let sink = Box::leak(Box::new(AsyncSinker {
            inner: Some(ErrorFinishSink),
            finished: false,
            input: InputPort::create(),
            input_data: None,
            called_on_start: true,
            called_on_finish: false,
        }));

        let result = sink.async_process().await;

        assert!(result.is_err(), "on_finish should propagate its error");
        assert!(
            sink.called_on_finish,
            "completed on_finish errors must be recorded as attempted"
        );

        Ok(())
    }
}
