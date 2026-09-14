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
pub trait AsyncAccumulatingTransform: Send {
    const NAME: &'static str;

    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>>;

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        Ok(None)
    }
}

pub struct AsyncAccumulatingTransformer<T: AsyncAccumulatingTransform + 'static> {
    inner: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    called_on_start: bool,
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
            called_on_start: false,
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
        if !self.called_on_start {
            return Ok(Event::Async);
        }

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
        if !self.called_on_start {
            self.called_on_start = true;
            self.inner.on_start().await?;
            return Ok(());
        }

        if let Some(data_block) = self.input_data.take() {
            self.output_data = self.inner.transform(data_block).await?;
            return Ok(());
        }

        if !self.called_on_finish {
            let result = self.inner.on_finish(true).await;
            self.called_on_finish = true;
            self.output_data = result?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;
    use databend_common_expression::DataBlock;
    use databend_common_pipeline::core::InputPort;
    use databend_common_pipeline::core::OutputPort;
    use databend_common_pipeline::core::Processor;
    use tokio::sync::oneshot;

    use super::AsyncAccumulatingTransform;
    use super::AsyncAccumulatingTransformer;

    struct BlockingFinishTransform {
        finish_entered_tx: Option<oneshot::Sender<()>>,
        finish_release_rx: Option<oneshot::Receiver<()>>,
    }

    #[async_trait]
    impl AsyncAccumulatingTransform for BlockingFinishTransform {
        const NAME: &'static str = "BlockingFinishTransform";

        async fn transform(&mut self, _data: DataBlock) -> Result<Option<DataBlock>> {
            Ok(None)
        }

        async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
            if let Some(tx) = self.finish_entered_tx.take() {
                let _ = tx.send(());
            }

            if let Some(rx) = self.finish_release_rx.take() {
                let _ = rx.await;
            }

            Ok(None)
        }
    }

    struct ErrorFinishTransform;

    #[async_trait]
    impl AsyncAccumulatingTransform for ErrorFinishTransform {
        const NAME: &'static str = "ErrorFinishTransform";

        async fn transform(&mut self, _data: DataBlock) -> Result<Option<DataBlock>> {
            Ok(None)
        }

        async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
            Err(ErrorCode::Internal("finish failed"))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_cancel_pending_on_finish_does_not_mark_finish_called() -> Result<()> {
        let (finish_entered_tx, finish_entered_rx) = oneshot::channel();
        let (_finish_release_tx, finish_release_rx) = oneshot::channel();

        let mut transform = AsyncAccumulatingTransformer {
            inner: BlockingFinishTransform {
                finish_entered_tx: Some(finish_entered_tx),
                finish_release_rx: Some(finish_release_rx),
            },
            input: InputPort::create(),
            output: OutputPort::create(),
            called_on_start: true,
            called_on_finish: false,
            input_data: None,
            output_data: None,
        };

        let mut finish_future = Box::pin(transform.async_process());

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
            !transform.called_on_finish,
            "pending on_finish must not be recorded as completed"
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_on_finish_error_marks_finish_called() -> Result<()> {
        let mut transform = AsyncAccumulatingTransformer {
            inner: ErrorFinishTransform,
            input: InputPort::create(),
            output: OutputPort::create(),
            called_on_start: true,
            called_on_finish: false,
            input_data: None,
            output_data: None,
        };

        let result = transform.async_process().await;

        assert!(result.is_err(), "on_finish should propagate its error");
        assert!(
            transform.called_on_finish,
            "completed on_finish errors must be recorded as attempted"
        );

        Ok(())
    }
}
