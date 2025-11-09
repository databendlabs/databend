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

use crate::core::Event;
use crate::core::InputPort;
use crate::core::Processor;

/// Sink with multiple inputs.
pub trait SyncMpscSink: Send {
    const NAME: &'static str;

    fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<bool>;
}

pub struct SyncMpscSinker<T: SyncMpscSink + 'static> {
    inner: T,
    finished: bool,
    inputs: Vec<Arc<InputPort>>,
    input_data: Option<DataBlock>,

    cur_input_index: usize,
    called_on_start: bool,
    called_on_finish: bool,
}

impl<T: SyncMpscSink + 'static> SyncMpscSinker<T> {
    pub fn create(inputs: Vec<Arc<InputPort>>, inner: T) -> Box<dyn Processor> {
        for input in &inputs {
            input.set_need_data();
        }
        Box::new(SyncMpscSinker {
            inner,
            inputs,
            finished: false,
            input_data: None,
            cur_input_index: 0,
            called_on_start: false,
            called_on_finish: false,
        })
    }

    fn get_current_input(&mut self) -> Option<Arc<InputPort>> {
        let mut finished = true;
        let mut index = self.cur_input_index;

        loop {
            let input = &self.inputs[index];

            if !input.is_finished() {
                finished = false;

                if input.has_data() {
                    self.cur_input_index = index;
                    return Some(input.clone());
                }
            }

            index += 1;
            if index == self.inputs.len() {
                index = 0;
            }

            if index == self.cur_input_index {
                return match finished {
                    true => Some(input.clone()),
                    false => None,
                };
            }
        }
    }

    fn finish_inputs(&mut self) {
        for input in &self.inputs {
            input.finish();
        }
    }
}

#[async_trait::async_trait]
impl<T: SyncMpscSink + 'static> Processor for SyncMpscSinker<T> {
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

        if self.finished {
            if !self.called_on_finish {
                return Ok(Event::Sync);
            }

            self.finish_inputs();
            return Ok(Event::Finished);
        }

        match self.get_current_input() {
            Some(input) => {
                if input.is_finished() {
                    // All finished
                    if self.called_on_finish {
                        Ok(Event::Finished)
                    } else {
                        Ok(Event::Sync)
                    }
                } else {
                    let block = input.pull_data().ok_or_else(|| {
                        databend_common_exception::ErrorCode::Internal(
                            "Failed to pull data from input port in sync mpsc sink",
                        )
                    })??;
                    self.input_data = Some(block);
                    Ok(Event::Sync)
                }
            }
            None => Ok(Event::NeedData),
        }
    }

    fn process(&mut self) -> Result<()> {
        if !self.called_on_start {
            self.called_on_start = true;
            self.inner.on_start()?;
        } else if let Some(data_block) = self.input_data.take() {
            self.finished = self.inner.consume(data_block)?;
        } else if !self.called_on_finish {
            self.called_on_finish = true;
            self.inner.on_finish()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use databend_common_expression::DataBlock;

    use crate::core::port::connect;
    use crate::core::Event;
    use crate::core::InputPort;
    use crate::core::OutputPort;
    use crate::sinks::sync_mpsc_sink::SyncMpscSink;
    use crate::sinks::sync_mpsc_sink::SyncMpscSinker;

    struct TestSink {
        count: Arc<AtomicUsize>,
    }

    impl TestSink {
        fn create(count: Arc<AtomicUsize>) -> Self {
            Self { count }
        }
    }

    impl SyncMpscSink for TestSink {
        const NAME: &'static str = "TestSink";

        fn on_start(&mut self) -> databend_common_exception::Result<()> {
            self.count.store(1, Ordering::SeqCst);
            Ok(())
        }

        fn on_finish(&mut self) -> databend_common_exception::Result<()> {
            self.count.fetch_add(10, Ordering::SeqCst);
            Ok(())
        }

        fn consume(&mut self, data_block: DataBlock) -> databend_common_exception::Result<bool> {
            self.count
                .fetch_add(data_block.num_rows(), Ordering::SeqCst);
            Ok(false)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_sync_mpsc_sink() -> databend_common_exception::Result<()> {
        let input1 = InputPort::create();
        let input2 = InputPort::create();

        let count = Arc::new(AtomicUsize::new(0));

        let mut sink = SyncMpscSinker::create(
            vec![input1.clone(), input2.clone()],
            TestSink::create(count.clone()),
        );

        let upstream_output1 = OutputPort::create();
        let upstream_output2 = OutputPort::create();

        unsafe {
            connect(&input1, &upstream_output1);
            connect(&input2, &upstream_output2);
        }

        upstream_output1.push_data(Ok(DataBlock::new(vec![], 1)));
        upstream_output2.push_data(Ok(DataBlock::new(vec![], 2)));

        // on start
        matches!(sink.event()?, Event::Sync);
        sink.process()?;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        // consume block1
        matches!(sink.event()?, Event::Sync);
        sink.process()?;
        assert_eq!(count.load(Ordering::SeqCst), 2);
        // consume block2
        matches!(sink.event()?, Event::Sync);
        sink.process()?;
        assert_eq!(count.load(Ordering::SeqCst), 4);
        // on finish
        matches!(sink.event()?, Event::Sync);
        sink.process()?;
        assert_eq!(count.load(Ordering::SeqCst), 14);
        // finished
        matches!(sink.event()?, Event::Finished);

        Ok(())
    }
}
