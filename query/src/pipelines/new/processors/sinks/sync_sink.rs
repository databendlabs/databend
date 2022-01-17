use std::sync::Arc;
use common_datablocks::DataBlock;
use common_exception::Result;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};

pub trait Sink: Send {
    fn consume(&mut self, data_block: DataBlock) -> Result<()>;
}

struct SinkWrap<T: Sink> {
    inner: T,
    input: Arc<InputPort>,
    input_data: Option<DataBlock>,
}

impl<T: Sink> SinkWrap<T> {
    pub fn create(input: Arc<InputPort>, inner: T) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(SinkWrap {
            inner,
            input,
            input_data: None,
        }))
    }
}

#[async_trait::async_trait]
impl<T: Sink> Processor for SinkWrap<T> {
    fn event(&mut self) -> Result<Event> {
        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.finish() {
            return Ok(Event::Finished);
        }

        match self.input.has_data() {
            true => {
                self.input_data = Some(self.input.pull_data().unwrap()?);
                Ok(Event::Sync)
            }
            false => {
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data) = self.input_data.take() {
            self.inner.consume(data)?;
        }

        Ok(())
    }
}

