use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::Result;

use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;

pub trait Sink: Send {
    const NAME: &'static str;

    fn consume(&mut self, data_block: DataBlock) -> Result<()>;
}

pub struct Sinker<T: Sink + 'static> {
    inner: T,
    input: Arc<InputPort>,
    input_data: Option<DataBlock>,
}

impl<T: Sink + 'static> Sinker<T> {
    pub fn create(input: Arc<InputPort>, inner: T) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(Sinker {
            inner,
            input,
            input_data: None,
        }))
    }
}

#[async_trait::async_trait]
impl<T: Sink + 'static> Processor for Sinker<T> {
    fn name(&self) -> &'static str {
        T::NAME
    }

    fn event(&mut self) -> Result<Event> {
        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
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
