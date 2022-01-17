use std::sync::Arc;
use common_datablocks::DataBlock;
use common_exception::{ErrorCode, Result};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::Event;

// TODO: maybe we also need async transform for `SELECT sleep(1)`?
pub trait Transform: Send {
    fn transform(&mut self, data: DataBlock) -> Result<DataBlock>;
}

pub struct TransformWrap<T: Transform> {
    transform: T,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
}

#[async_trait::async_trait]
impl<T: Transform> Processor for TransformWrap<T> {
    fn event(&mut self) -> Result<Event> {
        match self.output.is_finished() {
            true => self.finish_input(),
            false if !self.output.can_push() => self.not_need_data(),
            false => match self.output_data.take() {
                None if self.input_data.is_some() => Ok(Event::Sync),
                None => self.pull_data(),
                Some(data) => {
                    self.output.push_data(Ok(data));
                    return Ok(Event::NeedConsume);
                }
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            self.output_data = Some(self.transform.transform(data_block)?);
        }

        Ok(())
    }
}

impl<T: Transform> TransformWrap<T> {
    fn pull_data(&mut self) -> Result<Event> {
        match self.input.is_finished() {
            true => self.finish_output(),
            false if !self.input.has_data() => self.need_data(),
            false => self.receive_input_data()
        }
    }

    fn need_data(&mut self) -> Result<Event> {
        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn not_need_data(&mut self) -> Result<Event> {
        self.input.set_not_need_data();
        Ok(Event::NeedConsume)
    }

    fn finish_input(&mut self) -> Result<Event> {
        self.input.finish();
        Ok(Event::Finished)
    }

    fn finish_output(&mut self) -> Result<Event> {
        self.output.finish();
        Ok(Event::Finished)
    }

    fn receive_input_data(&mut self) -> Result<Event> {
        self.input_data = Some(self.input.pull_data().unwrap()?);
        Ok(Event::Sync)
    }
}
