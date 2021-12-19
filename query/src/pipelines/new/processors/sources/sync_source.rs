use std::cell::UnsafeCell;
use std::sync::Arc;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{PrepareState, ProcessorPtr};
use common_exception::Result;

/// Synchronized source. such as:
///     - Memory storage engine.
///     - SELECT * FROM numbers_mt(1000)
pub trait SyncSource {
    fn generate(&mut self) -> Result<Option<DataBlock>>;
}

// TODO: This can be refactored using proc macros
pub struct SyncSourceProcessorWrap<T: 'static + SyncSource> {
    is_finish: bool,
    best_push_pos: usize,

    inner: T,
    outputs: Vec<OutputPort>,
    generated_data: Option<DataBlock>,
}

impl<T: 'static + SyncSource> SyncSourceProcessorWrap<T> {
    pub fn create(mut outputs: Vec<OutputPort>, inner: T) -> Result<ProcessorPtr> {
        match outputs.len() {
            0 => Err(ErrorCode::LogicalError("Source output port is empty.")),
            1 => Ok(SingleOutputSyncSourceProcessorWrap::create(outputs.remove(0), inner)),
            _ => Ok(Arc::new(UnsafeCell::new(Self {
                inner,
                outputs,
                is_finish: false,
                best_push_pos: 0,
                generated_data: None,
            })))
        }
    }

    #[inline(always)]
    fn push_data_to_output(&mut self, index: usize) {
        if let Some(generated_data) = self.generated_data.take() {
            self.outputs[index].push_data(Ok(generated_data));
            self.best_push_pos += 1;
        }
    }

    #[inline(always)]
    fn push_data_to_outputs(&mut self) -> bool {
        let mut outputs_finished = true;
        let mut index = self.best_push_pos;

        loop {
            if self.outputs[index].can_push() {
                self.push_data_to_output(index);
                return false;
            }

            if outputs_finished && !self.outputs[index].is_finished() {
                outputs_finished = false;
            }

            match index >= self.outputs.len() {
                true => index = 0,
                false => index += 1,
            }

            if index == self.best_push_pos {
                return outputs_finished;
            }
        }
    }

    #[inline(always)]
    fn close_outputs(&mut self) -> PrepareState {
        if !self.is_finish {
            self.is_finish = true;
        }

        for output_port in &self.outputs {
            if !output_port.is_finished() {
                output_port.finish();
            }
        }

        PrepareState::Finished
    }
}

#[async_trait::async_trait]
impl<T: 'static + SyncSource> Processor for SyncSourceProcessorWrap<T> {
    fn prepare(&mut self) -> Result<PrepareState> {
        Ok(match &self.generated_data {
            None if self.is_finish => self.close_outputs(),
            None => PrepareState::Sync,
            Some(_) => match self.push_data_to_outputs() {
                true => self.close_outputs(),
                false => PrepareState::NeedConsume,
            },
        })
    }

    fn process(&mut self) -> Result<()> {
        match self.inner.generate()? {
            None => self.is_finish = true,
            Some(data_block) => self.generated_data = Some(data_block),
        };

        Ok(())
    }
}

/// The optimization of SyncSourceProcessorWrap.
/// It's used when source has only one output port.
struct SingleOutputSyncSourceProcessorWrap<T: 'static + SyncSource> {
    is_finish: bool,

    inner: T,
    output: OutputPort,
    generated_data: Option<DataBlock>,
}

impl<T: 'static + SyncSource> SingleOutputSyncSourceProcessorWrap<T> {
    pub fn create(output: OutputPort, inner: T) -> ProcessorPtr {
        Arc::new(UnsafeCell::new(
            Self {
                inner,
                output,
                is_finish: false,
                generated_data: None,
            }
        ))
    }

    #[inline(always)]
    fn push_data(&mut self) -> PrepareState {
        if let Some(generated_data) = self.generated_data.take() {
            self.output.push_data(Ok(generated_data));
        }

        PrepareState::NeedConsume
    }

    #[inline(always)]
    fn close_output(&mut self) -> PrepareState {
        if !self.is_finish {
            self.is_finish = true;
        }

        if !self.output.is_finished() {
            self.output.finish();
        }

        PrepareState::Finished
    }
}

#[async_trait::async_trait]
impl<T: 'static + SyncSource> Processor for SingleOutputSyncSourceProcessorWrap<T> {
    fn prepare(&mut self) -> Result<PrepareState> {
        Ok(match &self.generated_data {
            None if self.is_finish => self.close_output(),
            None => PrepareState::Sync,
            Some(_) if self.output.can_push() => self.push_data(),
            Some(_) if self.output.is_finished() => self.close_output(),
            Some(_) => PrepareState::NeedConsume,
        })
    }

    fn process(&mut self) -> Result<()> {
        match self.inner.generate()? {
            None => self.is_finish = true,
            Some(data_block) => self.generated_data = Some(data_block),
        };

        Ok(())
    }
}
