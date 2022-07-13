use std::any::Any;
use std::sync::Arc;
use common_base::base::{Progress, ProgressValues};
use common_datablocks::DataBlock;
use common_exception::{ErrorCode, Result};
use common_planners::PartInfoPtr;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockReader;
use crate::storages::result::result_table_source::State::Generated;

enum State {
    ReadData(PartInfoPtr),
    Deserialize(PartInfoPtr, Vec<Vec<u8>>),
    Generated(Option<PartInfoPtr>, DataBlock),
    Finish,
}

pub struct ResultTableSource {
    state: State,
    ctx: Arc<QueryContext>,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,
    output: Arc<OutputPort>,
}

impl ResultTableSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let mut partitions = ctx.try_get_partitions(1)?;
        match partitions.is_empty() {
            true => Ok(ProcessorPtr::create(Box::new(ResultTableSource {
                ctx,
                output,
                block_reader,
                scan_progress,
                state: State::Finish,
            }))),
            false => Ok(ProcessorPtr::create(Box::new(ResultTableSource {
                ctx,
                output,
                block_reader,
                scan_progress,
                state: State::ReadData(partitions.remove(0)),
            }))),
        }
    }
}

#[async_trait::async_trait]
impl Processor for ResultTableSource {
    fn name(&self) -> &'static str {
        "ResultTableSource"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Generated(_, _)) {
            if let Generated(part, data_block) = std::mem::replace(&mut self.state, State::Finish) {
                self.state = match part {
                    None => State::Finish,
                    Some(part) => State::ReadData(part),
                };

                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }
        }

        match self.state {
            State::Finish => Ok(Event::Finished),
            State::ReadData(_) => Ok(Event::Async),
            State::Deserialize(_, _) => Ok(Event::Sync),
            State::Generated(_, _) => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::Deserialize(part, chunks) => {
                let data_block = self.block_reader.deserialize(part, chunks)?;
                let mut partitions = self.ctx.try_get_partitions(1)?;

                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);

                self.state = match partitions.is_empty() {
                    true => State::Generated(None, data_block),
                    false => State::Generated(Some(partitions.remove(0)), data_block),
                };
                Ok(())
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(part) => {
                let chunks = self.block_reader.read_columns_data(part.clone()).await?;
                self.state = State::Deserialize(part, chunks);
                Ok(())
            }
            _ => Err(ErrorCode::LogicalError("It's a bug.")),
        }
    }
}