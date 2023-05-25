use std::any::Any;
use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::io::BlockReader;
use crate::FuseTable;

pub struct ReadParquetKnnSource {
    is_finish: bool,
    ctx: Arc<dyn TableContext>,
    table: Arc<dyn Table>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    block_reader: Arc<BlockReader>,
    limit: usize,
    target: Vec<f32>,
}

impl ReadParquetKnnSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        table: Arc<dyn Table>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        limit: usize,
        target: Vec<f32>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(Self {
            is_finish: false,
            ctx,
            table,
            output,
            output_data: None,
            block_reader,
            limit,
            target,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for ReadParquetKnnSource {
    fn name(&self) -> String {
        String::from("ReadParquetKnnSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finish {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data) = self.output_data.take() {
            self.output.push_data(Ok(data));
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if self.is_finish {
            return Ok(());
        }
        let table = FuseTable::try_from_table(self.table.as_ref())?;
        self.output_data = table
            .read_by_vector_index(
                self.block_reader.clone(),
                self.ctx.clone(),
                self.limit,
                &self.target,
            )
            .await?;
        self.is_finish = true;
        Ok(())
    }
}
