use std::sync::Arc;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::processors::processor::{Event, ProcessorPtr};
use std::any::Any;
use common_datablocks::DataBlock;
use crate::io::BlockReader;
use crate::operations::read::parquet_data_source::DataSourceMeta;
use common_exception::Result;
use common_pipeline_sources::processors::sources::{SyncSource, SyncSourcer};

pub struct ReadParquetDataSource<const BLOCKING_IO: bool> {
    finished: bool,
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,

    output: Arc<OutputPort>,
    output_data: Option<(PartInfoPtr, Vec<(usize, Vec<u8>)>)>,
}

impl ReadParquetDataSource<true> {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>, block_reader: Arc<BlockReader>) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output.clone(), ReadParquetDataSource::<true> {
            ctx,
            output,
            block_reader,
            finished: false,
            output_data: None,
        })
    }
}

impl ReadParquetDataSource<false> {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>, block_reader: Arc<BlockReader>) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(ReadParquetDataSource::<false> {
            ctx,
            output,
            block_reader,
            finished: false,
            output_data: None,
        })))
    }
}

impl SyncSource for ReadParquetDataSource<true> {
    const NAME: &'static str = "SyncReadParquetDataSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.ctx.try_get_part() {
            None => Ok(None),
            Some(part) => Ok(Some(DataBlock::empty_with_meta(
                DataSourceMeta::create(
                    part.clone(),
                    self.block_reader.sync_read_columns_data(part)?,
                )
            ))),
        }
    }
}

#[async_trait::async_trait]
impl Processor for ReadParquetDataSource<false> {
    fn name(&self) -> String {
        String::from("AsyncReadParquetDataSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some((part, data)) = self.output_data.take() {
            let output = DataBlock::empty_with_meta(DataSourceMeta::create(part, data));
            self.output.push_data(Ok(output));
            return Ok(Event::NeedConsume);
        }

        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(part) = self.ctx.try_get_part() {
            self.output_data = Some((part.clone(), self.block_reader.read_columns_data(part).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
