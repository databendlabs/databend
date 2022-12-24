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
use crate::operations::read::native_data_source::{DataChunks, NativeDataSourceMeta};

pub struct ReadNativeDataSource<const BLOCKING_IO: bool> {
    finished: bool,
    batch_size: usize,
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,

    output: Arc<OutputPort>,
    output_data: Option<(Vec<PartInfoPtr>, Vec<DataChunks>)>,
}

impl ReadNativeDataSource<true> {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>, block_reader: Arc<BlockReader>) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        SyncSourcer::create(ctx.clone(), output.clone(), ReadNativeDataSource::<true> {
            ctx,
            output,
            batch_size,
            block_reader,
            finished: false,
            output_data: None,
        })
    }
}

impl ReadNativeDataSource<false> {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>, block_reader: Arc<BlockReader>) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        Ok(ProcessorPtr::create(Box::new(ReadNativeDataSource::<false> {
            ctx,
            output,
            batch_size,
            block_reader,
            finished: false,
            output_data: None,
        })))
    }
}

impl SyncSource for ReadNativeDataSource<true> {
    const NAME: &'static str = "SyncReadNativeDataSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.ctx.try_get_part() {
            None => Ok(None),
            Some(part) => Ok(Some(DataBlock::empty_with_meta(
                NativeDataSourceMeta::create(
                    vec![part.clone()],
                    vec![self.block_reader.sync_read_native_columns_data(part)?],
                )
            ))),
        }
    }
}

#[async_trait::async_trait]
impl Processor for ReadNativeDataSource<false> {
    fn name(&self) -> String {
        String::from("AsyncReadNativeDataSource")
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
            let output = DataBlock::empty_with_meta(NativeDataSourceMeta::create(part, data));
            self.output.push_data(Ok(output));
            // return Ok(Event::NeedConsume);
        }

        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> Result<()> {
        let mut parts = Vec::with_capacity(self.batch_size);
        let mut chunks = Vec::with_capacity(self.batch_size);

        for _index in 0..self.batch_size {
            if let Some(part) = self.ctx.try_get_part() {
                parts.push(part.clone());
                let block_reader = self.block_reader.clone();

                chunks.push(async move {
                    let handler = common_base::base::tokio::spawn(async move {
                        block_reader.async_read_native_columns_data(part).await
                    });
                    handler.await.unwrap()
                });
            }
        }

        if !parts.is_empty() {
            self.output_data = Some((parts, futures::future::try_join_all(chunks).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
