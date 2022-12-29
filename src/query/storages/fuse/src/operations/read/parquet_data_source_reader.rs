//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_sources::processors::sources::SyncSource;
use common_pipeline_sources::processors::sources::SyncSourcer;

use crate::io::BlockReader;
use crate::operations::read::parquet_data_source::DataSourceMeta;
use crate::MergeIOReadResult;

pub struct ReadParquetDataSource<const BLOCKING_IO: bool> {
    finished: bool,
    ctx: Arc<dyn TableContext>,
    batch_size: usize,
    block_reader: Arc<BlockReader>,

    output: Arc<OutputPort>,
    output_data: Option<(Vec<PartInfoPtr>, Vec<MergeIOReadResult>)>,
}

impl ReadParquetDataSource<true> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
    ) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        SyncSourcer::create(ctx.clone(), output.clone(), ReadParquetDataSource::<true> {
            ctx,
            output,
            batch_size,
            block_reader,
            finished: false,
            output_data: None,
        })
    }
}

impl ReadParquetDataSource<false> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
    ) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        Ok(ProcessorPtr::create(Box::new(ReadParquetDataSource::<
            false,
        > {
            ctx,
            output,
            batch_size,
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
            Some(part) => Ok(Some(DataBlock::empty_with_meta(DataSourceMeta::create(
                vec![part.clone()],
                vec![self.block_reader.sync_read_columns_data(&self.ctx, part)?],
            )))),
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
            // return Ok(Event::NeedConsume);
        }

        Ok(Event::Async)
    }

    async fn async_process(&mut self) -> Result<()> {
        let parts = self.ctx.try_get_parts(self.batch_size);

        if !parts.is_empty() {
            let mut chunks = Vec::with_capacity(parts.len());
            for part in &parts {
                let part = part.clone();
                let ctx = self.ctx.clone();
                let block_reader = self.block_reader.clone();

                chunks.push(async move {
                    tokio::spawn(async move {
                        block_reader.read_columns_data_by_merge_io(ctx, part).await
                    })
                    .await
                    .unwrap()
                });
            }

            self.output_data = Some((parts, futures::future::try_join_all(chunks).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
