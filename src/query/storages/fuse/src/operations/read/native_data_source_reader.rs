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

use common_base::base::tokio;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::StealablePartitions;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_sources::SyncSource;
use common_pipeline_sources::SyncSourcer;

use crate::io::BlockReader;
use crate::operations::read::native_data_source::DataChunks;
use crate::operations::read::native_data_source::NativeDataSourceMeta;

pub struct ReadNativeDataSource<const BLOCKING_IO: bool> {
    id: usize,
    finished: bool,
    batch_size: usize,
    block_reader: Arc<BlockReader>,

    output: Arc<OutputPort>,
    output_data: Option<(Vec<PartInfoPtr>, Vec<DataChunks>)>,
    partitions: StealablePartitions,
}

impl ReadNativeDataSource<true> {
    pub fn create(
        id: usize,
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        partitions: StealablePartitions,
    ) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        SyncSourcer::create(ctx.clone(), output.clone(), ReadNativeDataSource::<true> {
            id,
            output,
            batch_size,
            block_reader,
            finished: false,
            output_data: None,
            partitions,
        })
    }
}

impl ReadNativeDataSource<false> {
    pub fn create(
        id: usize,
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        partitions: StealablePartitions,
    ) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        Ok(ProcessorPtr::create(Box::new(ReadNativeDataSource::<
            false,
        > {
            id,
            output,
            batch_size,
            block_reader,
            finished: false,
            output_data: None,
            partitions,
        })))
    }
}

impl SyncSource for ReadNativeDataSource<true> {
    const NAME: &'static str = "SyncReadNativeDataSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.partitions.steal_one(self.id) {
            None => Ok(None),
            Some(part) => Ok(Some(DataBlock::empty_with_meta(
                NativeDataSourceMeta::create(vec![part.clone()], vec![
                    self.block_reader.sync_read_native_columns_data(part)?,
                ]),
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

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let parts = self.partitions.steal(self.id, self.batch_size);

        if !parts.is_empty() {
            let mut chunks = Vec::with_capacity(parts.len());
            for part in &parts {
                let part = part.clone();
                let block_reader = self.block_reader.clone();

                chunks.push(async move {
                    let handler = tokio::spawn(async_backtrace::location!().frame(async move {
                        block_reader.async_read_native_columns_data(part).await
                    }));
                    handler.await.unwrap()
                });
            }

            self.output_data = Some((parts, futures::future::try_join_all(chunks).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
