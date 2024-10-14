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

use async_channel::Receiver;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use log::debug;

use super::native_data_source::NativeDataSource;
use crate::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub struct ReadNativeDataSourceWithChannel<const BLOCKING_IO: bool> {
    meta_receiver: Receiver<Partitions>,
    index_reader: Arc<Option<AggIndexReader>>,

    block_reader: Arc<BlockReader>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    ctx: Arc<dyn TableContext>,

    output: Arc<OutputPort>,
    output_data: Option<(Vec<PartInfoPtr>, Vec<NativeDataSource>)>,

    finished: bool,
}

impl ReadNativeDataSourceWithChannel<true> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
        meta_receiver: Receiver<Partitions>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(
            ctx.clone(),
            output.clone(),
            ReadNativeDataSourceWithChannel::<true> {
                meta_receiver,
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
                output,
                output_data: None,
                finished: false,
            },
        )
    }
}

impl ReadNativeDataSourceWithChannel<false> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
        meta_receiver: Receiver<Partitions>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(
            ReadNativeDataSourceWithChannel::<false> {
                meta_receiver,
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
                output,
                output_data: None,
                finished: false,
            },
        )))
    }
}
impl SyncSource for ReadNativeDataSourceWithChannel<true> {
    const NAME: &'static str = "SyncReadNativeDataSourceWithChannel";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        let partitions = self.meta_receiver.recv_blocking().map_err(|_err| {
            ErrorCode::Internal("ReadNativeDataSourceWithChannel receive problem")
        })?;
        let mut native_data_source = Vec::with_capacity(partitions.partitions.len());
        let mut native_part_infos = Vec::with_capacity(partitions.partitions.len());
        for info_ptr in partitions.partitions {
            native_part_infos.push(info_ptr.clone());
            let fuse_part = FuseBlockPartInfo::from_part(&info_ptr)?;

            if let Some(index_reader) = self.index_reader.as_ref() {
                let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                    &fuse_part.location,
                    index_reader.index_id(),
                );

                if let Some(data) = index_reader.sync_read_native_data(&loc) {
                    native_data_source.push(NativeDataSource::AggIndex(data));
                    continue;
                }
            }

            if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                let loc =
                    TableMetaLocationGenerator::gen_virtual_block_location(&fuse_part.location);

                if let Some((mut virtual_source_data, ignore_column_ids)) =
                    virtual_reader.sync_read_native_data(&loc)
                {
                    let mut source_data = self
                        .block_reader
                        .sync_read_native_columns_data(&info_ptr, &ignore_column_ids)?;
                    source_data.append(&mut virtual_source_data);
                    native_data_source.push(NativeDataSource::Normal(source_data));
                    continue;
                }
            }

            let source_data = self
                .block_reader
                .sync_read_native_columns_data(&info_ptr, &None)?;
            native_data_source.push(NativeDataSource::Normal(source_data));
        }

        Ok(Some(DataBlock::empty_with_meta(
            DataSourceWithMeta::create(native_part_infos, native_data_source),
        )))
    }
}

#[async_trait::async_trait]
impl Processor for ReadNativeDataSourceWithChannel<false> {
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
            let output = DataBlock::empty_with_meta(DataSourceWithMeta::create(part, data));
            self.output.push_data(Ok(output));
            // return Ok(Event::NeedConsume);
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Ok(parts) = self.meta_receiver.recv().await {
            if !parts.is_empty() {
                let mut chunks = Vec::with_capacity(parts.len());

                let mut native_part_infos = Vec::with_capacity(parts.len());
                for part in parts.partitions.into_iter() {
                    native_part_infos.push(part.clone());
                    let block_reader = self.block_reader.clone();
                    let index_reader = self.index_reader.clone();
                    let virtual_reader = self.virtual_reader.clone();
                    let ctx = self.ctx.clone();
                    chunks.push(async move {
                        let handler = databend_common_base::runtime::spawn(async move {
                            let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                            if let Some(index_reader) = index_reader.as_ref() {
                                let loc =
                                    TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                                        &fuse_part.location,
                                        index_reader.index_id(),
                                    );
                                if let Some(data) = index_reader.read_native_data(&loc).await {
                                    // Read from aggregating index.
                                    return Ok::<_, ErrorCode>(NativeDataSource::AggIndex(data));
                                }
                            }

                            if let Some(virtual_reader) = virtual_reader.as_ref() {
                                let loc = TableMetaLocationGenerator::gen_virtual_block_location(
                                    &fuse_part.location,
                                );

                                // If virtual column file exists, read the data from the virtual columns directly.
                                if let Some((mut virtual_source_data, ignore_column_ids)) =
                                    virtual_reader.read_native_data(&loc).await
                                {
                                    let mut source_data = block_reader
                                        .async_read_native_columns_data(&part, &ctx, &ignore_column_ids)
                                        .await?;
                                    source_data.append(&mut virtual_source_data);
                                    return Ok(NativeDataSource::Normal(source_data));
                                }
                            }

                            Ok(NativeDataSource::Normal(
                                block_reader
                                    .async_read_native_columns_data(&part, &ctx, &None)
                                    .await?,
                            ))
                        });
                        handler.await.unwrap()
                    });
                }

                debug!("ReadNativeDataSource parts: {}", chunks.len());
                self.output_data = Some((
                    native_part_infos,
                    futures::future::try_join_all(chunks).await?,
                ));
                return Ok(());
            }
        }

        self.finished = true;
        Ok(())
    }
}
