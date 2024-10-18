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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use log::debug;

use super::native_data_source::NativeDataSource;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning_pipeline::PartitionsMeta;
use crate::FuseBlockPartInfo;

pub struct ReadNativeDataTransform<const BLOCKING_IO: bool> {
    index_reader: Arc<Option<AggIndexReader>>,

    block_reader: Arc<BlockReader>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    ctx: Arc<dyn TableContext>,
}

impl ReadNativeDataTransform<true> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            ReadNativeDataTransform::<true> {
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
            },
        )))
    }
}

impl ReadNativeDataTransform<false> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadNativeDataTransform::<false> {
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
            },
        )))
    }
}
impl Transform for ReadNativeDataTransform<true> {
    const NAME: &'static str = "SyncReadNativeDataTransform";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(info_ptr) = data.get_meta() {
            if let Some(partition_meta) = PartitionsMeta::downcast_from(info_ptr.clone()) {
                let partitions = partition_meta.partitions.partitions;
                let mut native_data_source = Vec::with_capacity(partitions.len());
                let mut native_part_infos = Vec::with_capacity(partitions.len());
                for info_ptr in partitions {
                    native_part_infos.push(info_ptr.clone());
                    let fuse_part = FuseBlockPartInfo::from_part(&info_ptr)?;

                    if let Some(index_reader) = self.index_reader.as_ref() {
                        let loc =
                            TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                                &fuse_part.location,
                                index_reader.index_id(),
                            );

                        if let Some(data) = index_reader.sync_read_native_data(&loc) {
                            native_data_source.push(NativeDataSource::AggIndex(data));
                            continue;
                        }
                    }

                    if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                        let loc = TableMetaLocationGenerator::gen_virtual_block_location(
                            &fuse_part.location,
                        );

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

                return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                    native_part_infos,
                    native_data_source,
                )));
            }
        }

        Err(ErrorCode::Internal(
            "SyncReadNativeDataTransform cannot downcast meta to PartitionsMeta",
        ))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadNativeDataTransform<false> {
    const NAME: &'static str = "AsyncReadNativeDataTransform";

    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(info_ptr) = data.get_meta() {
            if let Some(partition_meta) = PartitionsMeta::downcast_from(info_ptr.clone()) {
                let partitions = partition_meta.partitions.partitions;
                if !partitions.is_empty() {
                    let mut chunks = Vec::with_capacity(partitions.len());

                    let mut native_part_infos = Vec::with_capacity(partitions.len());
                    for part in partitions.into_iter() {
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
                    return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                        native_part_infos,
                        futures::future::try_join_all(chunks).await?,
                    )));
                }
            }
        }
        Err(ErrorCode::Internal(
            "AsyncReadNativeDataTransform cannot downcast meta to PartitionsMeta",
        ))
    }
}
