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
use databend_storages_common_io::ReadSettings;
use log::debug;

use super::parquet_data_source::ParquetDataSource;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::pruning_pipeline::PartitionsMeta;

pub struct ReadParquetDataTransform<const BLOCKING_IO: bool> {
    index_reader: Arc<Option<AggIndexReader>>,

    block_reader: Arc<BlockReader>,

    virtual_reader: Arc<Option<VirtualColumnReader>>,

    ctx: Arc<dyn TableContext>,
}

impl ReadParquetDataTransform<true> {
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
            ReadParquetDataTransform::<true> {
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
            },
        )))
    }
}

impl ReadParquetDataTransform<false> {
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
            ReadParquetDataTransform::<false> {
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
            },
        )))
    }
}

impl Transform for ReadParquetDataTransform<true> {
    const NAME: &'static str = "SyncReadParquetDataTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(info_ptr) = data.get_meta() {
            if let Some(partition_meta) = PartitionsMeta::downcast_from(info_ptr.clone()) {
                let partitions = partition_meta.partitions.partitions;
                let mut parquet_data_source = Vec::with_capacity(partitions.len());
                let mut parquet_part_infos = Vec::with_capacity(partitions.len());
                for info_ptr in partitions {
                    parquet_part_infos.push(info_ptr.clone());
                    let fuse_part = FuseBlockPartInfo::from_part(&info_ptr)?;

                    if let Some(index_reader) = self.index_reader.as_ref() {
                        let loc =
                            TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                                &fuse_part.location,
                                index_reader.index_id(),
                            );
                        if let Some(data) = index_reader.sync_read_parquet_data_by_merge_io(
                            &ReadSettings::from_ctx(&self.ctx)?,
                            &loc,
                        ) {
                            // Read from aggregating index.
                            parquet_data_source.push(ParquetDataSource::AggIndex(data));
                            continue;
                        }
                    }

                    // If virtual column file exists, read the data from the virtual columns directly.
                    let virtual_source = if let Some(virtual_reader) = self.virtual_reader.as_ref()
                    {
                        let loc = TableMetaLocationGenerator::gen_virtual_block_location(
                            &fuse_part.location,
                        );

                        virtual_reader.sync_read_parquet_data_by_merge_io(
                            &ReadSettings::from_ctx(&self.ctx)?,
                            &loc,
                        )
                    } else {
                        None
                    };
                    let ignore_column_ids = if let Some(virtual_source) = &virtual_source {
                        &virtual_source.ignore_column_ids
                    } else {
                        &None
                    };

                    let source = self.block_reader.sync_read_columns_data_by_merge_io(
                        &ReadSettings::from_ctx(&self.ctx)?,
                        &info_ptr,
                        ignore_column_ids,
                    )?;
                    parquet_data_source.push(ParquetDataSource::Normal((source, virtual_source)));
                }

                return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                    parquet_part_infos,
                    parquet_data_source,
                )));
            }
        }
        Err(ErrorCode::Internal(
            "SyncReadParquetDataTransform cannot downcast meta to PartitionsMeta",
        ))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadParquetDataTransform<false> {
    const NAME: &'static str = "AsyncReadParquetDataTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(info_ptr) = data.get_meta() {
            if let Some(partition_meta) = PartitionsMeta::downcast_from(info_ptr.clone()) {
                let partitions = partition_meta.partitions.partitions;
                if !partitions.is_empty() {
                    let mut chunks = Vec::with_capacity(partitions.len());

                    let mut parquet_part_infos = Vec::with_capacity(partitions.len());
                    for part in partitions.into_iter() {
                        parquet_part_infos.push(part.clone());
                        let block_reader = self.block_reader.clone();
                        let settings = ReadSettings::from_ctx(&self.ctx)?;
                        let index_reader = self.index_reader.clone();
                        let virtual_reader = self.virtual_reader.clone();

                        chunks.push(async move {
                            databend_common_base::runtime::spawn(async move {
                                let part = FuseBlockPartInfo::from_part(&part)?;

                                if let Some(index_reader) = index_reader.as_ref() {
                                    let loc =
                                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                                            &part.location,
                                            index_reader.index_id(),
                                        );
                                    if let Some(data) = index_reader
                                        .read_parquet_data_by_merge_io(&settings, &loc)
                                        .await
                                    {
                                        // Read from aggregating index.
                                        return Ok::<_, ErrorCode>(ParquetDataSource::AggIndex(data));
                                    }
                                }

                                // If virtual column file exists, read the data from the virtual columns directly.
                                let virtual_source = if let Some(virtual_reader) = virtual_reader.as_ref() {
                                    let loc = TableMetaLocationGenerator::gen_virtual_block_location(
                                        &part.location,
                                    );

                                    virtual_reader
                                        .read_parquet_data_by_merge_io(&settings, &loc)
                                        .await
                                } else {
                                    None
                                };

                                let ignore_column_ids = if let Some(virtual_source) = &virtual_source {
                                    &virtual_source.ignore_column_ids
                                } else {
                                    &None
                                };

                                let source = block_reader
                                    .read_columns_data_by_merge_io(
                                        &settings,
                                        &part.location,
                                        &part.columns_meta,
                                        ignore_column_ids,
                                    )
                                    .await?;

                                Ok(ParquetDataSource::Normal((source, virtual_source)))
                            })
                                .await
                                .unwrap()
                        });
                    }

                    debug!("AsyncReadParquetDataTransform parts: {}", chunks.len());

                    return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                        parquet_part_infos,
                        futures::future::try_join_all(chunks).await?,
                    )));
                }
            }
        }

        Err(ErrorCode::Internal(
            "AsyncReadParquetDataTransform cannot downcast meta to PartitionsMeta",
        ))
    }
}
