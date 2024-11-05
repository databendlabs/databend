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
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_sql::IndexType;
use log::debug;

use super::native_data_source::NativeDataSource;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::operations::read::runtime_filter_prunner::runtime_filter_pruner;
use crate::FuseBlockPartInfo;

pub struct ReadNativeDataTransform<const BLOCKING_IO: bool> {
    func_ctx: FunctionContext,
    block_reader: Arc<BlockReader>,

    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    table_schema: Arc<TableSchema>,
    table_index: IndexType,
    context: Arc<dyn TableContext>,
}

impl ReadNativeDataTransform<true> {
    pub fn create(
        table_index: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            ReadNativeDataTransform::<true> {
                func_ctx,
                block_reader,
                index_reader,
                virtual_reader,
                table_schema,
                table_index,
                context: ctx,
            },
        )))
    }
}

impl ReadNativeDataTransform<false> {
    pub fn create(
        table_index: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let func_ctx = ctx.get_function_context()?;
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadNativeDataTransform::<false> {
                func_ctx,
                block_reader,
                index_reader,
                virtual_reader,
                table_schema,
                table_index,
                context: ctx,
            },
        )))
    }
}

impl Transform for ReadNativeDataTransform<true> {
    const NAME: &'static str = "SyncReadNativeDataTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = data.get_meta() {
            if let Some(block_part_meta) = BlockPartitionMeta::downcast_ref_from(meta) {
                let mut partitions = block_part_meta.part_ptr.clone();
                debug_assert!(partitions.len() == 1);
                let part = partitions.pop().unwrap();
                let mut filters = self
                    .context
                    .get_inlist_runtime_filter_with_id(self.table_index);
                filters.extend(
                    self.context
                        .get_min_max_runtime_filter_with_id(self.table_index),
                );
                if runtime_filter_pruner(
                    self.table_schema.clone(),
                    &part,
                    &filters,
                    &self.func_ctx,
                )? {
                    return Ok(DataBlock::empty());
                }
                if let Some(index_reader) = self.index_reader.as_ref() {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                    let loc =
                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                            &fuse_part.location,
                            index_reader.index_id(),
                        );
                    if let Some(data) = index_reader.sync_read_native_data(&loc) {
                        // Read from aggregating index.
                        return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                            vec![part.clone()],
                            vec![NativeDataSource::AggIndex(data)],
                        )));
                    }
                }

                if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                    let loc =
                        TableMetaLocationGenerator::gen_virtual_block_location(&fuse_part.location);

                    // If virtual column file exists, read the data from the virtual columns directly.
                    if let Some((mut virtual_source_data, ignore_column_ids)) =
                        virtual_reader.sync_read_native_data(&loc)
                    {
                        let mut source_data = self
                            .block_reader
                            .sync_read_native_columns_data(&part, &ignore_column_ids)?;
                        source_data.append(&mut virtual_source_data);
                        return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                            vec![part.clone()],
                            vec![NativeDataSource::Normal(source_data)],
                        )));
                    }
                }

                return Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
                    vec![part.clone()],
                    vec![NativeDataSource::Normal(
                        self.block_reader
                            .sync_read_native_columns_data(&part, &None)?,
                    )],
                )));
            }
        }
        Err(ErrorCode::Internal(
            "ReadNativeDataTransform get wrong meta data",
        ))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadNativeDataTransform<false> {
    const NAME: &'static str = "AsyncReadNativeDataTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = data.get_meta() {
            if let Some(block_part_meta) = BlockPartitionMeta::downcast_ref_from(meta) {
                let parts = block_part_meta.part_ptr.clone();
                if !parts.is_empty() {
                    let mut chunks = Vec::with_capacity(parts.len());
                    let mut filters = self
                        .context
                        .get_inlist_runtime_filter_with_id(self.table_index);
                    filters.extend(
                        self.context
                            .get_min_max_runtime_filter_with_id(self.table_index),
                    );
                    let mut native_part_infos = Vec::with_capacity(parts.len());
                    for part in parts.into_iter() {
                        if runtime_filter_pruner(
                            self.table_schema.clone(),
                            &part,
                            &filters,
                            &self.func_ctx,
                        )? {
                            continue;
                        }

                        native_part_infos.push(part.clone());
                        let block_reader = self.block_reader.clone();
                        let index_reader = self.index_reader.clone();
                        let virtual_reader = self.virtual_reader.clone();
                        let ctx = self.context.clone();
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
            "AsyncReadNativeDataTransform get wrong meta data",
        ))
    }
}
