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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_sources::SyncSource;
use common_pipeline_sources::SyncSourcer;
use common_pipeline_transforms::processors::transforms::AsyncTransform;
use common_pipeline_transforms::processors::transforms::AsyncTransformer;
use common_pipeline_transforms::processors::transforms::BlockMetaTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaTransformer;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::operations::read::parquet_data_source::DataSourceMeta;
use crate::FusePartMeta;
use crate::MergeIOReadResult;

pub struct ReadParquetDataSource<const BLOCKING_IO: bool> {
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
}

impl<const BLOCKING_IO: bool> ReadParquetDataSource<BLOCKING_IO> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
    ) -> Result<ProcessorPtr> {
        if BLOCKING_IO {
            Ok(ProcessorPtr::create(BlockMetaTransformer::create(
                input,
                output,
                ReadParquetDataSource::<true> { ctx, block_reader },
            )))
        } else {
            Ok(ProcessorPtr::create(AsyncTransformer::create(
                input,
                output,
                ReadParquetDataSource::<false> { ctx, block_reader },
            )))
        }
    }
}

impl BlockMetaTransform<FusePartMeta> for ReadParquetDataSource<true> {
    const NAME: &'static str = "SyncReadParquetDataSource";

    fn transform(&mut self, meta: FusePartMeta) -> Result<DataBlock> {
        Ok(DataBlock::empty_with_meta(DataSourceMeta::create(
            vec![meta.part_info.clone()],
            vec![self.block_reader.sync_read_columns_data_by_merge_io(
                &ReadSettings::from_ctx(&self.ctx)?,
                meta.part_info,
            )?],
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadParquetDataSource<false> {
    const NAME: &'static str = "AsyncReadParquetDataSource";

    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = FusePartMeta::downcast_from(data.take_meta().unwrap()) {
            let block_reader = self.block_reader.clone();
            let settings = ReadSettings::from_ctx(&self.ctx)?;
            let part = FusePartInfo::from_part(&meta.part_info)?;

            let parts = vec![meta.part_info.clone()];
            let data = vec![
                block_reader
                    .read_columns_data_by_merge_io(&settings, &part.location, &part.columns_meta)
                    .await?,
            ];

            return Ok(DataBlock::empty_with_meta(DataSourceMeta::create(
                parts, data,
            )));
        }

        Err(ErrorCode::Internal(
            "AsyncReadParquetDataSource only recv FusePartMeta",
        ))
    }
}
