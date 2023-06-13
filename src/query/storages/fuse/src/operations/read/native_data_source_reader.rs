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

use crate::io::BlockReader;
use crate::operations::read::native_data_source::DataChunks;
use crate::operations::read::native_data_source::NativeDataSourceMeta;
use crate::FusePartMeta;

pub struct ReadNativeDataSource<const BLOCKING_IO: bool> {
    block_reader: Arc<BlockReader>,
}

impl ReadNativeDataSource<true> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            ReadNativeDataSource::<true> { block_reader },
        )))
    }
}

impl ReadNativeDataSource<false> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadNativeDataSource::<false> { block_reader },
        )))
    }
}

impl BlockMetaTransform<FusePartMeta> for ReadNativeDataSource<true> {
    const NAME: &'static str = "SyncReadNativeDataSource";

    fn transform(&mut self, meta: FusePartMeta) -> Result<DataBlock> {
        Ok(DataBlock::empty_with_meta(NativeDataSourceMeta::create(
            vec![meta.part_info.clone()],
            vec![
                self.block_reader
                    .sync_read_native_columns_data(meta.part_info)?,
            ],
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadNativeDataSource<false> {
    const NAME: &'static str = "AsyncReadNativeDataSource";

    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let meta = data.take_meta().unwrap();
        if let Some(block_meta) = FusePartMeta::downcast_from(meta) {
            return Ok(DataBlock::empty_with_meta(NativeDataSourceMeta::create(
                vec![block_meta.part_info.clone()],
                vec![
                    self.block_reader
                        .clone()
                        .async_read_native_columns_data(block_meta.part_info)
                        .await?,
                ],
            )));
        }

        Err(ErrorCode::Internal(
            "AsyncReadNativeDataSource only recv FusePartMeta",
        ))
    }
}
