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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::AsyncTransform;
use common_pipeline_transforms::processors::transforms::AsyncTransformer;
use common_pipeline_transforms::processors::transforms::Transformer;
use opendal::Operator;
use storages_common_cache::LoadParams;

use crate::index_analyzer::segment_info_meta::CompactSegmentInfoMeta;
use crate::index_analyzer::segment_location_meta::SegmentLocationMetaInfo;
use crate::io::MetaReaders;

pub struct SegmentReadTransform {
    operator: Operator,
    table_schema: TableSchemaRef,
}

impl SegmentReadTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
        table_schema: TableSchemaRef,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            SegmentReadTransform {
                operator,
                table_schema,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for SegmentReadTransform {
    const NAME: &'static str = "SegmentLoadTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        if let Some(block_meta) = data.take_meta() {
            if let Some(location) = SegmentLocationMetaInfo::downcast_from(block_meta) {
                let segment_location = location.segment_location;
                let operator = self.operator.clone();
                let table_schema = self.table_schema.clone();

                // Keep in mind that segment_info_read must need a schema
                let segment_reader = MetaReaders::segment_info_reader(operator, table_schema);
                let (location, ver) = segment_location.location.clone();
                let load_params = LoadParams {
                    location,
                    len_hint: None,
                    ver,
                    put_cache: true,
                };

                let info = segment_reader.read(&load_params).await?;
                return Ok(DataBlock::empty_with_meta(CompactSegmentInfoMeta::create(
                    segment_location,
                    info,
                )));
            }
        }

        Err(ErrorCode::Internal(
            "SegmentInfoTransform only recv SegmentLocationMetaInfo meta",
        ))
    }
}
