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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;

use crate::pipelines::processors::transforms::aggregator::FlightSerialized;
use crate::pipelines::processors::transforms::aggregator::FlightSerializedMeta;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;

pub struct TransformExchangeAsyncBarrier;

impl TransformExchangeAsyncBarrier {
    pub fn try_create(input: Arc<InputPort>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            TransformExchangeAsyncBarrier {},
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for TransformExchangeAsyncBarrier {
    const NAME: &'static str = "TransformExchangeAsyncBarrier";

    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        if let Some(meta) = data
            .take_meta()
            .and_then(FlightSerializedMeta::downcast_from)
        {
            let mut futures = Vec::with_capacity(meta.serialized_blocks.len());

            for serialized_block in meta.serialized_blocks {
                futures.push(databend_common_base::runtime::spawn(async move {
                    match serialized_block {
                        FlightSerialized::DataBlock(v) => Ok(v),
                        FlightSerialized::Future(f) => f.await,
                    }
                }));
            }

            return match futures::future::try_join_all(futures).await {
                Err(_) => Err(ErrorCode::TokioError("Cannot join tokio job")),
                Ok(spilled_data) => Ok(DataBlock::empty_with_meta(ExchangeShuffleMeta::create(
                    spilled_data.into_iter().collect::<Result<Vec<_>>>()?,
                ))),
            };
        }

        Err(ErrorCode::Internal(""))
    }
}
