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

use arrow_ipc::CompressionType;
use arrow_ipc::writer::IpcWriteOptions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::UnknownMode;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_settings::FlightCompression;

use crate::pipelines::processors::transforms::aggregator::AggregateExchangeDataCodec;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::exchange::serde::serialize_block;
use crate::servers::flight::v1::network::ExchangeDataCodec;

pub struct TransformExchangeAggregateSerializer {
    local_pos: usize,
    options: IpcWriteOptions,

    codec: Arc<AggregateExchangeDataCodec>,
}

impl TransformExchangeAggregateSerializer {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        _ctx: Arc<crate::sessions::QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        compression: Option<FlightCompression>,
        local_pos: usize,
    ) -> Result<Box<dyn Processor>> {
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
                FlightCompression::Zstd => Some(CompressionType::ZSTD),
            },
        };

        Ok(BlockMetaTransformer::create(
            input,
            output,
            TransformExchangeAggregateSerializer {
                codec: AggregateExchangeDataCodec::create(params),
                local_pos,
                options: IpcWriteOptions::default()
                    .try_with_compression(compression)
                    .unwrap(),
            },
        ))
    }
}

impl BlockMetaTransform<ExchangeShuffleMeta> for TransformExchangeAggregateSerializer {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformExchangeAggregateSerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<Vec<DataBlock>> {
        let mut serialized_blocks = Vec::with_capacity(meta.blocks.len());
        for (index, mut block) in meta.blocks.into_iter().enumerate() {
            if block.is_empty() && block.get_meta().is_none() {
                serialized_blocks.push(block);
                continue;
            }

            let (block_number, meta): (isize, BlockMetaInfoPtr) =
                match block.take_meta().and_then(AggregateMeta::downcast_from) {
                    Some(AggregateMeta::AggregatePayload(payload)) => (
                        payload.exchange_block_number(),
                        Box::new(AggregateMeta::AggregatePayload(payload)),
                    ),
                    Some(AggregateMeta::Partitioned { data, .. }) => {
                        (-1, AggregateMeta::create_partitioned(None, data))
                    }
                    _ => {
                        return Err(ErrorCode::Internal(
                            "Unexpected aggregate exchange metadata",
                        ));
                    }
                };
            block.replace_meta(meta);
            if index == self.local_pos {
                serialized_blocks.push(block);
                continue;
            }
            let transport = self.codec.encode(block)?.unwrap_or_else(DataBlock::empty);
            serialized_blocks.push(serialize_block(block_number, transport, &self.options)?);
        }

        Ok(vec![DataBlock::empty_with_meta(
            ExchangeShuffleMeta::create(serialized_blocks),
        )])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipelines::processors::transforms::aggregator::PartitionedData;
    use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_codec::tests::params;
    use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_codec::tests::payload_block;
    use crate::servers::flight::v1::exchange::serde::ExchangeSerializeMeta;

    #[test]
    fn test_shuffle_codec_preserves_local_payload_and_remote_routing() -> Result<()> {
        let mut serializer = TransformExchangeAggregateSerializer {
            local_pos: 0,
            options: IpcWriteOptions::default(),
            codec: AggregateExchangeDataCodec::create(params()),
        };
        let mut result = serializer.transform(ExchangeShuffleMeta {
            blocks: vec![
                payload_block(vec![11, 22]),
                payload_block(vec![33, 44]),
                DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
                    Some(2),
                    PartitionedData::Empty,
                )),
            ],
        })?;
        let result = result[0]
            .take_meta()
            .and_then(ExchangeShuffleMeta::downcast_from)
            .unwrap();
        assert_eq!(result.blocks.len(), 3);
        let Some(AggregateMeta::AggregatePayload(local)) = result.blocks[0]
            .get_meta()
            .and_then(AggregateMeta::downcast_ref_from)
        else {
            panic!("local payload was serialized")
        };
        assert_eq!(local.payload.len(), 2);
        let remote = result.blocks[1]
            .get_meta()
            .and_then(ExchangeSerializeMeta::downcast_ref_from)
            .unwrap();
        assert_eq!(remote.block_number, 8007);
        assert!(!remote.packet.is_empty());
        let empty = result.blocks[2]
            .get_meta()
            .and_then(ExchangeSerializeMeta::downcast_ref_from)
            .unwrap();
        assert_eq!(empty.block_number, -1);
        assert!(empty.packet.is_empty());
        Ok(())
    }
}
