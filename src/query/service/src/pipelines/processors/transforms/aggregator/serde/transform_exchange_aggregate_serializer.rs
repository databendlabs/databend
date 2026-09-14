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
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::UnknownMode;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_settings::FlightCompression;
use databend_common_storages_parquet::serialize_row_group_meta_to_bytes;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionItem;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;
use crate::pipelines::processors::transforms::aggregator::SerializeAggregateStream;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::exchange::serde::serialize_block;

pub struct TransformExchangeAggregateSerializer {
    local_pos: usize,
    options: IpcWriteOptions,

    params: Arc<AggregatorParams>,
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
                params,
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

            match block.take_meta().and_then(AggregateMeta::downcast_from) {
                Some(AggregateMeta::AggregatePayload(p)) => {
                    let block_number = p.exchange_block_number();

                    if index == self.local_pos {
                        serialized_blocks.push(
                            block.add_meta(Some(Box::new(AggregateMeta::AggregatePayload(p))))?,
                        );
                        continue;
                    }

                    let stream = SerializeAggregateStream::create(&self.params, p);
                    let mut stream_blocks = stream.into_iter().collect::<Result<Vec<_>>>()?;
                    debug_assert!(!stream_blocks.is_empty());
                    let mut c = DataBlock::concat(&stream_blocks)?;
                    if let Some(meta) = stream_blocks[0].take_meta() {
                        c.replace_meta(meta);
                    }
                    let c = serialize_block(block_number, c, &self.options)?;
                    serialized_blocks.push(c);
                }
                Some(AggregateMeta::Partitioned { data, .. }) => {
                    if index == self.local_pos {
                        serialized_blocks.push(
                            block.add_meta(Some(AggregateMeta::create_partitioned(None, data)))?,
                        );
                        continue;
                    }
                    let data_block = match data {
                        PartitionedData::Empty => DataBlock::empty(),
                        PartitionedData::Serialized(data) if data.is_empty() => DataBlock::empty(),
                        PartitionedData::AggregatePayload(data) if data.is_empty() => {
                            DataBlock::empty()
                        }
                        PartitionedData::BucketSpilled(data) if data.is_empty() => {
                            DataBlock::empty()
                        }
                        PartitionedData::AggregatePayload(data) => {
                            let mut buckets = Vec::with_capacity(data.len());
                            let mut payload_row_counts = Vec::with_capacity(data.len());
                            let mut payload_blocks = Vec::with_capacity(data.len());

                            for payload in data {
                                let block = payload.payload.aggregate_flush_all()?;
                                if block.num_rows() == 0 {
                                    continue;
                                }
                                buckets.push(payload.bucket);
                                payload_row_counts.push(block.num_rows());
                                payload_blocks.push(block);
                            }

                            // Only create empty block when ALL payloads are empty
                            if payload_blocks.is_empty() {
                                DataBlock::empty()
                            } else {
                                let merged_block = DataBlock::concat(&payload_blocks)?;
                                merged_block.add_meta(Some(
                                    AggregateSerdeMeta::create_partitioned_payload(
                                        buckets,
                                        payload_row_counts,
                                        false,
                                    ),
                                ))?
                            }
                        }
                        PartitionedData::BucketSpilled(data) => {
                            let bucket_num = data.len();
                            let mut bucket_column = Vec::with_capacity(bucket_num);
                            let mut row_group_column = Vec::with_capacity(bucket_num);
                            let mut location_column = Vec::with_capacity(bucket_num);

                            for payload in data {
                                bucket_column.push(payload.bucket as i64);
                                location_column.push(payload.location);
                                row_group_column
                                    .push(serialize_row_group_meta_to_bytes(&payload.row_group)?);
                            }
                            let data_block = DataBlock::new_from_columns(vec![
                                Int64Type::from_data(bucket_column),
                                StringType::from_data(location_column),
                                BinaryType::from_data(row_group_column),
                            ]);

                            data_block.add_meta(Some(AggregateSerdeMeta::create_spilled(
                                bucket_num as isize,
                            )))?
                        }
                        PartitionedData::Mixed(data) => PartitionItem::serialize_mixed(data)?,
                        data => {
                            return Err(ErrorCode::Internal(format!(
                                "Partitioned meta cannot be serialized from this payload batch: {data:?}"
                            )));
                        }
                    };
                    let serialized = serialize_block(-1, data_block, &self.options)?;
                    serialized_blocks.push(serialized);
                }
                _ => unreachable!("unexpected aggregate meta"),
            };
        }

        Ok(vec![DataBlock::empty_with_meta(
            ExchangeShuffleMeta::create(serialized_blocks),
        )])
    }
}
