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

use arrow_ipc::writer::IpcWriteOptions;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::BlockMetaTransform;
use databend_common_settings::FlightCompression;

use crate::pipelines::processors::transforms::aggregator::FlightSerializedMeta;
use crate::pipelines::processors::transforms::sort::SortCollectedMeta;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::exchange::ExchangeSorting;
use crate::servers::flight::v1::exchange::MergeExchangeParams;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;

pub struct SortInjector {}

impl ExchangeInjector for SortInjector {
    fn flight_scatter(
        &self,
        ctx: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        match exchange {
            DataExchange::Merge(_) | DataExchange::Broadcast(_) => unreachable!(),
            DataExchange::ShuffleDataExchange(exchange) => Ok(Arc::new(Box::new(DummyScatter {}))),
        }
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        None
    }

    fn apply_merge_serializer(
        &self,
        params: &MergeExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        unreachable!()
    }

    fn apply_merge_deserializer(
        &self,
        params: &MergeExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        unreachable!()
    }

    fn apply_shuffle_serializer(
        &self,
        params: &ShuffleExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        todo!()
    }

    fn apply_shuffle_deserializer(
        &self,
        params: &ShuffleExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        todo!()
    }
}

pub struct DummyScatter {}

impl FlightScatter for DummyScatter {
    fn execute(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        Ok(vec![data_block])
    }
}

pub struct TransformExchangeSortSerializer {
    ctx: Arc<QueryContext>,
    local_pos: usize,
    options: IpcWriteOptions,

    // params: Arc<AggregatorParams>,
    spiller: Arc<Spiller>,
}

impl BlockMetaTransform<ExchangeShuffleMeta> for TransformExchangeSortSerializer {
    const NAME: &'static str = "TransformExchangeSortSerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<Vec<DataBlock>> {
        let serialized_blocks = meta
            .blocks
            .into_iter()
            .map(|mut block| {
                let SortCollectedMeta {
                    params,
                    bounds,
                    blocks,
                } = SortCollectedMeta::downcast_from(block.take_meta().unwrap()).unwrap();

                //             match index == self.local_pos {
                //                 true => local_agg_spilling_aggregate_payload(
                //                     self.ctx.clone(),
                //                     self.spiller.clone(),
                //                     payload,
                //                 )?,
                //                 false => exchange_agg_spilling_aggregate_payload(
                //                     self.ctx.clone(),
                //                     self.spiller.clone(),
                //                     payload,
                //                 )?,
                //             },
            })
            .collect();

        // let meta = SortCollectedMeta::downcast_from(block.take_meta().unwrap()).unwrap();

        // match AggregateMeta::downcast_from(block.take_meta().unwrap()) {
        //     None => unreachable!(),
        //     Some(AggregateMeta::Spilled(_)) => unreachable!(),
        //     Some(AggregateMeta::Serialized(_)) => unreachable!(),
        //     Some(AggregateMeta::BucketSpilled(_)) => unreachable!(),
        //     Some(AggregateMeta::Partitioned { .. }) => unreachable!(),
        //     Some(AggregateMeta::AggregateSpilling(payload)) => {
        //         serialized_blocks.push(FlightSerialized::Future(

        //         ));
        //     }

        //     Some(AggregateMeta::AggregatePayload(p)) => {
        //         let (bucket, max_partition_count) = (p.bucket, p.max_partition_count);

        //         if index == self.local_pos {
        //             serialized_blocks.push(FlightSerialized::DataBlock(
        //                 block.add_meta(Some(Box::new(AggregateMeta::AggregatePayload(p))))?,
        //             ));
        //             continue;
        //         }

        //         let block_number = compute_block_number(bucket, max_partition_count)?;
        //         let stream = SerializeAggregateStream::create(
        //             &self.params,
        //             SerializePayload::AggregatePayload(p),
        //         );
        //         let mut stream_blocks = stream.into_iter().collect::<Result<Vec<_>>>()?;
        //         debug_assert!(!stream_blocks.is_empty());
        //         let mut c = DataBlock::concat(&stream_blocks)?;
        //         if let Some(meta) = stream_blocks[0].take_meta() {
        //             c.replace_meta(meta);
        //         }
        //         let c = serialize_block(block_number, c, &self.options)?;
        //         serialized_blocks.push(FlightSerialized::DataBlock(c));
        //     }
        // };

        Ok(vec![DataBlock::empty_with_meta(
            FlightSerializedMeta::create(serialized_blocks),
        )])
    }
}
