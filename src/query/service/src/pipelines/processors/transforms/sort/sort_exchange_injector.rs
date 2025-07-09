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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::Pipeline;
use databend_common_settings::FlightCompression;

use crate::pipelines::processors::transforms::SortBound;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::DefaultExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeSorting;
use crate::servers::flight::v1::exchange::MergeExchangeParams;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;

pub struct SortInjector {}

impl ExchangeInjector for SortInjector {
    fn flight_scatter(
        &self,
        _: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        match exchange {
            DataExchange::Merge(_) | DataExchange::Broadcast(_) => unreachable!(),
            DataExchange::ShuffleDataExchange(exchange) => {
                Ok(Arc::new(Box::new(SortBoundScatter {
                    partitions: exchange.destination_ids.len() as _,
                })))
            }
        }
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        None
    }

    fn apply_merge_serializer(
        &self,
        _: &MergeExchangeParams,
        _: Option<FlightCompression>,
        _: &mut Pipeline,
    ) -> Result<()> {
        unreachable!()
    }

    fn apply_merge_deserializer(&self, _: &MergeExchangeParams, _: &mut Pipeline) -> Result<()> {
        unreachable!()
    }

    fn apply_shuffle_serializer(
        &self,
        params: &ShuffleExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        DefaultExchangeInjector::create().apply_shuffle_serializer(params, compression, pipeline)
    }

    fn apply_shuffle_deserializer(
        &self,
        params: &ShuffleExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        DefaultExchangeInjector::create().apply_shuffle_deserializer(params, pipeline)
    }
}

pub struct SortBoundScatter {
    partitions: u32,
}

impl FlightScatter for SortBoundScatter {
    fn name(&self) -> &'static str {
        "SortBound"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        bound_scatter(data_block, self.partitions)
    }
}

pub(super) fn bound_scatter(data_block: DataBlock, n: u32) -> Result<Vec<DataBlock>> {
    let meta = data_block
        .get_meta()
        .and_then(SortBound::downcast_ref_from)
        .unwrap();

    let index = meta.bound_index % n;

    Ok(std::iter::repeat_n(DataBlock::empty(), index as _)
        .chain(Some(data_block))
        .collect())
}

// pub struct TransformExchangeSortSerializer {
//     ctx: Arc<QueryContext>,
//     local_pos: usize,
//     options: IpcWriteOptions,

//     spiller: Arc<Spiller>,
// }

// impl BlockMetaTransform<ExchangeShuffleMeta> for TransformExchangeSortSerializer {
//     const NAME: &'static str = "TransformExchangeSortSerializer";

//     fn transform(&mut self, _meta: ExchangeShuffleMeta) -> Result<Vec<DataBlock>> {
//         // let serialized_blocks = meta
//         //     .blocks
//         //     .into_iter()
//         //     .map(|mut block| {
//         //         let SortCollectedMeta {
//         //             params,
//         //             bounds,
//         //             blocks,
//         //         } = block
//         //             .take_meta()
//         //             .and_then(SortCollectedMeta::downcast_from)
//         //             .unwrap();

//         //             match index == self.local_pos {
//         //                 true => local_agg_spilling_aggregate_payload(
//         //                     self.ctx.clone(),
//         //                     self.spiller.clone(),
//         //                     payload,
//         //                 )?,
//         //                 false => exchange_agg_spilling_aggregate_payload(
//         //                     self.ctx.clone(),
//         //                     self.spiller.clone(),
//         //                     payload,
//         //                 )?,
//         //             },
//         // })
//         // .collect();

//         // let meta = SortCollectedMeta::downcast_from(block.take_meta().unwrap()).unwrap();

//         // match AggregateMeta::downcast_from(block.take_meta().unwrap()) {
//         //     None => unreachable!(),
//         //     Some(AggregateMeta::Spilled(_)) => unreachable!(),
//         //     Some(AggregateMeta::Serialized(_)) => unreachable!(),
//         //     Some(AggregateMeta::BucketSpilled(_)) => unreachable!(),
//         //     Some(AggregateMeta::Partitioned { .. }) => unreachable!(),
//         //     Some(AggregateMeta::AggregateSpilling(payload)) => {
//         //         serialized_blocks.push(FlightSerialized::Future(

//         //         ));
//         //     }

//         //     Some(AggregateMeta::AggregatePayload(p)) => {
//         //         let (bucket, max_partition_count) = (p.bucket, p.max_partition_count);

//         //         if index == self.local_pos {
//         //             serialized_blocks.push(FlightSerialized::DataBlock(
//         //                 block.add_meta(Some(Box::new(AggregateMeta::AggregatePayload(p))))?,
//         //             ));
//         //             continue;
//         //         }

//         //         let block_number = compute_block_number(bucket, max_partition_count)?;
//         //         let stream = SerializeAggregateStream::create(
//         //             &self.params,
//         //             SerializePayload::AggregatePayload(p),
//         //         );
//         //         let mut stream_blocks = stream.into_iter().collect::<Result<Vec<_>>>()?;
//         //         debug_assert!(!stream_blocks.is_empty());
//         //         let mut c = DataBlock::concat(&stream_blocks)?;
//         //         if let Some(meta) = stream_blocks[0].take_meta() {
//         //             c.replace_meta(meta);
//         //         }
//         //         let c = serialize_block(block_number, c, &self.options)?;
//         //         serialized_blocks.push(FlightSerialized::DataBlock(c));
//         //     }
//         // };

//         todo!()

//         // Ok(vec![DataBlock::empty_with_meta(
//         //     FlightSerializedMeta::create(serialized_blocks),
//         // )])
//     }
// }

// struct SortScatter<R> {
//     ctx: Arc<QueryContext>,
//     local_pos: usize,
//     options: IpcWriteOptions,
//     schema: DataSchemaRef,

//     partitions: usize,
//     spiller: Arc<Spiller>,
//     data: Option<SortCollectedMeta>,
//     scatter_bounds: Bounds,
//     blocks: Vec<Box<[SpillableBlock]>>,

//     _r: PhantomData<R>,
// }

// #[async_trait::async_trait]
// impl<R: Rows> AsyncBlockingTransform for SortScatter<R> {
//     const NAME: &'static str = "TransformExchangeSortSerializer";

//     async fn consume(&mut self, mut block: DataBlock) -> Result<()> {
//         let meta = block
//             .take_meta()
//             .and_then(SortCollectedMeta::downcast_from)
//             .unwrap();
//         self.data = Some(meta);
//         Ok(())
//     }

//     async fn transform(&mut self) -> Result<Option<DataBlock>> {
//         todo!()
//     }
// }

// impl<R: Rows> SortScatter<R> {
//     fn scatter_bounds(&self, bounds: Bounds) -> Bounds {
//         let n = self.partitions - 1;
//         let bounds = if bounds.len() < n {
//             bounds
//         } else {
//             bounds.dedup_reduce::<R>(n)
//         };
//         assert!(bounds.len() < self.partitions);
//         bounds
//     }

//     async fn scatter(&mut self) -> Result<Vec<Option<SortCollectedMeta>>> {
//         // if self.scatter_bounds.is_empty() {
//         //     return Ok(vec![Some(SortCollectedMeta {
//         //         params,
//         //         bounds,
//         //         blocks,
//         //     })]);
//         // }

//         // let base = {
//         //     Base {
//         //         schema: self.schema.clone(),
//         //         spiller: self.spiller.clone(),
//         //         sort_row_offset: self.schema.fields.len() - 1,
//         //         limit: None,
//         //     }
//         // };

//         // let mut scattered_blocks = Vec::with_capacity(self.scatter_bounds.len() + 1);

//         // let Some(list) = self.blocks.pop() else {
//         //     todo!()
//         // };
//         // let scattered = base
//         //     .scatter_stream::<R>(Vec::from(list).into(), self.scatter_bounds.clone())
//         //     .await?;

//         //        ExchangeShuffleMeta::create(blocks);

//         // for list in  {

//         // }

//         // let scattered_meta = scattered_blocks
//         //     .into_iter()
//         //     .map(|blocks| {
//         //         (!blocks.is_empty()).then_some(SortCollectedMeta {
//         //             params: todo!(),
//         //             bounds: todo!(),
//         //             blocks,
//         //         })
//         //     })
//         //     .collect();
//         // Ok(scattered_meta)

//         todo!()
//     }
// }
