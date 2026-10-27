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

use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_storages_parquet::serialize_row_group_meta_to_bytes;

use crate::physical_plans::AggregateShuffleMode;
use crate::pipelines::processors::transforms::aggregator::AggregateBucketScatter;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateRowScatter;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionItem;
use crate::pipelines::processors::transforms::aggregator::SerializeAggregateStream;
use crate::pipelines::processors::transforms::aggregator::SpilledRef;
use crate::pipelines::processors::transforms::aggregator::decode_aggregate_block;
use crate::servers::flight::v1::network::HashShuffleCodec;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::servers::flight::v1::scatter::GlobalScatterBuilder;

/// Routes partial aggregate states through the global hash shuffle, straight
/// to the final aggregate thread that owns their groups, so final threads need
/// no local repartition.
pub struct AggregateShuffleScatter {
    params: Arc<AggregatorParams>,
    shuffle_mode: AggregateShuffleMode,
    /// The partitions of each destination node, in order.
    nodes: Vec<Range<usize>>,
}

impl AggregateShuffleScatter {
    /// `partitions` is the number the plan expects: row shuffle spills states
    /// into this many, and spilled states are routed by the partition they were
    /// spilled into, so the exchange must have exactly as many.
    pub fn builder(
        params: Arc<AggregatorParams>,
        shuffle_mode: AggregateShuffleMode,
        partitions: usize,
    ) -> GlobalScatterBuilder {
        Arc::new(move |nodes: &[Range<usize>]| {
            let exchange_partitions = nodes.last().map_or(0, |node| node.end);
            if exchange_partitions != partitions {
                return Err(ErrorCode::Internal(format!(
                    "Aggregate shuffle has {exchange_partitions} partitions, but the plan expects {partitions}"
                )));
            }
            Ok(Box::new(AggregateShuffleScatter {
                params: params.clone(),
                shuffle_mode: shuffle_mode.clone(),
                nodes: nodes.to_vec(),
            }) as Box<dyn FlightScatter>)
        })
    }
}

impl FlightScatter for AggregateShuffleScatter {
    fn name(&self) -> &'static str {
        "AggregateShuffle"
    }

    fn execute(&self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let nodes = &self.nodes;
        let partitions = nodes.last().map_or(0, |node| node.end);
        match self.shuffle_mode {
            AggregateShuffleMode::Row => AggregateRowScatter {
                buckets: partitions,
                aggregate_params: self.params.clone(),
            }
            .scatter(block),
            // Spread the buckets evenly over nodes first, then over their threads:
            // there may be up to twice as many buckets as threads.
            AggregateShuffleMode::Bucket(_) => {
                AggregateBucketScatter::scatter_by(block, partitions, |bucket| {
                    let node = &nodes[bucket % nodes.len()];
                    node.start + (bucket / nodes.len()) % node.len()
                })
            }
        }
    }
}

/// Converts the partial aggregate states sent to other nodes into blocks of
/// the wire format; those for the local node are delivered as they are.
pub struct AggregateShuffleCodec {
    params: Arc<AggregatorParams>,
}

impl AggregateShuffleCodec {
    pub fn create(params: Arc<AggregatorParams>) -> Arc<dyn HashShuffleCodec> {
        Arc::new(AggregateShuffleCodec { params })
    }
}

impl HashShuffleCodec for AggregateShuffleCodec {
    fn encode(&self, mut block: DataBlock) -> Result<DataBlock> {
        match block.take_meta().and_then(AggregateMeta::downcast_from) {
            Some(AggregateMeta::AggregatePayload(p)) => {
                let stream = SerializeAggregateStream::create(&self.params, p);
                let mut stream_blocks = stream.into_iter().collect::<Result<Vec<_>>>()?;
                debug_assert!(!stream_blocks.is_empty());
                let mut block = DataBlock::concat(&stream_blocks)?;
                if let Some(meta) = stream_blocks[0].take_meta() {
                    block.replace_meta(meta);
                }
                Ok(block)
            }
            Some(AggregateMeta::Partitioned { data, .. }) => encode_partitioned(data.into_items()),
            _ => Err(ErrorCode::Internal(
                "AggregateShuffleCodec can only encode aggregate payloads",
            )),
        }
    }

    fn decode(&self, mut block: DataBlock) -> Result<DataBlock> {
        let Some(meta) = block
            .take_meta()
            .and_then(AggregateSerdeMeta::downcast_from)
        else {
            return Err(ErrorCode::BadBytes(
                "Aggregate shuffle received a block without AggregateSerdeMeta",
            ));
        };

        Ok(decode_aggregate_block(&meta, block)?.unwrap_or_else(DataBlock::empty))
    }
}

/// Encode the payloads bound for one thread as a single block: the states in
/// memory as rows, one bucket after another, and the spilled buckets as
/// references in the meta.
fn encode_partitioned(items: Vec<PartitionItem>) -> Result<DataBlock> {
    let mut buckets = Vec::with_capacity(items.len());
    let mut payload_row_counts = Vec::with_capacity(items.len());
    let mut payload_blocks = Vec::with_capacity(items.len());
    let mut spilled = vec![];

    for item in items {
        let (bucket, block) = match item {
            PartitionItem::Serialized(payload) => (payload.bucket, payload.data_block),
            PartitionItem::AggregatePayload(payload) => {
                (payload.bucket, payload.payload.aggregate_flush_all()?)
            }
            PartitionItem::BucketSpilled(payload) => {
                spilled.push(SpilledRef {
                    bucket: payload.bucket,
                    location: payload.location,
                    row_group: serialize_row_group_meta_to_bytes(&payload.row_group)?,
                });
                continue;
            }
        };
        if block.num_rows() == 0 {
            continue;
        }
        buckets.push(bucket);
        payload_row_counts.push(block.num_rows());
        payload_blocks.push(block);
    }

    let is_empty = payload_blocks.is_empty() && spilled.is_empty();
    let block = match payload_blocks.is_empty() {
        true => DataBlock::empty(),
        false => DataBlock::concat(&payload_blocks)?,
    };
    block.add_meta(Some(AggregateSerdeMeta::create_partitioned_payload(
        buckets,
        payload_row_counts,
        spilled,
        is_empty,
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow_schema::Schema as ArrowSchema;
    use bumpalo::Bump;
    use databend_common_expression::AggregatePayload;
    use databend_common_expression::BlockMetaInfo;
    use databend_common_expression::BlockMetaInfoPtr;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;
    use databend_common_expression::Payload;
    use databend_common_expression::SerializedPayload;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use parquet::basic::Repetition;
    use parquet::file::metadata::RowGroupMetaData;
    use parquet::schema::types::SchemaDescriptor;
    use parquet::schema::types::Type;

    use super::*;
    use crate::pipelines::processors::transforms::aggregator::PartitionedData;
    use crate::pipelines::processors::transforms::aggregator::SpilledPayload;
    use crate::servers::flight::v1::network::wire_format::deserialize_flight_data;
    use crate::servers::flight::v1::network::wire_format::encode_block_for_thread;
    use crate::servers::flight::v1::network::wire_format::make_ipc_options;
    use crate::servers::flight::v1::network::wire_format::strip_tid;

    /// Three nodes of two threads.
    const NODES: [Range<usize>; 3] = [0..2, 2..4, 4..6];

    fn params() -> Arc<AggregatorParams> {
        let key_type = DataType::Number(NumberDataType::Int64);
        AggregatorParams::try_create(
            DataSchemaRefExt::create(vec![DataField::new("key", key_type.clone())]),
            vec![key_type],
            &[0],
            &[],
            &[],
            true,
            1024,
            1024 * 1024,
        )
        .unwrap()
    }

    fn scatter(shuffle_mode: AggregateShuffleMode) -> Box<dyn FlightScatter> {
        AggregateShuffleScatter::builder(params(), shuffle_mode, 6)(&NODES).unwrap()
    }

    fn codec() -> Arc<dyn HashShuffleCodec> {
        AggregateShuffleCodec::create(params())
    }

    fn payload(keys: Vec<i64>) -> Payload {
        let params = params();
        SerializedPayload {
            bucket: 0,
            data_block: DataBlock::new_from_columns(vec![Int64Type::from_data(keys)]),
            max_partition_count: 0,
        }
        .convert_to_single_payload(
            params.group_data_types.clone(),
            vec![],
            0,
            Arc::new(Bump::new()),
        )
        .unwrap()
    }

    fn spilled(bucket: isize) -> SpilledPayload {
        let schema = Type::group_type_builder("schema")
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap();
        let row_group = RowGroupMetaData::builder(SchemaDescriptor::new(Arc::new(schema)).into())
            .set_num_rows(17)
            .build()
            .unwrap();
        SpilledPayload {
            bucket,
            location: format!("spill/{bucket}"),
            row_group,
        }
    }

    fn partitioned(data: PartitionedData) -> DataBlock {
        DataBlock::empty_with_meta(AggregateMeta::create_partitioned(None, data))
    }

    fn take_meta(mut block: DataBlock) -> AggregateMeta {
        block
            .take_meta()
            .and_then(AggregateMeta::downcast_from)
            .unwrap()
    }

    fn keys(block: &DataBlock) -> Vec<i64> {
        let column = block.get_by_offset(block.num_columns() - 1).to_column();
        Int64Type::try_downcast_column(&column).unwrap().to_vec()
    }

    /// Send `block` as a remote partition does: encode, wire format, decode.
    fn remote(codec: &dyn HashShuffleCodec, block: DataBlock) -> AggregateMeta {
        let schema = params().spill_schema();
        let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
        let item = encode_block_for_thread(
            0,
            codec.encode(block).unwrap(),
            &arrow_schema,
            &make_ipc_options(None).unwrap(),
            None,
        )
        .unwrap();
        let block = deserialize_flight_data(strip_tid(item), &schema, &arrow_schema).unwrap();
        take_meta(codec.decode(block).unwrap())
    }

    /// Row shuffle spills states into the partition they would be shuffled to, so
    /// a spilled partition must reach the same thread as the states in memory.
    #[test]
    fn test_row_scatter_sends_spilled_partitions_to_their_thread() -> Result<()> {
        let scatter = scatter(AggregateShuffleMode::Row);

        let blocks = scatter.execute(DataBlock::empty_with_meta(Box::new(
            AggregateMeta::AggregatePayload(AggregatePayload {
                bucket: 0,
                payload: payload((0..100).collect()),
                max_partition_count: 0,
            }),
        )))?;
        assert_eq!(blocks.len(), 6);
        let rows = blocks
            .into_iter()
            .map(|block| match take_meta(block) {
                AggregateMeta::AggregatePayload(p) => p.payload.len(),
                meta => panic!("unexpected {meta:?}"),
            })
            .sum::<usize>();
        assert_eq!(rows, 100);

        let spilled = partitioned(PartitionedData::BucketSpilled(
            (0..6).rev().map(spilled).collect(),
        ));
        for (partition, block) in scatter.execute(spilled)?.into_iter().enumerate() {
            let AggregateMeta::Partitioned {
                data: PartitionedData::BucketSpilled(payloads),
                ..
            } = take_meta(block)
            else {
                panic!("expected spilled payloads")
            };
            assert_eq!(payloads.len(), 1);
            assert_eq!(payloads[0].bucket, partition as isize);
        }
        Ok(())
    }

    /// Each bucket reaches one thread, and nodes get the buckets evenly even when
    /// there are more buckets than threads.
    #[test]
    fn test_bucket_scatter_spreads_buckets_over_nodes() -> Result<()> {
        let scatter = scatter(AggregateShuffleMode::Bucket(8));
        let block = partitioned(PartitionedData::AggregatePayload(
            (0..8)
                .map(|bucket| AggregatePayload {
                    bucket,
                    payload: payload(vec![bucket as i64]),
                    max_partition_count: 0,
                })
                .collect(),
        ));

        let mut seen = HashSet::new();
        let mut per_node = [0; 3];
        for (partition, block) in scatter.execute(block)?.into_iter().enumerate() {
            let AggregateMeta::Partitioned {
                data: PartitionedData::AggregatePayload(payloads),
                ..
            } = take_meta(block)
            else {
                panic!("expected aggregate payloads")
            };
            for p in payloads {
                assert!(seen.insert(p.bucket), "bucket {} sent twice", p.bucket);
                per_node[partition / 2] += 1;
            }
        }
        assert_eq!(seen.len(), 8);
        assert_eq!(per_node, [3, 3, 2]);
        Ok(())
    }

    #[test]
    fn test_scatter_rejects_unexpected_partitions() {
        let builder = AggregateShuffleScatter::builder(params(), AggregateShuffleMode::Row, 6);
        assert!(builder(&NODES[..2]).is_err());
    }

    #[test]
    fn test_remote_states_round_trip() {
        let codec = codec();

        let block = DataBlock::empty_with_meta(Box::new(AggregateMeta::AggregatePayload(
            AggregatePayload {
                bucket: 5,
                payload: payload(vec![11, 22, 33]),
                max_partition_count: 0,
            },
        )));
        let AggregateMeta::Serialized(p) = remote(codec.as_ref(), block) else {
            panic!("expected serialized states")
        };
        assert_eq!(p.bucket, 5);
        assert_eq!(keys(&p.data_block), vec![11, 22, 33]);

        let block = partitioned(PartitionedData::AggregatePayload(vec![
            AggregatePayload {
                bucket: 4,
                payload: payload(vec![1, 2]),
                max_partition_count: 0,
            },
            AggregatePayload {
                bucket: 9,
                payload: payload(vec![3]),
                max_partition_count: 0,
            },
        ]));
        let AggregateMeta::Partitioned {
            data: PartitionedData::Serialized(payloads),
            ..
        } = remote(codec.as_ref(), block)
        else {
            panic!("expected partitioned states")
        };
        let payloads = payloads
            .iter()
            .map(|p| (p.bucket, keys(&p.data_block)))
            .collect::<Vec<_>>();
        assert_eq!(payloads, vec![(4, vec![1, 2]), (9, vec![3])]);

        let block = partitioned(PartitionedData::BucketSpilled(vec![spilled(6)]));
        let AggregateMeta::Partitioned {
            data: PartitionedData::BucketSpilled(payloads),
            ..
        } = remote(codec.as_ref(), block)
        else {
            panic!("expected spilled payloads")
        };
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].bucket, 6);
        assert_eq!(payloads[0].location, "spill/6");
        assert_eq!(payloads[0].row_group.num_rows(), 17);
    }

    #[test]
    fn test_decode_rejects_block_without_aggregate_meta() {
        let codec = codec();
        let block = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![1])]);
        assert!(codec.decode(block).is_err());
    }

    /// Splitting states for the send queue moves whole pages: every group ends up
    /// in exactly one part, and each part still encodes for a remote node.
    #[test]
    fn test_split_keeps_every_group_once() -> Result<()> {
        let codec = codec();
        let payload = payload((0..100_000).collect());
        let max_bytes = payload.memory_size() / 2;
        let mut meta = AggregateMeta::AggregatePayload(AggregatePayload {
            bucket: 3,
            payload,
            max_partition_count: 0,
        });

        let rest = meta.split(max_bytes)?;
        assert!(!rest.is_empty(), "states spanning pages must be split");

        let mut keys_seen = vec![];
        for part in std::iter::once(Box::new(meta) as BlockMetaInfoPtr).chain(rest) {
            assert!(part.output_stats().unwrap().bytes <= max_bytes);
            let AggregateMeta::Partitioned {
                data: PartitionedData::Serialized(payloads),
                ..
            } = remote(codec.as_ref(), DataBlock::empty_with_meta(part))
            else {
                panic!("expected partitioned states")
            };
            for p in payloads {
                assert_eq!(p.bucket, 3);
                keys_seen.extend(keys(&p.data_block));
            }
        }
        keys_seen.sort();
        assert_eq!(keys_seen, (0..100_000).collect::<Vec<_>>());
        Ok(())
    }
}
