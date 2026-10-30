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
use databend_common_expression::AggregatePayload;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::group_hash_entries;
use databend_common_expression::group_hash_route_index;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::Transform;
use databend_common_pipeline_transforms::Transformer;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateStatesMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionedData;
use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::scatter::FlightScatter;

pub trait LocalScatter: Sync + Send {
    fn name(&self) -> &'static str;

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>>;
}

pub struct LocalScatterTransform {
    scatter: Arc<Box<dyn LocalScatter>>,
}

impl LocalScatterTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        scatter: Arc<Box<dyn LocalScatter>>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Transformer::create(input, output, LocalScatterTransform {
            scatter,
        }))
    }
}

impl Transform for LocalScatterTransform {
    const NAME: &'static str = "LocalScatterTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let blocks = self.scatter.execute(data)?;
        Ok(DataBlock::empty_with_meta(ExchangeShuffleMeta::create(
            blocks,
        )))
    }

    fn name(&self) -> String {
        format!("LocalScatterTransform({})", self.scatter.name())
    }
}

/// Routes the output of the partial aggregates to `partitions` final aggregates by
/// `group_hash_route_index` of the group hash, so that the states and the raw rows of a group key
/// reach the same one. A partial aggregate table has a single partition, so the number of final
/// aggregates, whatever the cluster size, never fragments it.
pub struct AggregateScatter {
    partitions: usize,
    params: Arc<AggregatorParams>,
    /// Partitions read on this node, which take states in memory. The states sent to other
    /// partitions are serialized with `states_schema`.
    local_partitions: Range<usize>,
    states_schema: DataSchemaRef,
}

impl AggregateScatter {
    /// A scatter to the final aggregates of this node.
    pub fn local(partitions: usize, params: Arc<AggregatorParams>) -> Self {
        Self::create(partitions, 0..partitions, params)
    }

    /// A scatter to the final aggregates of all nodes, `local_partitions` of them on this node.
    pub fn create(
        partitions: usize,
        local_partitions: Range<usize>,
        params: Arc<AggregatorParams>,
    ) -> Self {
        AggregateScatter {
            partitions,
            states_schema: params.spill_schema(),
            params,
            local_partitions,
        }
    }

    /// The partition of each raw row.
    fn raw_partitions(&self, block: &DataBlock) -> Vec<u64> {
        let mut hashes = vec![0; block.num_rows()];
        let group_columns = ProjectedBlock::project(&self.params.group_columns, block);
        group_hash_entries(group_columns, &mut hashes);

        let partitions = self.partitions as u64;
        for hash in hashes.iter_mut() {
            *hash = group_hash_route_index(*hash, partitions);
        }
        hashes
    }

    /// Group the states of partial aggregate tables by partition.
    fn scatter_states(&self, payloads: Vec<AggregatePayload>) -> Vec<Vec<AggregatePayload>> {
        if self.partitions == 1 {
            return vec![payloads];
        }

        let mut partitions = (0..self.partitions).map(|_| vec![]).collect::<Vec<_>>();
        for payload in payloads {
            let scattered = payload.payload.scatter_into_buckets(self.partitions);
            for (partition, part) in scattered.into_iter().enumerate() {
                if part.len() != 0 {
                    partitions[partition].push(AggregatePayload {
                        bucket: payload.bucket,
                        payload: part,
                        max_partition_count: payload.max_partition_count,
                    });
                }
            }
        }
        partitions
    }

    fn take_states(block: DataBlock) -> Result<Vec<AggregatePayload>> {
        let meta = block
            .get_owned_meta()
            .and_then(AggregateMeta::downcast_from)
            .ok_or_else(|| {
                ErrorCode::Internal("AggregateScatter expects partial aggregate states")
            })?;
        match meta {
            AggregateMeta::Partitioned {
                data: PartitionedData::AggregatePayload(payloads),
                ..
            } => Ok(payloads),
            other => Err(ErrorCode::Internal(format!(
                "AggregateScatter cannot route {other:?}"
            ))),
        }
    }

    fn states_block(payloads: Vec<AggregatePayload>) -> DataBlock {
        AggregateMeta::Partitioned {
            bucket: None,
            data: PartitionedData::AggregatePayload(payloads),
        }
        .into_datablock()
    }

    /// Serialize the states for a remote node, one run of rows per bucket.
    fn serialize_states(&self, payloads: Vec<AggregatePayload>) -> Result<DataBlock> {
        let mut buckets = Vec::with_capacity(payloads.len());
        let mut row_counts = Vec::with_capacity(payloads.len());
        let mut blocks = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let block = payload.payload.aggregate_flush_all()?;
            if block.num_rows() == 0 {
                continue;
            }
            buckets.push(payload.bucket);
            row_counts.push(block.num_rows());
            blocks.push(block);
        }
        if blocks.is_empty() {
            return Ok(DataBlock::empty());
        }
        DataBlock::concat(&blocks)?.add_meta(Some(AggregateStatesMeta::create(
            self.states_schema.clone(),
            buckets,
            row_counts,
        )))
    }
}

impl LocalScatter for AggregateScatter {
    fn name(&self) -> &'static str {
        "Aggregate"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if data_block.get_meta().is_none() {
            let indices = self.raw_partitions(&data_block);
            return data_block.scatter(&indices, self.partitions);
        }
        Ok(self
            .scatter_states(Self::take_states(data_block)?)
            .into_iter()
            .map(|payloads| match payloads.is_empty() {
                true => DataBlock::empty(),
                false => Self::states_block(payloads),
            })
            .collect())
    }
}

impl FlightScatter for AggregateScatter {
    fn name(&self) -> &'static str {
        "Aggregate"
    }

    fn execute(&self, _: DataBlock) -> Result<Vec<DataBlock>> {
        Err(ErrorCode::Internal(
            "AggregateScatter only routes through the hash shuffle",
        ))
    }

    fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        Ok(Some(self.raw_partitions(data_block)))
    }

    /// Route the states of a partial aggregate table: states stay in memory for the partitions
    /// of this node and are serialized for the others.
    fn route_meta_block(&self, data_block: DataBlock) -> Result<Option<Vec<DataBlock>>> {
        self.scatter_states(Self::take_states(data_block)?)
            .into_iter()
            .enumerate()
            .map(|(partition, payloads)| match payloads.is_empty() {
                true => Ok(DataBlock::empty()),
                false if self.local_partitions.contains(&partition) => {
                    Ok(Self::states_block(payloads))
                }
                false => self.serialize_states(payloads),
            })
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bumpalo::Bump;
    use databend_common_expression::AggregateHashTable;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::HashTableConfig;
    use databend_common_expression::ProbeState;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    const KEYS: u64 = 2000;

    fn params() -> Result<Arc<AggregatorParams>> {
        let key_type = DataType::Number(NumberDataType::UInt64);
        AggregatorParams::try_create(
            Arc::new(DataSchema::new(vec![DataField::new(
                "key",
                key_type.clone(),
            )])),
            vec![key_type],
            &[0],
            &[],
            &[],
            true,
            1024,
            1024 * 1024,
        )
    }

    fn keys_block() -> DataBlock {
        DataBlock::new_from_columns(vec![UInt64Type::from_data((0..KEYS).collect::<Vec<_>>())])
    }

    fn keys(block: &DataBlock, column: usize) -> Vec<u64> {
        let column = block.get_by_offset(column).to_column();
        UInt64Type::try_downcast_column(&column)
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    /// The states of a partial aggregate table holding every key once.
    fn states_block(params: &AggregatorParams) -> Result<DataBlock> {
        let config = HashTableConfig::partial_aggregate();
        let mut table = AggregateHashTable::new(
            params.group_data_types.clone(),
            vec![],
            config,
            Arc::new(Bump::new()),
        );
        let block = keys_block();
        table.add_groups(
            &mut ProbeState::default(),
            ProjectedBlock::project(&params.group_columns, &block),
            &[],
            (&[]).into(),
            block.num_rows(),
        )?;
        let payloads = table
            .payload
            .into_non_empty_bucket_payloads()
            .map(|(bucket, payload)| AggregatePayload {
                bucket: bucket as isize,
                payload,
                max_partition_count: 0,
            })
            .collect();
        Ok(AggregateScatter::states_block(payloads))
    }

    /// Keys of the states in `block`, in memory or serialized for a remote node.
    fn state_keys(mut block: DataBlock) -> Result<Vec<u64>> {
        let meta = block.take_meta().unwrap();
        let meta = match AggregateStatesMeta::downcast_ref_from(&meta).is_some() {
            true => AggregateStatesMeta::downcast_from(meta)
                .unwrap()
                .into_aggregate_meta(block)?,
            false => AggregateMeta::downcast_from(meta).unwrap(),
        };
        let mut result = vec![];
        match meta {
            AggregateMeta::Partitioned {
                data: PartitionedData::AggregatePayload(payloads),
                ..
            } => {
                for payload in payloads {
                    result.extend(keys(&payload.payload.aggregate_flush_all()?, 0));
                }
            }
            AggregateMeta::Partitioned {
                data: PartitionedData::Serialized(payloads),
                ..
            } => {
                for payload in payloads {
                    result.extend(keys(&payload.data_block, 0));
                }
            }
            other => unreachable!("{other:?}"),
        }
        Ok(result)
    }

    fn partition_of_keys(blocks: Vec<Vec<u64>>) -> HashMap<u64, usize> {
        let mut partitions = HashMap::new();
        for (partition, keys) in blocks.into_iter().enumerate() {
            for key in keys {
                assert!(
                    partitions.insert(key, partition).is_none(),
                    "key {key} twice"
                );
            }
        }
        assert_eq!(partitions.len(), KEYS as usize);
        partitions
    }

    /// The states and the raw rows of a key reach the same final aggregate, on this node or
    /// through the hash shuffle, for any number of partitions.
    #[test]
    fn test_states_and_raw_rows_of_a_key_meet() -> Result<()> {
        let params = params()?;
        for partitions in [1, 3, 8, 48] {
            let local = AggregateScatter::local(partitions, params.clone());
            let raw = LocalScatter::execute(&local, keys_block())?
                .iter()
                .map(|block| keys(block, 0))
                .collect();
            let raw = partition_of_keys(raw);

            let states = LocalScatter::execute(&local, states_block(&params)?)?
                .into_iter()
                .map(|block| match block.get_meta() {
                    None => Ok(vec![]),
                    Some(_) => state_keys(block),
                })
                .collect::<Result<_>>()?;
            assert_eq!(partition_of_keys(states), raw, "{partitions} partitions");

            // Through the hash shuffle with partition 0 on this node.
            let global = AggregateScatter::create(partitions, 0..1, params.clone());
            let indices = global.scatter_indices(&keys_block())?.unwrap();
            let shuffled = (0..KEYS)
                .zip(indices)
                .map(|(k, p)| (k, p as usize))
                .collect::<HashMap<_, _>>();
            assert_eq!(raw, shuffled);

            let routed = global.route_meta_block(states_block(&params)?)?.unwrap();
            for (partition, block) in routed.iter().enumerate().skip(1) {
                if block.get_meta().is_some() {
                    assert!(
                        AggregateStatesMeta::downcast_ref_from(block.get_meta().unwrap()).is_some()
                    );
                    assert_ne!(block.num_rows(), 0, "partition {partition}");
                }
            }
            let states = routed
                .into_iter()
                .map(|block| match block.get_meta() {
                    None => Ok(vec![]),
                    Some(_) => state_keys(block),
                })
                .collect::<Result<_>>()?;
            assert_eq!(partition_of_keys(states), raw, "{partitions} partitions");
        }
        Ok(())
    }

    /// The keys of a final aggregate still spread over the low bits of their group hash, which
    /// index its hash table.
    #[test]
    fn test_route_spreads_low_hash_bits() -> Result<()> {
        let params = params()?;
        let block = keys_block();
        let mut hashes = vec![0; block.num_rows()];
        group_hash_entries(
            ProjectedBlock::project(&params.group_columns, &block),
            &mut hashes,
        );

        let partitions = 16;
        let scatter = AggregateScatter::local(partitions, params);
        let routed = scatter.raw_partitions(&block);
        for partition in 0..partitions as u64 {
            let low_bits = hashes
                .iter()
                .zip(&routed)
                .filter(|(_, routed)| **routed == partition)
                .map(|(hash, _)| hash % partitions as u64)
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(low_bits.len(), partitions, "partition {partition}");
        }
        Ok(())
    }
}
