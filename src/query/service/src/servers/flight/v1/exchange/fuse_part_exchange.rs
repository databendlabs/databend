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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Pipeline;
use databend_common_settings::FlightCompression;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::operations::BlockPartitionMeta;

use super::DataExchange;
use super::DefaultExchangeInjector;
use super::ExchangeInjector;
use super::ExchangeSorting;
use super::MergeExchangeParams;
use super::ShuffleExchangeParams;
use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::scatter::FlightScatter;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;

pub struct FusePartExchangeInjector {
    default_injector: Arc<dyn ExchangeInjector>,
}

impl FusePartExchangeInjector {
    pub fn create() -> Arc<dyn ExchangeInjector> {
        Arc::new(Self {
            default_injector: DefaultExchangeInjector::create(),
        })
    }
}

fn build_bucket_to_output(
    destination_ids: &[String],
    cache_ids: &HashMap<String, String>,
) -> Result<Vec<usize>> {
    let mut outputs_by_cache_id = destination_ids
        .iter()
        .enumerate()
        .map(|(output, id)| {
            cache_ids
                .get(id)
                .map(|cache_id| (cache_id.clone(), id.clone(), output))
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Cannot find cache id for exchange destination {id}"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    outputs_by_cache_id
        .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    Ok(outputs_by_cache_id
        .into_iter()
        .map(|(_, _, output)| output)
        .collect())
}

impl ExchangeInjector for FusePartExchangeInjector {
    fn flight_scatter(
        &self,
        ctx: &Arc<QueryContext>,
        exchange: &DataExchange,
    ) -> Result<Arc<Box<dyn FlightScatter>>> {
        let DataExchange::NodeToNodeExchange(exchange) = exchange else {
            return Err(ErrorCode::Internal(
                "Fuse block partition exchange requires a node-to-node exchange",
            ));
        };

        let cache_ids = ctx
            .get_cluster()
            .get_nodes()
            .iter()
            .map(|node| (node.id.clone(), node.cache_id.clone()))
            .collect::<HashMap<_, _>>();
        let bucket_to_output = build_bucket_to_output(&exchange.destination_ids, &cache_ids)?;
        Ok(Arc::new(Box::new(FusePartFlightScatter {
            bucket_to_output,
        })))
    }

    fn exchange_sorting(&self) -> Option<Arc<dyn ExchangeSorting>> {
        self.default_injector.exchange_sorting()
    }

    fn apply_merge_serializer(
        &self,
        params: &MergeExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.default_injector
            .apply_merge_serializer(params, compression, pipeline)
    }

    fn apply_shuffle_serializer(
        &self,
        params: &ShuffleExchangeParams,
        compression: Option<FlightCompression>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.default_injector
            .apply_shuffle_serializer(params, compression, pipeline)
    }

    fn apply_merge_deserializer(
        &self,
        params: &MergeExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.default_injector
            .apply_merge_deserializer(params, pipeline)
    }

    fn apply_shuffle_deserializer(
        &self,
        params: &ShuffleExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.default_injector
            .apply_shuffle_deserializer(params, pipeline)
    }
}

struct FusePartFlightScatter {
    /// Hash buckets are ordered by persistent cache id, while exchange outputs use destination id
    /// order. This map translates a stable hash bucket into the corresponding output position.
    bucket_to_output: Vec<usize>,
}

impl FlightScatter for FusePartFlightScatter {
    fn name(&self) -> &'static str {
        "FusePartFlightScatter"
    }

    fn execute(&self, mut data_block: DataBlock) -> Result<Vec<DataBlock>> {
        if !data_block.is_empty() {
            return Err(ErrorCode::Internal(
                "Fuse block partition exchange received a non-empty data block",
            ));
        }
        if self.bucket_to_output.is_empty() {
            return Err(ErrorCode::Internal(
                "Fuse block partition exchange has no destination",
            ));
        }

        let meta = data_block.take_meta().ok_or_else(|| {
            ErrorCode::Internal("Fuse block partition exchange received data without metadata")
        })?;
        let meta = BlockPartitionMeta::downcast_from(meta).ok_or_else(|| {
            ErrorCode::Internal("Fuse block partition exchange received unexpected metadata")
        })?;

        let mut partitions = vec![Vec::new(); self.bucket_to_output.len()];
        for part in meta.part_ptr {
            FuseBlockPartInfo::from_part(&part)?;
            let bucket = (part.hash() % self.bucket_to_output.len() as u64) as usize;
            partitions[self.bucket_to_output[bucket]].push(part);
        }

        Ok(partitions
            .into_iter()
            .map(|parts| match parts.is_empty() {
                true => DataBlock::empty(),
                false => DataBlock::empty_with_meta(BlockPartitionMeta::create(parts)),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use databend_common_catalog::plan::PartInfoPtr;
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::BlockMetaInfoPtr;
    use databend_common_expression::Scalar;
    use databend_common_io::prelude::bincode_deserialize_from_slice;
    use databend_common_io::prelude::bincode_serialize_into_buf;
    use databend_common_storages_fuse::FuseLazyPartInfo;
    use databend_storages_common_pruner::BlockMetaIndex;
    use databend_storages_common_pruner::VirtualBlockMetaIndex;
    use databend_storages_common_table_meta::meta::ColumnMeta;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::Compression;
    use databend_storages_common_table_meta::meta::SingleColumnMeta;

    use super::*;

    fn part(location: &str, block_idx: usize) -> PartInfoPtr {
        FuseBlockPartInfo::create(
            location.to_string(),
            Some((format!("{location}.bloom"), 1)),
            32,
            10,
            HashMap::from([(1, ColumnMeta::Parquet(SingleColumnMeta::new(11, 22, 10)))]),
            Some(HashMap::from([(
                1,
                ColumnStatistics::new(Scalar::from(1_i64), Scalar::from(9_i64), 0, 80, Some(9)),
            )])),
            Compression::Lz4Raw,
            Some((Scalar::from(1_i64), Scalar::from(9_i64))),
            Some(BlockMetaIndex {
                block_idx,
                block_id: block_idx + 100,
                block_location: location.to_string(),
                segment_location: "segment-1".to_string(),
                snapshot_location: Some("snapshot-1".to_string()),
                range: Some(2..8),
                virtual_block_meta: Some(VirtualBlockMetaIndex {
                    virtual_block_location: format!("{location}.virtual"),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            None,
        )
    }

    fn take_parts(mut block: DataBlock) -> Vec<PartInfoPtr> {
        let Some(meta) = block.take_meta() else {
            return vec![];
        };
        BlockPartitionMeta::downcast_from(meta).unwrap().part_ptr
    }

    #[test]
    fn test_fuse_part_scatter_routes_every_part_once() -> Result<()> {
        let bucket_to_output = vec![2, 0, 3, 1];
        let scatter = FusePartFlightScatter {
            bucket_to_output: bucket_to_output.clone(),
        };
        let parts = (0..100)
            .map(|index| part(&format!("block-{index}"), index))
            .collect::<Vec<_>>();
        let expected = parts
            .iter()
            .map(|part| {
                let bucket = (part.hash() % bucket_to_output.len() as u64) as usize;
                (
                    FuseBlockPartInfo::from_part(part).unwrap().location.clone(),
                    bucket_to_output[bucket],
                )
            })
            .collect::<HashMap<_, _>>();

        let outputs = scatter.execute(DataBlock::empty_with_meta(BlockPartitionMeta::create(
            parts,
        )))?;
        assert_eq!(outputs.len(), bucket_to_output.len());

        let mut seen = HashSet::new();
        for (output, block) in outputs.into_iter().enumerate() {
            for part in take_parts(block) {
                let part = FuseBlockPartInfo::from_part(&part)?;
                assert_eq!(expected[&part.location], output);
                assert!(seen.insert(part.location.clone()));
                assert_eq!(part.block_meta_index.as_ref().unwrap().range, Some(2..8));
            }
        }
        assert_eq!(seen.len(), expected.len());
        Ok(())
    }

    #[test]
    fn test_cache_id_order_is_independent_of_exchange_order() -> Result<()> {
        let cache_ids = HashMap::from([
            ("node-a".to_string(), "cache-2".to_string()),
            ("node-b".to_string(), "cache-1".to_string()),
            ("node-c".to_string(), "cache-3".to_string()),
        ]);
        let first = vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
        ];
        let second = vec![
            "node-c".to_string(),
            "node-a".to_string(),
            "node-b".to_string(),
        ];

        let first_nodes = build_bucket_to_output(&first, &cache_ids)?
            .into_iter()
            .map(|output| first[output].clone())
            .collect::<Vec<_>>();
        let second_nodes = build_bucket_to_output(&second, &cache_ids)?
            .into_iter()
            .map(|output| second[output].clone())
            .collect::<Vec<_>>();

        assert_eq!(first_nodes, vec!["node-b", "node-a", "node-c"]);
        assert_eq!(first_nodes, second_nodes);
        Ok(())
    }

    #[test]
    fn test_fuse_part_scatter_does_not_emit_meta_for_empty_destinations() -> Result<()> {
        let scatter = FusePartFlightScatter {
            bucket_to_output: vec![0, 1, 2, 3],
        };
        let outputs = scatter.execute(DataBlock::empty_with_meta(BlockPartitionMeta::create(
            vec![part("only-block", 0)],
        )))?;

        assert_eq!(outputs.len(), 4);
        assert_eq!(
            outputs
                .iter()
                .filter(|block| block.get_meta().is_some())
                .count(),
            1
        );
        Ok(())
    }

    #[test]
    fn test_fuse_part_scatter_rejects_missing_meta_and_non_block_parts() {
        let scatter = FusePartFlightScatter {
            bucket_to_output: vec![0, 1],
        };
        assert!(scatter.execute(DataBlock::empty()).is_err());

        let lazy_part = FuseLazyPartInfo::create(0, ("segment-0".to_string(), 1));
        let block = DataBlock::empty_with_meta(BlockPartitionMeta::create(vec![lazy_part]));
        assert!(scatter.execute(block).is_err());
    }

    #[test]
    fn test_block_partition_meta_bincode_round_trip() -> Result<()> {
        let meta: Option<BlockMetaInfoPtr> = Some(BlockPartitionMeta::create(vec![part(
            "block-round-trip",
            7,
        )]));
        let mut encoded = Vec::new();
        bincode_serialize_into_buf(&mut encoded, &meta)?;
        let decoded: Option<BlockMetaInfoPtr> = bincode_deserialize_from_slice(&encoded)?;
        let decoded = BlockPartitionMeta::downcast_from(decoded.unwrap()).unwrap();
        let part = FuseBlockPartInfo::from_part(&decoded.part_ptr[0])?;

        assert_eq!(part.location, "block-round-trip");
        assert_eq!(
            part.bloom_filter_index_location.as_ref().unwrap().0,
            "block-round-trip.bloom"
        );
        assert_eq!(part.bloom_filter_index_size, 32);
        assert_eq!(part.columns_meta[&1].offset_length(), (11, 22));
        assert_eq!(
            part.columns_stat.as_ref().unwrap()[&1].min(),
            &Scalar::from(1_i64)
        );
        assert_eq!(
            part.sort_min_max.clone(),
            Some((Scalar::from(1_i64), Scalar::from(9_i64)))
        );
        assert_eq!(part.block_meta_index.as_ref().unwrap(), &BlockMetaIndex {
            block_idx: 7,
            block_id: 107,
            block_location: "block-round-trip".to_string(),
            segment_location: "segment-1".to_string(),
            snapshot_location: Some("snapshot-1".to_string()),
            range: Some(2..8),
            virtual_block_meta: Some(VirtualBlockMetaIndex {
                virtual_block_location: "block-round-trip.virtual".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        });
        Ok(())
    }
}
