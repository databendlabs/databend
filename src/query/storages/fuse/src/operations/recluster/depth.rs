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

use std::borrow::Borrow;

use databend_common_catalog::plan::ReclusterDepthKind;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use indexmap::IndexSet;

use super::hilbert_recluster::build_hilbert_candidates;
use super::overlap_selection::identity_overlaps;
use crate::statistics::calculate_block_overlap_depths;
use crate::statistics::cluster_stats_scalar_overlap;

/// Block-level stats needed to compare recluster input and output overlap depth.
#[derive(Clone)]
pub(crate) struct ReclusterDepthStats {
    cluster_stats: ClusterStatistics,
    vector_stats: Option<VectorColumnStatistics>,
}

impl Borrow<ClusterStatistics> for ReclusterDepthStats {
    fn borrow(&self) -> &ClusterStatistics {
        &self.cluster_stats
    }
}

/// Collect the depth input for one block under the active recluster strategy.
pub(crate) fn collect_depth_stats(
    kind: &ReclusterDepthKind,
    block: &BlockMeta,
    cluster_stats: ClusterStatistics,
) -> ReclusterDepthStats {
    let vector_stats = match kind {
        ReclusterDepthKind::Vector {
            column_id,
            distance_type,
            ..
        } => block
            .vector_stats
            .as_ref()
            .and_then(|stats| stats.get(&(*column_id, *distance_type)))
            .cloned(),
        _ => None,
    };
    ReclusterDepthStats {
        cluster_stats,
        vector_stats,
    }
}

/// Calculate the maximum overlap depth for a set of blocks.
pub(crate) fn calculate_max_depth(
    kind: &ReclusterDepthKind,
    stats: &[ReclusterDepthStats],
) -> Result<usize> {
    match kind {
        ReclusterDepthKind::Linear { cluster_key_types } => {
            let ranges = stats
                .iter()
                .map(|stats| {
                    (
                        stats.cluster_stats.min().clone(),
                        stats.cluster_stats.max().clone(),
                    )
                })
                .collect::<Vec<_>>();
            Ok(calculate_block_overlap_depths(&ranges, cluster_key_types)?
                .into_iter()
                .map(|stats| stats.depth)
                .max()
                .unwrap_or(0))
        }
        ReclusterDepthKind::Hilbert {
            require_scalar_overlap,
        } => Ok(build_hilbert_candidates(stats, |left, right| {
            !require_scalar_overlap
                || cluster_stats_scalar_overlap(
                    &stats[left].cluster_stats,
                    &stats[right].cluster_stats,
                )
        })
        .max_depth()),
        ReclusterDepthKind::Vector {
            distance_type,
            require_scalar_overlap,
            ..
        } => {
            let mut overlaps = identity_overlaps(stats.len());
            for left in 0..stats.len() {
                for right in left + 1..stats.len() {
                    if *require_scalar_overlap
                        && !cluster_stats_scalar_overlap(
                            &stats[left].cluster_stats,
                            &stats[right].cluster_stats,
                        )
                    {
                        continue;
                    }
                    let vector_overlap = match (
                        stats[left].vector_stats.as_ref(),
                        stats[right].vector_stats.as_ref(),
                    ) {
                        (Some(left), Some(right)) => left.spheres_overlap(right, *distance_type)?,
                        _ => true,
                    };
                    if vector_overlap {
                        overlaps[left].insert(right);
                        overlaps[right].insert(left);
                    }
                }
            }
            Ok(overlaps.iter().map(IndexSet::len).max().unwrap_or(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::number::NumberScalar;

    use super::*;

    fn scalar(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    fn stats(min: Vec<Scalar>, max: Vec<Scalar>) -> ReclusterDepthStats {
        ReclusterDepthStats {
            cluster_stats: ClusterStatistics::new(0, min, max, 0, None),
            vector_stats: None,
        }
    }

    #[test]
    fn test_linear_max_depth() -> Result<()> {
        let stats = vec![
            stats(vec![scalar(0)], vec![scalar(10)]),
            stats(vec![scalar(2)], vec![scalar(8)]),
            stats(vec![scalar(4)], vec![scalar(6)]),
            stats(vec![scalar(20)], vec![scalar(30)]),
        ];
        let kind = ReclusterDepthKind::Linear {
            cluster_key_types: vec![DataType::Number(
                databend_common_expression::types::number::NumberDataType::Int32,
            )],
        };
        assert_eq!(calculate_max_depth(&kind, &stats)?, 3);
        Ok(())
    }

    #[test]
    fn test_hilbert_max_depth() -> Result<()> {
        let tuple =
            |values: &[i32]| Scalar::Tuple(values.iter().copied().map(scalar).collect::<Vec<_>>());
        let stats = vec![
            stats(vec![tuple(&[0, 0])], vec![tuple(&[10, 10])]),
            stats(vec![tuple(&[1, 1])], vec![tuple(&[4, 4])]),
            stats(vec![tuple(&[6, 6])], vec![tuple(&[9, 9])]),
        ];
        let kind = ReclusterDepthKind::Hilbert {
            require_scalar_overlap: false,
        };
        assert_eq!(calculate_max_depth(&kind, &stats)?, 2);
        Ok(())
    }
}
