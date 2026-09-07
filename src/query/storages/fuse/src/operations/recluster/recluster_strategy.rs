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

use std::cmp;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use databend_common_catalog::plan::ClusterLevelLogStats;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Expr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_sql::ClusterKeys;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::table::ClusterType;

use crate::FuseTable;
use crate::MAX_RECLUSTER_DEPTH;
use crate::SegmentLocation;
use crate::operations::common::BlockMetaIndex as BlockIndex;
use crate::operations::recluster::HilbertReclusterStrategy;
use crate::operations::recluster::LinearReclusterStrategy;
use crate::operations::recluster::VectorReclusterStrategy;
use crate::statistics::PreparedClusterKeyExpr;
use crate::statistics::VectorClusterInfo;
use crate::statistics::cluster_stats_from_col_stats;
use crate::statistics::prepare_cluster_key_exprs;

/// Recluster candidate selection mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclusterMode {
    /// Legacy one-window probing with tighter rewrite selection.
    Conservative,
    /// Broader probing that mixes levels 1 through 3 while keeping other levels separate.
    Aggressive,
}

/// Immutable inputs shared by all recluster strategies.
#[derive(Clone)]
pub(crate) struct ReclusterProperties {
    pub(crate) mode: ReclusterMode,
    pub(crate) depth_threshold: f64,
    pub(crate) block_thresholds: BlockThresholds,
    pub(crate) cluster_key_info: ClusterKeyInfo,
    pub(crate) partition_key_count: usize,
    pub(crate) memory_threshold: usize,
    pub(crate) prepared_cluster_key_exprs: Vec<PreparedClusterKeyExpr>,
    pub(crate) scalar_cluster_key_types: Vec<DataType>,
}

impl ReclusterProperties {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_create(
        table: &FuseTable,
        schema: &TableSchemaRef,
        cluster_keys: ClusterKeys,
        mode: ReclusterMode,
        depth_threshold: f64,
        block_thresholds: BlockThresholds,
        cluster_key_info: ClusterKeyInfo,
        memory_threshold: usize,
    ) -> Result<(Self, Arc<dyn ReclusterStrategy>)> {
        let (cluster_key_exprs, strategy): (Vec<Expr<usize>>, Arc<dyn ReclusterStrategy>) =
            match cluster_keys {
                ClusterKeys::Linear(keys) => {
                    if keys.is_empty() {
                        return Err(ErrorCode::Internal(
                            "recluster requires non-empty cluster key expressions",
                        ));
                    }
                    (keys, Arc::new(LinearReclusterStrategy))
                }
                ClusterKeys::Vector {
                    mut keys,
                    vector_index,
                } => {
                    let strategy =
                        VectorReclusterStrategy::try_create(table, &mut keys, vector_index)?;
                    (keys, Arc::new(strategy))
                }
                ClusterKeys::Hilbert(dimensions) => {
                    (dimensions, Arc::new(HilbertReclusterStrategy))
                }
            };
        let scalar_cluster_key_types = if cluster_key_info.cluster_type == ClusterType::Hilbert {
            Vec::new()
        } else {
            cluster_key_exprs
                .iter()
                .map(|expr| expr.data_type().clone())
                .collect()
        };
        let prepared_cluster_key_exprs =
            prepare_cluster_key_exprs(&cluster_key_exprs, schema.as_ref());
        let properties = Self {
            mode,
            depth_threshold,
            block_thresholds,
            cluster_key_info,
            partition_key_count: table.partition_key_count(),
            memory_threshold,
            prepared_cluster_key_exprs,
            scalar_cluster_key_types,
        };
        Ok((properties, strategy))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_test(
        schema: &TableSchemaRef,
        cluster_key_exprs: Vec<Expr<usize>>,
        mode: ReclusterMode,
        depth_threshold: f64,
        block_thresholds: BlockThresholds,
        cluster_key_info: ClusterKeyInfo,
        partition_key_count: usize,
        memory_threshold: usize,
        vector_cluster_info: Option<VectorClusterInfo>,
    ) -> (Self, Arc<dyn ReclusterStrategy>) {
        let cluster_key_exprs = cluster_key_exprs
            .into_iter()
            .filter(|expr| !matches!(expr.data_type().remove_nullable(), DataType::Vector(_)))
            .collect::<Vec<_>>();
        let strategy: Arc<dyn ReclusterStrategy> = match vector_cluster_info {
            Some(info) => Arc::new(VectorReclusterStrategy::new(info)),
            None => {
                assert!(
                    !cluster_key_exprs.is_empty(),
                    "recluster requires non-empty cluster key expressions"
                );
                Arc::new(LinearReclusterStrategy)
            }
        };
        let scalar_cluster_key_types = cluster_key_exprs
            .iter()
            .map(|expr| expr.data_type().clone())
            .collect();
        let prepared_cluster_key_exprs =
            prepare_cluster_key_exprs(&cluster_key_exprs, schema.as_ref());
        let properties = Self {
            mode,
            depth_threshold,
            block_thresholds,
            cluster_key_info,
            partition_key_count,
            memory_threshold,
            prepared_cluster_key_exprs,
            scalar_cluster_key_types,
        };
        (properties, strategy)
    }
}

/// Algorithm-specific behavior used by the recluster workflow.
pub(crate) trait ReclusterStrategy: Send + Sync {
    /// Select windows from a partition-local segment slice. ReclusterMutator performs partition
    /// grouping and filters segments without exact partition metadata before calling strategies.
    fn select_segments(
        &self,
        properties: &ReclusterProperties,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>>;

    fn fetch_task_candidates(
        &self,
        properties: &ReclusterProperties,
        group: ReclusterGroup,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>>;

    fn can_reuse_cluster_stats(
        &self,
        properties: &ReclusterProperties,
        stats: &ClusterStatistics,
    ) -> bool {
        stats.cluster_key_id == properties.cluster_key_info.cluster_key_id()
    }

    fn build_cluster_stats(
        &self,
        properties: &ReclusterProperties,
        cluster_stats: Option<&ClusterStatistics>,
        col_stats: &StatisticsOfColumns,
    ) -> ClusterStatistics {
        if let Some(stats) = cluster_stats {
            if self.can_reuse_cluster_stats(properties, stats) {
                return stats.clone();
            }
        }

        cluster_stats_from_col_stats(
            &properties.prepared_cluster_key_exprs,
            col_stats,
            properties.cluster_key_info.cluster_key_id(),
            0,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReclusterGroup {
    /// A single level forms its own group.
    Level(i32),
    /// Aggressive mode groups the low mature levels 1 through 3.
    LowMaturity,
}

impl Ord for ReclusterGroup {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let key = |group: &Self| match group {
            Self::Level(level) => (*level, 0),
            Self::LowMaturity => (1, 1),
        };
        key(self).cmp(&key(other))
    }
}

impl PartialOrd for ReclusterGroup {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ReclusterGroup {
    /// Assign a block's recluster group for the given mode.
    pub(crate) fn assign(level: i32, mode: ReclusterMode) -> ReclusterGroup {
        match mode {
            ReclusterMode::Aggressive if (1..=3).contains(&level) => ReclusterGroup::LowMaturity,
            _ => ReclusterGroup::Level(level),
        }
    }

    /// Return the base level consumed by the execution path, which writes blocks at `level + 1`.
    fn base_level(self, task_indices: &[usize], blocks: &[&ReclusterBlock]) -> i32 {
        match self {
            ReclusterGroup::Level(level) => level,
            ReclusterGroup::LowMaturity => {
                let max_level = task_indices
                    .iter()
                    .map(|idx| blocks[*idx].stats().level)
                    .max()
                    .expect("recluster task must contain blocks");
                if max_level == 1 {
                    return 1;
                }

                let mut total_size = 0;
                let mut max_level_size = 0;
                for &idx in task_indices {
                    let block = blocks[idx];
                    total_size += block.meta.block_size;
                    if block.stats().level == max_level {
                        max_level_size += block.meta.block_size;
                    }
                }

                // The execution path adds one. Keep the final level at max_level unless the
                // highest input level owns at least half of the selected logical bytes.
                if max_level_size * 2 >= total_size {
                    max_level
                } else {
                    max_level - 1
                }
            }
        }
    }
}

impl fmt::Display for ReclusterGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReclusterGroup::Level(level) => write!(f, "{}", level),
            ReclusterGroup::LowMaturity => write!(f, "1-3"),
        }
    }
}

/// Ranking score for a recluster task candidate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CandidateScore {
    pub selected_total_bytes: usize,
    pub max_depth: usize,
    pub average_depth: f64,
}

impl CandidateScore {
    /// Compare scores in descending priority order.
    pub fn cmp_desc(&self, other: &Self) -> cmp::Ordering {
        self.max_depth
            .cmp(&other.max_depth)
            .then_with(|| {
                self.average_depth
                    .partial_cmp(&other.average_depth)
                    .unwrap_or(cmp::Ordering::Equal)
            })
            .then_with(|| self.selected_total_bytes.cmp(&other.selected_total_bytes))
    }
}

/// In-memory rewrite candidate produced from one probed window.
#[derive(Clone)]
pub(crate) struct ReclusterTaskCandidate {
    pub(crate) score: CandidateScore,
    // Empty means a rebuild-only repack candidate.
    pub(crate) selected_blocks: Vec<(usize, Vec<usize>)>,
    pub(crate) base_level: i32,
    pub(crate) input_level_stats: Vec<ClusterLevelLogStats>,
    pub(crate) all_ordered: bool,
}

impl ReclusterTaskCandidate {
    pub(crate) fn selected_block_count(&self) -> usize {
        self.selected_blocks
            .iter()
            .map(|(_, block_indices)| block_indices.len())
            .sum()
    }

    /// Requested output level for logs, or "null" for repack-only candidates.
    /// Perfect output blocks may instead become -1.
    pub(crate) fn requested_output_level(&self) -> String {
        if self.is_repack_only() {
            "null".to_string()
        } else {
            (self.base_level + 1).to_string()
        }
    }

    /// Whether this candidate only repacks unchanged blocks into fewer segments.
    pub(crate) fn is_repack_only(&self) -> bool {
        self.selected_blocks.is_empty()
    }
}

impl fmt::Display for ReclusterTaskCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "requested_output_level={} repack_only={} max_depth={} avg_depth={} block_count={} block_size={}",
            self.requested_output_level(),
            self.is_repack_only(),
            self.score.max_depth,
            self.score.average_depth,
            self.selected_block_count(),
            self.score.selected_total_bytes,
        )
    }
}

/// Cluster statistics for a candidate block.
pub(crate) enum ReclusterBlockStats {
    Original,
    Normalized(ClusterStatistics),
}

/// Block plus the cluster statistics view used for candidate selection.
pub(crate) struct ReclusterBlock {
    pub(crate) index: BlockIndex,
    pub(crate) meta: Arc<BlockMeta>,
    pub(crate) stats: ReclusterBlockStats,
}

impl ReclusterBlock {
    pub(crate) fn stats(&self) -> &ClusterStatistics {
        match &self.stats {
            ReclusterBlockStats::Original => self
                .meta
                .cluster_stats
                .as_ref()
                .expect("Original implies matched cluster_stats"),
            ReclusterBlockStats::Normalized(stats) => stats,
        }
    }
}

/// Segment selected for probing by recluster.
#[derive(Clone)]
pub struct SelectedReclusterSegment {
    pub loc: SegmentLocation,
    pub info: Arc<CompactSegmentInfo>,
}

pub(crate) fn task_candidate(
    group: ReclusterGroup,
    score: CandidateScore,
    task_indices: &[usize],
    blocks: &[&ReclusterBlock],
) -> ReclusterTaskCandidate {
    use std::collections::HashMap;

    let mut selected_block_positions: HashMap<usize, usize> =
        HashMap::with_capacity(task_indices.len());
    let mut selected_blocks = Vec::<(usize, Vec<usize>)>::with_capacity(task_indices.len());
    for &idx in task_indices {
        let block = &blocks[idx];
        if let Some(&position) = selected_block_positions.get(&block.index.segment_idx) {
            selected_blocks[position].1.push(block.index.block_idx);
        } else {
            selected_block_positions.insert(block.index.segment_idx, selected_blocks.len());
            selected_blocks.push((block.index.segment_idx, vec![block.index.block_idx]));
        }
    }

    let base_level = group.base_level(task_indices, blocks);
    let mut stats_by_level = BTreeMap::<i32, ClusterLevelLogStats>::new();
    for &idx in task_indices {
        let block = blocks[idx];
        let level = block.stats().level;
        let stats = stats_by_level
            .entry(level)
            .or_insert_with(|| ClusterLevelLogStats {
                level: Some(level),
                ..Default::default()
            });
        stats.block_count += 1;
        stats.row_count = stats.row_count.saturating_add(block.meta.row_count);
        stats.block_size = stats.block_size.saturating_add(block.meta.block_size);
        stats.file_size = stats.file_size.saturating_add(block.meta.file_size);
    }
    let all_ordered = task_indices
        .iter()
        .all(|idx| matches!(&blocks[*idx].stats, ReclusterBlockStats::Original));
    ReclusterTaskCandidate {
        score,
        selected_blocks,
        base_level,
        input_level_stats: stats_by_level.into_values().collect(),
        all_ordered,
    }
}

pub(crate) fn passes_depth_gate(
    depth_threshold: f64,
    average_depth: f64,
    max_depth: usize,
) -> bool {
    let mature_gate = (2.0 * depth_threshold).min(MAX_RECLUSTER_DEPTH as f64);
    average_depth > depth_threshold || max_depth as f64 >= mature_gate
}
