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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Expr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

use crate::FuseTable;
use crate::MAX_RECLUSTER_DEPTH;
use crate::MIN_RECLUSTER_DEPTH;
use crate::SegmentLocation;
use crate::operations::common::BlockMetaIndex as BlockIndex;
use crate::operations::recluster::LinearReclusterStrategy;
use crate::operations::recluster::VectorReclusterStrategy;
use crate::statistics::PreparedClusterKeyExpr;
use crate::statistics::VectorClusterInfo;
use crate::statistics::get_min_max_stats;
use crate::statistics::prepare_cluster_key_exprs;

/// Recluster candidate selection mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclusterMode {
    /// Legacy one-window probing with tighter rewrite selection.
    Conservative,
    /// Broader probing that groups mature blocks by level ranges.
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
        mut cluster_key_exprs: Vec<Expr<usize>>,
        mode: ReclusterMode,
        depth_threshold: f64,
        block_thresholds: BlockThresholds,
        cluster_key_info: ClusterKeyInfo,
        memory_threshold: usize,
    ) -> Result<(Self, Arc<dyn ReclusterStrategy>)> {
        let vector_indices = cluster_key_exprs
            .iter()
            .enumerate()
            .filter_map(|(idx, expr)| {
                matches!(expr.data_type().remove_nullable(), DataType::Vector(_)).then_some(idx)
            })
            .collect::<Vec<_>>();
        if vector_indices.len() > 1 {
            return Err(ErrorCode::InvalidClusterKeys(
                "Only one vector column is supported in cluster by",
            ));
        }
        let strategy: Arc<dyn ReclusterStrategy> = match vector_indices.first().copied() {
            Some(vector_index) => Arc::new(VectorReclusterStrategy::try_create(
                table,
                &mut cluster_key_exprs,
                vector_index,
            )?),
            None => {
                if cluster_key_exprs.is_empty() {
                    return Err(ErrorCode::Internal(
                        "recluster requires non-empty cluster key expressions",
                    ));
                }
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

        let (min, max) = get_min_max_stats(
            &properties.prepared_cluster_key_exprs,
            col_stats,
            cluster_stats,
            Some(properties.cluster_key_info.cluster_key_id()),
        );
        ClusterStatistics::new(properties.cluster_key_info.cluster_key_id(), min, max, 0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ReclusterGroup {
    /// A single level forms its own group.
    Level(i32),
    /// Aggressive mode: a fixed maturity bin identified by its lower bound `lo`.
    Range(i32),
}

impl ReclusterGroup {
    /// Assign a block's recluster group for the given mode.
    pub(crate) fn assign(level: i32, mode: ReclusterMode) -> ReclusterGroup {
        match mode {
            ReclusterMode::Conservative => ReclusterGroup::Level(level),
            ReclusterMode::Aggressive if level == 0 => ReclusterGroup::Level(level),
            ReclusterMode::Aggressive => {
                debug_assert!(level > 0);
                let lo = match level {
                    1..=3 => 1,
                    4..=8 => 4,
                    _ => 9,
                };
                ReclusterGroup::Range(lo)
            }
        }
    }

    fn output_level(self, task_indices: &[usize], blocks: &[&ReclusterBlock]) -> i32 {
        match self {
            ReclusterGroup::Level(level) => level,
            ReclusterGroup::Range(lo) => {
                let mut counts: BTreeMap<i32, usize> = BTreeMap::new();
                for &idx in task_indices {
                    let level = blocks[idx].stats().level;
                    *counts.entry(level).or_default() += 1;
                }
                let mut best = (lo, 0usize);
                for (level, count) in counts {
                    if count > best.1 {
                        best = (level, count);
                    }
                }
                best.0
            }
        }
    }
}

impl fmt::Display for ReclusterGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReclusterGroup::Level(level) => write!(f, "{}", level),
            ReclusterGroup::Range(1) => write!(f, "1-3"),
            ReclusterGroup::Range(4) => write!(f, "4-8"),
            ReclusterGroup::Range(9) => write!(f, "9+"),
            ReclusterGroup::Range(lo) => unreachable!("unexpected FINAL bin lower bound: {lo}"),
        }
    }
}

/// Ranking score for a recluster task candidate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CandidateScore {
    pub selected_total_bytes: usize,
    pub selected_block_count: usize,
    pub max_depth: usize,
    pub average_depth: f64,
    pub estimated_depth_gain: u64,
}

impl CandidateScore {
    // A recluster task has a fixed scheduling cost even if it rewrites little
    // data: it consumes one distributed task slot, builds one pipeline, and
    // advances a whole candidate window. Model that fixed cost as 8 MiB so FINAL
    // prefers candidates that remove enough overlap depth per occupied slot,
    // instead of spending late rounds on many tiny low-gain cleanups.
    const TASK_SLOT_COST_BYTES: usize = 8 * 1024 * 1024;
    // Aggressive FINAL tail cutoff, expressed as adjusted bytes per removed
    // overlap-depth point. 32 KiB means a large-table cleanup task should remove
    // at least roughly one depth point per 32 KiB of input plus fixed slot cost.
    // Tiny candidates are exempted below because the fixed 8 MiB cost would
    // otherwise dominate ordinary small-table FINAL recluster behavior.
    const MAX_TASK_COST_ADJUSTED_BYTES_PER_DEPTH_GAIN: f64 = 32.0 * 1024.0;

    pub fn bytes_per_depth_gain(&self) -> f64 {
        if self.estimated_depth_gain == 0 {
            f64::INFINITY
        } else {
            self.selected_total_bytes as f64 / self.estimated_depth_gain as f64
        }
    }

    pub fn task_cost_adjusted_bytes_per_depth_gain(&self) -> f64 {
        if self.estimated_depth_gain == 0 {
            return f64::INFINITY;
        }

        let fragmentation_ratio =
            self.selected_block_count.max(1) as f64 / self.max_depth.max(1) as f64;
        let task_cost = self
            .selected_total_bytes
            .saturating_add(Self::TASK_SLOT_COST_BYTES);
        task_cost as f64 / self.estimated_depth_gain as f64 * fragmentation_ratio.sqrt()
    }

    pub fn passes_tail_efficiency_threshold(&self) -> bool {
        // The tail threshold is meant to stop large-table FINAL recluster from
        // spending distributed task slots on low-gain cleanup. Do not apply it
        // to tiny candidates: for small tables the fixed slot cost dominates
        // the score and would make FINAL skip the only valid rewrite.
        if self.selected_total_bytes < Self::TASK_SLOT_COST_BYTES {
            return true;
        }

        self.task_cost_adjusted_bytes_per_depth_gain()
            <= Self::MAX_TASK_COST_ADJUSTED_BYTES_PER_DEPTH_GAIN
    }

    /// Compare scores in descending priority order.
    pub fn cmp_desc(&self, other: &Self) -> cmp::Ordering {
        other
            .task_cost_adjusted_bytes_per_depth_gain()
            .partial_cmp(&self.task_cost_adjusted_bytes_per_depth_gain())
            .unwrap_or(cmp::Ordering::Equal)
            .then_with(|| {
                other
                    .bytes_per_depth_gain()
                    .partial_cmp(&self.bytes_per_depth_gain())
                    .unwrap_or(cmp::Ordering::Equal)
            })
            .then_with(|| self.estimated_depth_gain.cmp(&other.estimated_depth_gain))
            .then_with(|| other.selected_total_bytes.cmp(&self.selected_total_bytes))
    }
}

/// In-memory rewrite candidate produced from one probed window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclusterTaskKind {
    Recluster,
    PhysicalCompaction,
    RepackOnly,
}

#[derive(Clone)]
pub(crate) struct ReclusterTaskCandidate {
    pub(crate) score: CandidateScore,
    pub(crate) selected_blocks: Vec<(usize, Vec<usize>)>,
    pub(crate) output_level: i32,
    pub(crate) all_ordered: bool,
    pub(crate) kind: ReclusterTaskKind,
}

impl ReclusterTaskCandidate {
    pub(crate) fn selected_block_count(&self) -> usize {
        self.selected_blocks
            .iter()
            .map(|(_, block_indices)| block_indices.len())
            .sum()
    }

    /// Whether this candidate only repacks unchanged blocks into fewer segments.
    pub(crate) fn is_repack_only(&self) -> bool {
        self.kind == ReclusterTaskKind::RepackOnly
    }

    pub(crate) fn is_physical_compaction(&self) -> bool {
        self.kind == ReclusterTaskKind::PhysicalCompaction
    }

    pub(crate) fn passes_aggressive_tail_filter(&self) -> bool {
        self.is_repack_only()
            || self.is_physical_compaction()
            || self.score.passes_tail_efficiency_threshold()
    }
}

impl fmt::Display for ReclusterTaskCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "kind={:?} output_level={} max_depth={} avg_depth={} selected_count={} bytes={} estimated_depth_gain={} bytes_per_depth_gain={} task_cost_adjusted_bytes_per_depth_gain={}",
            self.kind,
            self.output_level,
            self.score.max_depth,
            self.score.average_depth,
            self.selected_block_count(),
            self.score.selected_total_bytes,
            self.score.estimated_depth_gain,
            self.score.bytes_per_depth_gain(),
            self.score.task_cost_adjusted_bytes_per_depth_gain(),
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
    kind: ReclusterTaskKind,
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

    let output_level = group.output_level(task_indices, blocks);
    let all_ordered = task_indices
        .iter()
        .all(|idx| matches!(&blocks[*idx].stats, ReclusterBlockStats::Original));
    ReclusterTaskCandidate {
        score,
        selected_blocks,
        output_level,
        all_ordered,
        kind,
    }
}

pub(crate) fn passes_depth_gate(
    depth_threshold: f64,
    average_depth: f64,
    max_depth: usize,
) -> bool {
    let mature_gate = if depth_threshold <= MIN_RECLUSTER_DEPTH as f64 {
        depth_threshold
    } else {
        (2.0 * depth_threshold).min(MAX_RECLUSTER_DEPTH as f64)
    };
    average_depth > depth_threshold || max_depth as f64 > mature_gate
}
