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
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use databend_common_catalog::plan::ClusterLevelLogStats;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
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
    pub(crate) enable_task_selection_v2: bool,
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
        enable_task_selection_v2: bool,
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
            enable_task_selection_v2: enable_task_selection_v2_for_mode(
                mode,
                enable_task_selection_v2,
            ),
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
        enable_task_selection_v2: bool,
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
            enable_task_selection_v2: enable_task_selection_v2_for_mode(
                mode,
                enable_task_selection_v2,
            ),
            prepared_cluster_key_exprs,
            scalar_cluster_key_types,
        };
        (properties, strategy)
    }
}

fn enable_task_selection_v2_for_mode(mode: ReclusterMode, enabled: bool) -> bool {
    enabled && mode == ReclusterMode::Aggressive
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
        depth_stats: Option<&super::ReclusterDepthStats>,
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
    pub selected_block_count: usize,
    pub max_depth: usize,
    pub average_depth: f64,
    pub estimated_depth_gain: i64,
    /// Task byte budget this candidate was packed against. Used to express how
    /// well the candidate fills one distributed task slot.
    pub task_threshold_bytes: usize,
    /// Distinct segments the selected blocks come from. Each extra segment adds
    /// metadata read and commit work that raw rewrite bytes do not capture.
    pub touched_segment_count: usize,
}

impl CandidateScore {
    /// Fill diagnostics; v2 ranking uses total estimated gain instead.
    pub const MIN_FILL_RATIO: f64 = 0.25;
    /// Fill exponent for the diagnostic density metric, not candidate ranking.
    const FILL_RATIO_EXPONENT: f64 = 0.5;

    pub fn bytes_per_depth_gain(&self) -> f64 {
        if self.estimated_depth_gain <= 0 {
            f64::INFINITY
        } else {
            self.selected_total_bytes as f64 / self.estimated_depth_gain as f64
        }
    }

    /// How much of one task slot this candidate uses, capped at 1.
    pub fn fill_ratio(&self) -> f64 {
        if self.task_threshold_bytes == 0 {
            return 1.0;
        }
        (self.selected_total_bytes as f64 / self.task_threshold_bytes as f64).clamp(0.0, 1.0)
    }

    /// Rewrite bytes without an additional segment penalty.
    pub fn effective_cost_bytes(&self) -> usize {
        self.selected_total_bytes
    }

    /// Diagnostic benefit density; v2 ranking compares total gain directly.
    pub fn fill_adjusted_gain_density(&self) -> f64 {
        if self.estimated_depth_gain == 0 || self.selected_total_bytes == 0 {
            return 0.0;
        }
        let density = self.estimated_depth_gain as f64 / self.effective_cost_bytes() as f64;
        density * self.fill_ratio().powf(Self::FILL_RATIO_EXPONENT)
    }

    /// Diagnostic flag for candidates below the fill threshold; not a ranking gate.
    pub fn is_underfilled(&self) -> bool {
        self.fill_ratio() < Self::MIN_FILL_RATIO
    }

    /// Selected blocks per unit of removed depth. Lower means the rewrite
    /// removes overlap with fewer scattered blocks.
    pub fn fragmentation_ratio(&self) -> f64 {
        self.selected_block_count.max(1) as f64 / self.max_depth.max(1) as f64
    }

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

    /// Prefer total estimated progress; use rewrite cost only to break ties.
    pub fn cmp_desc_v2(&self, other: &Self) -> cmp::Ordering {
        self.estimated_depth_gain
            .cmp(&other.estimated_depth_gain)
            .then_with(|| other.selected_total_bytes.cmp(&self.selected_total_bytes))
            .then_with(|| {
                other
                    .fragmentation_ratio()
                    .partial_cmp(&self.fragmentation_ratio())
                    .unwrap_or(cmp::Ordering::Equal)
            })
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
    pub(crate) key_span: Option<(Vec<Scalar>, Vec<Scalar>)>,
}

impl ReclusterTaskCandidate {
    pub(crate) fn key_span_intersects(&self, other: &Self) -> bool {
        let (Some((self_min, self_max)), Some((other_min, other_max))) =
            (&self.key_span, &other.key_span)
        else {
            return true;
        };
        fn le(left: &[Scalar], right: &[Scalar]) -> bool {
            left.iter()
                .map(Scalar::as_ref)
                .cmp(right.iter().map(Scalar::as_ref))
                != cmp::Ordering::Greater
        }
        le(self_min, other_max) && le(other_min, self_max)
    }

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
            "requested_output_level={} repack_only={} max_depth={} avg_depth={} selected_count={} bytes={} estimated_depth_gain={} bytes_per_depth_gain={} fill_ratio={} fill_adjusted_gain_density={} underfilled={}",
            self.requested_output_level(),
            self.is_repack_only(),
            self.score.max_depth,
            self.score.average_depth,
            self.selected_block_count(),
            self.score.selected_total_bytes,
            self.score.estimated_depth_gain,
            self.score.bytes_per_depth_gain(),
            self.score.fill_ratio(),
            self.score.fill_adjusted_gain_density(),
            self.score.is_underfilled(),
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
    mut score: CandidateScore,
    task_indices: &[usize],
    blocks: &[&ReclusterBlock],
) -> ReclusterTaskCandidate {
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
    // Grouping above already resolved how many distinct segments the task
    // spans, so record it for scoring instead of recomputing it per candidate.
    score.touched_segment_count = selected_blocks.len();

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
    let mut key_span: Option<(Vec<Scalar>, Vec<Scalar>)> = None;
    for &idx in task_indices {
        let stats = blocks[idx].stats();
        if stats.min.is_empty() || stats.max.is_empty() {
            key_span = None;
            break;
        }
        match &mut key_span {
            None => key_span = Some((stats.min.clone(), stats.max.clone())),
            Some((span_min, span_max)) => {
                let lt = |left: &[Scalar], right: &[Scalar]| {
                    left.iter()
                        .map(Scalar::as_ref)
                        .cmp(right.iter().map(Scalar::as_ref))
                        == cmp::Ordering::Less
                };
                if lt(&stats.min, span_min) {
                    *span_min = stats.min.clone();
                }
                if lt(span_max, &stats.max) {
                    *span_max = stats.max.clone();
                }
            }
        }
    }
    ReclusterTaskCandidate {
        score,
        selected_blocks,
        base_level,
        input_level_stats: stats_by_level.into_values().collect(),
        all_ordered,
        key_span,
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::CandidateScore;
    use super::ReclusterMode;
    use super::ReclusterTaskCandidate;
    use super::Scalar;
    use super::enable_task_selection_v2_for_mode;

    const MIB: usize = 1024 * 1024;

    fn score(bytes: usize, threshold: usize, gain: i64, blocks: usize) -> CandidateScore {
        segment_score(bytes, threshold, gain, blocks, 1)
    }

    fn segment_score(
        bytes: usize,
        threshold: usize,
        gain: i64,
        blocks: usize,
        segments: usize,
    ) -> CandidateScore {
        CandidateScore {
            selected_total_bytes: bytes,
            selected_block_count: blocks,
            max_depth: blocks,
            average_depth: blocks as f64,
            estimated_depth_gain: gain,
            task_threshold_bytes: threshold,
            touched_segment_count: segments,
        }
    }

    fn tiered_score(bytes: usize, threshold: usize, gain: i64, max_depth: usize) -> CandidateScore {
        CandidateScore {
            selected_total_bytes: bytes,
            selected_block_count: max_depth,
            max_depth,
            average_depth: max_depth as f64,
            estimated_depth_gain: gain,
            task_threshold_bytes: threshold,
            touched_segment_count: 1,
        }
    }

    #[test]
    fn test_v2_prefers_higher_total_gain_over_thin_density() {
        // Approximate the task sizes observed on a 1GiB task budget: the small
        // candidate has better raw density but only a fifth of the total gain.
        let small = score(184 * MIB, 1024 * MIB, 200, 21);
        let full = score(1020 * MIB, 1024 * MIB, 1000, 113);

        assert!(small.bytes_per_depth_gain() < full.bytes_per_depth_gain());
        assert_eq!(full.cmp_desc_v2(&small), Ordering::Greater);
    }

    #[test]
    fn test_v2_ranking_is_invariant_to_task_budget_scale() {
        // The same relative sizes must rank the same way whether the task
        // budget is 100MiB or 1GiB. A fixed byte slot cost could not do this.
        for threshold in [100 * MIB, 1024 * MIB] {
            let small = score(threshold * 18 / 100, threshold, 200, 21);
            let full = score(threshold * 99 / 100, threshold, 1000, 113);
            assert_eq!(
                full.cmp_desc_v2(&small),
                Ordering::Greater,
                "threshold={threshold}"
            );
        }
    }

    #[test]
    fn test_v2_gain_precedes_fill_ratio() {
        let underfilled = score(10 * MIB, 1024 * MIB, 5000, 4);
        let filled = score(900 * MIB, 1024 * MIB, 10, 100);

        assert!(underfilled.is_underfilled());
        assert!(!filled.is_underfilled());
        assert!(underfilled.fill_adjusted_gain_density() > filled.fill_adjusted_gain_density());
        assert_eq!(underfilled.cmp_desc_v2(&filled), Ordering::Greater);
    }

    #[test]
    fn test_v2_gain_precedes_density() {
        let cheap = score(100 * MIB, 1024 * MIB, 400, 20);
        let productive = score(900 * MIB, 1024 * MIB, 500, 20);
        assert!(cheap.fill_adjusted_gain_density() > productive.fill_adjusted_gain_density());
        assert_eq!(productive.cmp_desc_v2(&cheap), Ordering::Greater);
    }

    #[test]
    fn test_v2_uses_cost_for_equal_gain() {
        let cheap = score(100 * MIB, 1024 * MIB, 500, 20);
        let expensive = score(900 * MIB, 1024 * MIB, 500, 20);
        assert_eq!(cheap.cmp_desc_v2(&expensive), Ordering::Greater);
        assert_eq!(cheap.cmp_desc_v2(&cheap), Ordering::Equal);
    }

    #[test]
    fn test_v2_segment_cost_is_disabled() {
        let compact = segment_score(400 * MIB, 1024 * MIB, 500, 40, 1);
        let scattered = segment_score(400 * MIB, 1024 * MIB, 500, 40, 9);
        assert_eq!(
            compact.effective_cost_bytes(),
            scattered.effective_cost_bytes()
        );
        assert_eq!(compact.cmp_desc_v2(&scattered), Ordering::Equal);
    }

    fn spanned_candidate(min: i32, max: i32) -> ReclusterTaskCandidate {
        ReclusterTaskCandidate {
            score: tiered_score(MIB, 4 * MIB, 10, 8),
            selected_blocks: vec![(0, vec![0])],
            base_level: 0,
            input_level_stats: Vec::new(),
            all_ordered: false,
            key_span: Some((vec![Scalar::from(min)], vec![Scalar::from(max)])),
        }
    }

    #[test]
    fn test_key_span_intersection_detects_overlapping_rewrites() {
        let left = spanned_candidate(0, 100);
        assert!(left.key_span_intersects(&spanned_candidate(50, 150)));
        assert!(left.key_span_intersects(&spanned_candidate(100, 200)));
        assert!(!left.key_span_intersects(&spanned_candidate(101, 200)));
        assert!(!left.key_span_intersects(&spanned_candidate(-100, -1)));
    }

    #[test]
    fn test_unknown_key_span_is_treated_as_overlapping() {
        let mut unknown = spanned_candidate(0, 10);
        unknown.key_span = None;
        assert!(unknown.key_span_intersects(&spanned_candidate(100, 200)));
        assert!(spanned_candidate(100, 200).key_span_intersects(&unknown));
    }

    #[test]
    fn test_task_selection_v2_only_applies_to_aggressive_mode() {
        assert!(enable_task_selection_v2_for_mode(
            ReclusterMode::Aggressive,
            true
        ));
        assert!(!enable_task_selection_v2_for_mode(
            ReclusterMode::Aggressive,
            false
        ));
        assert!(!enable_task_selection_v2_for_mode(
            ReclusterMode::Conservative,
            true
        ));
        assert!(!enable_task_selection_v2_for_mode(
            ReclusterMode::Conservative,
            false
        ));
    }
}
