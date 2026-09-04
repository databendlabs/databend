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
    /// Task byte budget this candidate was packed against. Used to express how
    /// well the candidate fills one distributed task slot.
    pub task_threshold_bytes: usize,
    /// Distinct segments the selected blocks come from. Each extra segment adds
    /// metadata read and commit work that raw rewrite bytes do not capture.
    pub touched_segment_count: usize,
    /// Depth gate this candidate was probed against. Ranking uses it to tier
    /// candidates by how far above the gate their worst hotspot sits.
    pub depth_threshold: f64,
}

impl CandidateScore {
    /// Candidates filling less than this share of a task slot are ranked behind
    /// all better-filled candidates, so they are effectively deferred until no
    /// higher-value rewrite is left.
    pub const MIN_FILL_RATIO: f64 = 0.25;
    /// Weight of fill ratio relative to raw benefit density. `0.0` reduces to
    /// pure density, `1.0` reduces to ranking by total estimated gain.
    const FILL_RATIO_EXPONENT: f64 = 0.5;
    /// Bytes charged per extra segment a task spans. A multi-segment rewrite
    /// reads and rewrites more segment metadata and enlarges the commit, which
    /// rewrite bytes alone do not express. Kept well below a typical block so
    /// it only breaks ties between otherwise comparable candidates.
    const SEGMENT_COST_BYTES: usize = 4 * 1024 * 1024;
    /// Multiple of `depth_threshold` at or above which a hotspot is treated as
    /// tail work that must be drained before shallower rewrites, regardless of
    /// how good their benefit density looks. Matches the mature gate used by
    /// `passes_depth_gate`, so tiering agrees with admission.
    const TAIL_DEPTH_GATE_FACTOR: f64 = 2.0;

    /// Rank tier for tail protection. Lower sorts first.
    ///
    /// Benefit density alone leaves the deepest hotspots behind: a mid-depth
    /// candidate that rewrites fewer bytes per removed depth keeps outranking
    /// them, so a full FINAL can end with a worse p95/p99 than v1 even while
    /// average depth improves. Tiering by how far the worst hotspot sits above
    /// the depth gate drains the tail first and only then optimizes density.
    pub fn depth_tier(&self) -> u8 {
        if self.depth_threshold <= 0.0 {
            return 1;
        }
        let tail_gate =
            (Self::TAIL_DEPTH_GATE_FACTOR * self.depth_threshold).min(MAX_RECLUSTER_DEPTH as f64);
        if (self.max_depth as f64) >= tail_gate {
            0
        } else {
            1
        }
    }

    pub fn bytes_per_depth_gain(&self) -> f64 {
        if self.estimated_depth_gain == 0 {
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

    /// Rewrite bytes plus the metadata cost of spanning multiple segments.
    /// A single-segment task is charged nothing extra.
    pub fn effective_cost_bytes(&self) -> usize {
        let extra_segments = self.touched_segment_count.saturating_sub(1);
        self.selected_total_bytes
            .saturating_add(extra_segments.saturating_mul(Self::SEGMENT_COST_BYTES))
    }

    /// Benefit density discounted by how poorly the candidate fills a task
    /// slot. Higher is better. Replaces the previous fixed slot-cost term,
    /// which did not scale with the task byte budget.
    ///
    /// Cost counts extra touched segments, so a task that rewrites the same
    /// bytes from fewer segments ranks ahead of one scattered across many.
    pub fn fill_adjusted_gain_density(&self) -> f64 {
        if self.estimated_depth_gain == 0 || self.selected_total_bytes == 0 {
            return 0.0;
        }
        let density = self.estimated_depth_gain as f64 / self.effective_cost_bytes() as f64;
        density * self.fill_ratio().powf(Self::FILL_RATIO_EXPONENT)
    }

    /// Whether this candidate is too small to spend a task slot on right now.
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

    /// Compare scores by the experimental benefit-density order.
    ///
    /// Ordering, highest priority first:
    /// 1. deeper-than-gate hotspots, so the tail drains before shallow work,
    /// 2. candidates filling at least `MIN_FILL_RATIO` of a task slot,
    /// 3. fill-adjusted gain density,
    /// 4. total estimated depth gain,
    /// 5. less fragmented rewrites,
    /// 6. larger rewrites.
    ///
    /// Tiering by depth comes first because density is a ratio: a mid-depth
    /// candidate with a good bytes-per-gain ratio otherwise keeps outranking the
    /// worst hotspots, which leaves p95/p99 worse at the end of a full FINAL
    /// even when average depth improves.
    pub fn cmp_desc_v2(&self, other: &Self) -> cmp::Ordering {
        other
            .depth_tier()
            .cmp(&self.depth_tier())
            .then_with(|| other.is_underfilled().cmp(&self.is_underfilled()))
            .then_with(|| {
                self.fill_adjusted_gain_density()
                    .partial_cmp(&other.fill_adjusted_gain_density())
                    .unwrap_or(cmp::Ordering::Equal)
            })
            .then_with(|| self.estimated_depth_gain.cmp(&other.estimated_depth_gain))
            .then_with(|| {
                other
                    .fragmentation_ratio()
                    .partial_cmp(&self.fragmentation_ratio())
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
    pub(crate) output_level: i32,
    pub(crate) all_ordered: bool,
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
        self.selected_blocks.is_empty()
    }
}

impl fmt::Display for ReclusterTaskCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "output_level={} max_depth={} avg_depth={} selected_count={} bytes={} estimated_depth_gain={} bytes_per_depth_gain={} fill_ratio={} fill_adjusted_gain_density={} underfilled={}",
            self.output_level,
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

    let output_level = group.output_level(task_indices, blocks);
    let all_ordered = task_indices
        .iter()
        .all(|idx| matches!(&blocks[*idx].stats, ReclusterBlockStats::Original));
    ReclusterTaskCandidate {
        score,
        selected_blocks,
        output_level,
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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::CandidateScore;
    use super::ReclusterMode;
    use super::enable_task_selection_v2_for_mode;

    const MIB: usize = 1024 * 1024;

    fn score(bytes: usize, threshold: usize, gain: u64, blocks: usize) -> CandidateScore {
        segment_score(bytes, threshold, gain, blocks, 1)
    }

    /// Score with depth tiering disabled, so a test exercises only the fill and
    /// density order. `depth_threshold = 0` puts every candidate in one tier.
    fn segment_score(
        bytes: usize,
        threshold: usize,
        gain: u64,
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
            depth_threshold: 0.0,
        }
    }

    /// Score carrying a real depth gate, for the tail-protection order.
    fn tiered_score(
        bytes: usize,
        threshold: usize,
        gain: u64,
        max_depth: usize,
        depth_threshold: f64,
    ) -> CandidateScore {
        CandidateScore {
            selected_total_bytes: bytes,
            selected_block_count: max_depth,
            max_depth,
            average_depth: max_depth as f64,
            estimated_depth_gain: gain,
            task_threshold_bytes: threshold,
            touched_segment_count: 1,
            depth_threshold,
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
    fn test_v2_defers_underfilled_candidates() {
        let underfilled = score(10 * MIB, 1024 * MIB, 5000, 4);
        let filled = score(900 * MIB, 1024 * MIB, 10, 100);

        assert!(underfilled.is_underfilled());
        assert!(!filled.is_underfilled());
        // Even with a far better density, an underfilled candidate waits.
        assert!(underfilled.fill_adjusted_gain_density() > filled.fill_adjusted_gain_density());
        assert_eq!(filled.cmp_desc_v2(&underfilled), Ordering::Greater);
    }

    #[test]
    fn test_v2_prefers_fewer_touched_segments() {
        // Same bytes and same removable depth, but one rewrite is scattered
        // across many segments, so it costs more metadata and commit work.
        let compact = segment_score(400 * MIB, 1024 * MIB, 500, 40, 1);
        let scattered = segment_score(400 * MIB, 1024 * MIB, 500, 40, 9);

        assert!(compact.effective_cost_bytes() < scattered.effective_cost_bytes());
        assert_eq!(compact.cmp_desc_v2(&scattered), Ordering::Greater);
    }

    #[test]
    fn test_v2_drains_deep_tail_before_better_density() {
        // The shallow candidate has strictly better density and fills the slot,
        // so without tiering it wins. The deep one sits at the mature gate and
        // is the tail work that leaves p95/p99 worse if it keeps losing.
        let deep = tiered_score(400 * MIB, 1024 * MIB, 100, 32, 16.0);
        let shallow = tiered_score(1000 * MIB, 1024 * MIB, 900, 8, 16.0);

        assert_eq!(deep.depth_tier(), 0);
        assert_eq!(shallow.depth_tier(), 1);
        assert!(shallow.fill_adjusted_gain_density() > deep.fill_adjusted_gain_density());
        assert_eq!(deep.cmp_desc_v2(&shallow), Ordering::Greater);
    }

    #[test]
    fn test_v2_ranks_by_density_inside_one_depth_tier() {
        // Tiering must not flatten the density order it wraps: two candidates on
        // the same side of the gate still compare by fill-adjusted density.
        let a = tiered_score(1000 * MIB, 1024 * MIB, 900, 32, 16.0);
        let b = tiered_score(1000 * MIB, 1024 * MIB, 300, 32, 16.0);

        assert_eq!(a.depth_tier(), b.depth_tier());
        assert_eq!(a.cmp_desc_v2(&b), Ordering::Greater);
    }

    #[test]
    fn test_depth_tier_gate_tracks_configured_threshold() {
        // The gate is relative, so a table with a lower depth setting treats a
        // shallower hotspot as tail work.
        assert_eq!(tiered_score(MIB, MIB, 1, 8, 4.0).depth_tier(), 0);
        assert_eq!(tiered_score(MIB, MIB, 1, 7, 4.0).depth_tier(), 1);
        // Capped at MAX_RECLUSTER_DEPTH so a high setting cannot disable tiering.
        assert_eq!(tiered_score(MIB, MIB, 1, 32, 64.0).depth_tier(), 0);
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
