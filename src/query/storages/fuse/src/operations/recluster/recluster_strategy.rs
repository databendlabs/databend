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
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use databend_common_catalog::plan::ClusterLevelLogStats;
use databend_common_catalog::plan::ReclusterTaskKind;
use databend_common_catalog::plan::VerticalReclusterKind;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ComputedExpr;
use databend_common_expression::Expr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_settings::Settings;
use databend_common_sql::ClusterKeys;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::table::ClusterType;

use crate::FUSE_OPT_KEY_ENABLE_RECLUSTER_BLOCK_REDUCTION;
use crate::FuseStorageFormat;
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
    pub(crate) partition_key_id: Option<u32>,
    pub(crate) memory_threshold: usize,
    /// Mirrors `ReclusterStrategy::supports_ordered_merge` so candidate builders can classify
    /// tasks without holding the strategy.
    pub(crate) supports_ordered_merge: bool,
    /// Split unordered linear inputs before overlap-based merge selection.
    /// Materialized views retain their re-aggregation selection semantics.
    pub(crate) split_sort_tasks: bool,
    pub(crate) prepared_cluster_key_exprs: Vec<PreparedClusterKeyExpr>,
    pub(crate) scalar_cluster_key_types: Vec<DataType>,
    pub(crate) vertical_kind: Option<VerticalReclusterKind>,
    pub(crate) enable_block_reduction: bool,
    /// Blocks of the final full pass of a vector RECLUSTER FINAL, which are
    /// rewritten once regardless of level; see `ReclusterFinalCarry`.
    pub(crate) vector_full_pass: Option<Arc<HashSet<String>>>,
    /// Splits linear merge tasks into merge groups; disabled unless set from the settings.
    pub(crate) merge_group_limits: MergeGroupLimits,
}

/// Limits of the merge groups a linear merge task is split into.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MergeGroupLimits {
    /// Most input blocks of one group; 0 does not split tasks.
    pub(crate) max_blocks: usize,
    /// Most ratio of the largest to the smallest block of one group; 0 does not bound it.
    pub(crate) max_size_ratio: u64,
}

impl MergeGroupLimits {
    /// The limits for `table`, which are off when the executor would not run its merge tasks
    /// with the multiway merge; the other paths merge a task whole.
    pub(crate) fn for_table(table: &FuseTable, settings: &Settings) -> Result<Self> {
        let multiway_merge = matches!(table.get_storage_format(), FuseStorageFormat::Parquet)
            && !table
                .schema()
                .fields()
                .iter()
                .any(|field| matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_))));
        match multiway_merge {
            true => Self::from_settings(settings),
            false => Ok(Self::default()),
        }
    }

    pub(crate) fn from_settings(settings: &Settings) -> Result<Self> {
        if !settings.get_enable_recluster_multiway_merge()? {
            // Only the multiway merge runs groups one after another.
            return Ok(Self::default());
        }
        let max_blocks = settings.get_recluster_max_group_blocks()?;
        if max_blocks == 1 {
            return Err(ErrorCode::BadArguments(
                "recluster_max_group_blocks must be 0 or at least 2",
            ));
        }
        Ok(Self {
            max_blocks: max_blocks as usize,
            max_size_ratio: settings.get_recluster_group_max_size_ratio()?,
        })
    }
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
        vertical_kind: Option<VerticalReclusterKind>,
    ) -> Result<(Self, Arc<dyn ReclusterStrategy>)> {
        if vertical_kind.is_some() && !matches!(&cluster_keys, ClusterKeys::Linear(_)) {
            return Err(ErrorCode::Unimplemented(
                "vertical recluster supports linear scalar cluster keys only",
            ));
        }
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
        let enable_block_reduction =
            table.get_option(FUSE_OPT_KEY_ENABLE_RECLUSTER_BLOCK_REDUCTION, 0u32) != 0;
        let properties = Self {
            mode,
            depth_threshold,
            block_thresholds,
            supports_ordered_merge: strategy.supports_ordered_merge(),
            split_sort_tasks: strategy.supports_ordered_merge()
                && !is_materialized_view_engine(table.engine()),
            cluster_key_info,
            partition_key_count: table.partition_key_count(),
            partition_key_id: table.partition_key_id(),
            memory_threshold,
            prepared_cluster_key_exprs,
            scalar_cluster_key_types,
            vertical_kind,
            enable_block_reduction,
            vector_full_pass: None,
            merge_group_limits: MergeGroupLimits::default(),
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
            supports_ordered_merge: strategy.supports_ordered_merge(),
            split_sort_tasks: strategy.supports_ordered_merge(),
            cluster_key_info,
            partition_key_count,
            partition_key_id: (partition_key_count != 0).then_some(0),
            memory_threshold,
            prepared_cluster_key_exprs,
            scalar_cluster_key_types,
            vertical_kind: None,
            enable_block_reduction: false,
            vector_full_pass: None,
            merge_group_limits: MergeGroupLimits::default(),
        };
        (properties, strategy)
    }
}

/// Algorithm-specific behavior used by the recluster workflow.
pub(crate) trait ReclusterStrategy: Send + Sync {
    /// Only scalar linear clustering preserves source row order during merging.
    /// The persisted ClusterType::Linear also covers vector clustering and is
    /// not sufficient to select the merge-only execution path.
    fn supports_ordered_merge(&self) -> bool {
        false
    }

    /// Select windows from a partition-local segment slice. ReclusterMutator performs partition
    /// grouping and filters segments without exact partition metadata before calling strategies.
    fn select_segments(
        &self,
        properties: &ReclusterProperties,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>>;

    /// The blocks of a group that may be rewritten, before any selection, including
    /// small-block compaction. All by default.
    fn eligible_blocks(
        &self,
        _properties: &ReclusterProperties,
        indices: Vec<usize>,
        _blocks: &[&ReclusterBlock],
    ) -> Vec<usize> {
        indices
    }

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

/// The highest level of the low mature levels Aggressive mode groups together.
pub(crate) const LOW_MATURITY_MAX_LEVEL: i32 = 3;

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
            ReclusterMode::Aggressive if (1..=LOW_MATURITY_MAX_LEVEL).contains(&level) => {
                ReclusterGroup::LowMaturity
            }
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

/// The selector class that produced a recluster task candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReclusterCandidateKind {
    Repack,
    BlockReduction,
    Depth,
}

impl fmt::Display for ReclusterCandidateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repack => write!(f, "repack"),
            Self::BlockReduction => write!(f, "block_reduction"),
            Self::Depth => write!(f, "depth"),
        }
    }
}

/// In-memory rewrite candidate produced from one probed window.
#[derive(Clone)]
pub(crate) struct ReclusterTaskCandidate {
    pub(crate) score: CandidateScore,
    pub(crate) kind: ReclusterCandidateKind,
    // Empty means a rebuild-only repack candidate.
    pub(crate) selected_blocks: Vec<(usize, Vec<usize>)>,
    pub(crate) base_level: i32,
    pub(crate) input_level_stats: Vec<ClusterLevelLogStats>,
    /// Row-sort stage work for the horizontal pipeline; `MergeBlocks` implies every input is
    /// already ordered by the current linear cluster key.
    pub(crate) task_kind: ReclusterTaskKind,
    pub(crate) vertical_kind: Option<VerticalReclusterKind>,
    /// Merge groups of a split linear merge task, in order, as the `(window position, block
    /// index)` of their blocks. Empty means one group.
    pub(crate) merge_groups: Vec<Vec<(usize, usize)>>,
    /// Selected blocks left out of the task because no merge group admitted another block.
    pub(crate) skipped_blocks: usize,
    /// Left-out blocks whose level is raised without rewriting them, as `(window position,
    /// block index)`.
    pub(crate) promoted_blocks: Vec<(usize, usize)>,
}

impl ReclusterTaskCandidate {
    /// Whether this candidate only repacks unchanged blocks into fewer segments.
    pub(crate) fn is_repack_only(&self) -> bool {
        self.selected_blocks.is_empty()
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
}

impl fmt::Display for ReclusterTaskCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "requested_output_level={} repack_only={} candidate_kind={} task_kind={:?} executor_kind={:?} max_depth={} avg_depth={} block_count={} block_size={}",
            self.requested_output_level(),
            self.is_repack_only(),
            self.kind,
            self.task_kind,
            self.vertical_kind,
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
    properties: &ReclusterProperties,
    kind: ReclusterCandidateKind,
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
    let task_kind = match properties.supports_ordered_merge
        && task_indices
            .iter()
            .all(|idx| matches!(&blocks[*idx].stats, ReclusterBlockStats::Original))
    {
        true => ReclusterTaskKind::MergeBlocks,
        false => ReclusterTaskKind::SortBlocks,
    };
    ReclusterTaskCandidate {
        score,
        kind,
        selected_blocks,
        base_level,
        input_level_stats: stats_by_level.into_values().collect(),
        task_kind,
        vertical_kind: properties.vertical_kind,
        merge_groups: Vec::new(),
        skipped_blocks: 0,
        promoted_blocks: Vec::new(),
    }
}

/// A depth candidate for the blocks selected as one task. A linear merge task is split into
/// merge groups, and a block left alone in its group is left out of the task, as a merge of
/// one block would only rewrite it. None when no group remains.
///
/// A left-out block larger than every grouped block is promoted one level, up to
/// `LOW_MATURITY_MAX_LEVEL`. At its level it would wait for blocks of its own size; at the next
/// level it meets the outputs of these groups, which grow towards it.
pub(crate) fn depth_task_candidate(
    properties: &ReclusterProperties,
    group: ReclusterGroup,
    score: CandidateScore,
    task_indices: &[usize],
    blocks: &[&ReclusterBlock],
) -> Option<ReclusterTaskCandidate> {
    let candidate = task_candidate(
        properties,
        ReclusterCandidateKind::Depth,
        group,
        score,
        task_indices,
        blocks,
    );
    let limits = properties.merge_group_limits;
    if limits.max_blocks == 0
        || task_indices.len() < 2
        || candidate.task_kind != ReclusterTaskKind::MergeBlocks
        || candidate.vertical_kind.is_some()
    {
        return Some(candidate);
    }
    let sizes = task_indices
        .iter()
        .map(|idx| blocks[*idx].meta.block_size)
        .collect::<Vec<_>>();
    let split = split_merge_groups(&sizes, limits);
    if split.groups.is_empty() {
        return None;
    }
    if split.groups.len() == 1 && split.singles.is_empty() {
        return Some(candidate);
    }
    let kept = split
        .groups
        .iter()
        .flatten()
        .map(|position| task_indices[*position])
        .collect::<Vec<_>>();
    let score = CandidateScore {
        selected_total_bytes: kept
            .iter()
            .map(|idx| blocks[*idx].meta.block_size as usize)
            .sum(),
        max_depth: score.max_depth.min(kept.len()),
        average_depth: score.average_depth,
    };
    let mut candidate = task_candidate(
        properties,
        ReclusterCandidateKind::Depth,
        group,
        score,
        &kept,
        blocks,
    );
    if split.groups.len() > 1 {
        candidate.merge_groups = split
            .groups
            .iter()
            .map(|members| {
                members
                    .iter()
                    .map(|position| {
                        let index = &blocks[task_indices[*position]].index;
                        (index.segment_idx, index.block_idx)
                    })
                    .collect()
            })
            .collect();
    }
    candidate.skipped_blocks = split.singles.len();
    let largest_grouped = kept
        .iter()
        .map(|idx| blocks[*idx].meta.block_size)
        .max()
        .unwrap_or(0);
    candidate.promoted_blocks = split
        .singles
        .iter()
        .map(|position| blocks[task_indices[*position]])
        .filter(|block| {
            block.meta.block_size > largest_grouped
                && (0..LOW_MATURITY_MAX_LEVEL).contains(&block.stats().level)
        })
        .map(|block| (block.index.segment_idx, block.index.block_idx))
        .collect();
    Some(candidate)
}

/// Merge groups of the blocks of one task, as positions in its block list.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MergeGroupSplit {
    /// Groups of at least two blocks, in the order they were opened.
    pub(crate) groups: Vec<Vec<usize>>,
    /// Blocks left alone in their group, in block order.
    pub(crate) singles: Vec<usize>,
}

/// Splits the blocks of a task, given by their uncompressed sizes in task order, into merge
/// groups by first fit. A block joins the first group that is not full and keeps the ratio of
/// its largest to its smallest block within `max_size_ratio`; otherwise it opens a group.
///
/// When every block is left alone, the blocks are split by the block limit only. The task then
/// still merges its blocks, rather than leaving the same selection unmerged in every round.
/// Without a block limit all blocks form one group.
pub(crate) fn split_merge_groups(sizes: &[u64], limits: MergeGroupLimits) -> MergeGroupSplit {
    if limits.max_blocks == 0 {
        return MergeGroupSplit {
            groups: vec![(0..sizes.len()).collect()],
            singles: Vec::new(),
        };
    }
    let split = first_fit_merge_groups(sizes, limits.max_blocks, limits.max_size_ratio);
    if split.groups.is_empty() && limits.max_size_ratio != 0 {
        return first_fit_merge_groups(sizes, limits.max_blocks, 0);
    }
    split
}

fn first_fit_merge_groups(
    sizes: &[u64],
    max_blocks: usize,
    max_size_ratio: u64,
) -> MergeGroupSplit {
    struct OpenGroup {
        members: Vec<usize>,
        smallest: u64,
        largest: u64,
    }
    let mut open: Vec<OpenGroup> = Vec::new();
    for (position, size) in sizes.iter().enumerate() {
        // An empty block counts as one byte, which keeps its ratio to other blocks finite.
        let size = (*size).max(1);
        let admits = |group: &&mut OpenGroup| {
            group.members.len() < max_blocks
                && (max_size_ratio == 0
                    || group.largest.max(size)
                        <= group.smallest.min(size).saturating_mul(max_size_ratio))
        };
        match open.iter_mut().find(admits) {
            Some(group) => {
                group.members.push(position);
                group.smallest = group.smallest.min(size);
                group.largest = group.largest.max(size);
            }
            None => open.push(OpenGroup {
                members: vec![position],
                smallest: size,
                largest: size,
            }),
        }
    }
    let (groups, singles): (Vec<_>, Vec<_>) = open
        .into_iter()
        .map(|group| group.members)
        .partition(|members| members.len() >= 2);
    // A group of one is opened by its block, so the singles are already in block order.
    MergeGroupSplit {
        groups,
        singles: singles.into_iter().flatten().collect(),
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
    use super::MergeGroupLimits;
    use super::ReclusterGroup;
    use super::ReclusterMode;
    use super::split_merge_groups;

    #[test]
    fn test_aggressive_groups_only_low_maturity_blocks() {
        for level in 1..=3 {
            assert_eq!(
                ReclusterGroup::assign(level, ReclusterMode::Aggressive),
                ReclusterGroup::LowMaturity
            );
        }
        for level in [0, 4, 8, 9, 15] {
            assert_eq!(
                ReclusterGroup::assign(level, ReclusterMode::Aggressive),
                ReclusterGroup::Level(level)
            );
        }
    }

    #[test]
    fn test_conservative_keeps_exact_level_groups() {
        assert_eq!(
            ReclusterGroup::assign(0, ReclusterMode::Conservative),
            ReclusterGroup::Level(0)
        );
        assert_eq!(
            ReclusterGroup::assign(1, ReclusterMode::Conservative),
            ReclusterGroup::Level(1)
        );
    }

    fn limits(max_blocks: usize, max_size_ratio: u64) -> MergeGroupLimits {
        MergeGroupLimits {
            max_blocks,
            max_size_ratio,
        }
    }

    #[test]
    fn test_merge_groups_skip_a_large_block_between_small_ones() {
        const MB: u64 = 1 << 20;
        // 50 blocks of 20 MiB, one of 2 GiB, then 50 more of 20 MiB.
        let mut sizes = vec![20 * MB; 50];
        sizes.push(2048 * MB);
        sizes.extend(vec![20 * MB; 50]);
        let split = split_merge_groups(&sizes, limits(64, 5));
        // The first group fills up with blocks from both sides of the large block.
        let first = (0..50).chain(51..65).collect::<Vec<_>>();
        assert_eq!(split.groups, vec![first, (65..101).collect()]);
        assert_eq!(split.singles, vec![50]);
    }

    #[test]
    fn test_merge_groups_close_when_full() {
        let split = split_merge_groups(&[10; 7], limits(3, 5));
        assert_eq!(split.groups, vec![vec![0, 1, 2], vec![3, 4, 5]]);
        // The last block opens a group alone.
        assert_eq!(split.singles, vec![6]);
    }

    #[test]
    fn test_merge_groups_admit_by_size_ratio() {
        // 5 fits with 1 at ratio 5; 6 does not, and opens a group 30 joins.
        let split = split_merge_groups(&[1, 5, 6, 30, 31, 2], limits(64, 5));
        assert_eq!(split.groups, vec![vec![0, 1, 5], vec![2, 3]]);
        assert_eq!(split.singles, vec![4]);
        // Without a size ratio only the block limit applies.
        let split = split_merge_groups(&[1, 5, 6, 30, 31, 2], limits(4, 0));
        assert_eq!(split.groups, vec![vec![0, 1, 2, 3], vec![4, 5]]);
        assert!(split.singles.is_empty());
        // An empty block counts as one byte.
        let split = split_merge_groups(&[0, 5, 6], limits(64, 5));
        assert_eq!(split.groups, vec![vec![0, 1]]);
        assert_eq!(split.singles, vec![2]);
    }

    #[test]
    fn test_merge_groups_drop_the_ratio_when_every_block_is_alone() {
        // Each block is more than 5 times the previous one: no group admits a second block,
        // so the blocks are split by the block limit only.
        let split = split_merge_groups(&[1, 6, 36, 216, 1296], limits(2, 5));
        assert_eq!(split.groups, vec![vec![0, 1], vec![2, 3]]);
        assert_eq!(split.singles, vec![4]);
        // A single block stays alone.
        let split = split_merge_groups(&[7], limits(2, 5));
        assert!(split.groups.is_empty());
        assert_eq!(split.singles, vec![0]);
    }

    #[test]
    fn test_merge_groups_without_block_limit() {
        let split = split_merge_groups(&[1, 1000, 1], limits(0, 5));
        assert_eq!(split.groups, vec![vec![0, 1, 2]]);
        assert!(split.singles.is_empty());
    }
}
