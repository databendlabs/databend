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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::ColumnId;
use databend_common_expression::local_block_meta_serde;
use databend_common_statistics::KllSketch;
use databend_common_storage::MetaHLL;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::merge_column_count_min_sketch_mut;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;
use databend_storages_common_table_meta::meta::merge_column_top_n_mut;

use crate::statistics::reduce_block_statistics;
use crate::statistics::reduce_cluster_statistics;
use crate::statistics::reducers::reduce_virtual_column_statistics;

/// Segment summaries reduced into the shape stored in the snapshot summary.
#[derive(Clone, Debug, Default)]
pub struct ReducedSegmentStats {
    pub col_stats: StatisticsOfColumns,
    pub virtual_col_stats: Option<StatisticsOfColumns>,
    pub cluster_stats: Option<ClusterStatistics>,
    /// Number of segments folded in. An empty accumulator must be replaced rather than
    /// reduced, because reducing `None` virtual or cluster statistics with anything yields
    /// `None`.
    segment_count: usize,
}

impl ReducedSegmentStats {
    pub fn fold(&mut self, summary: &Statistics, cluster_key_info: Option<&ClusterKeyInfo>) {
        let next = Self {
            col_stats: summary.col_stats.clone(),
            virtual_col_stats: summary.virtual_col_stats.clone(),
            cluster_stats: summary.cluster_stats.clone(),
            segment_count: 1,
        };
        self.merge(next, cluster_key_info);
    }

    pub fn merge(&mut self, other: Self, cluster_key_info: Option<&ClusterKeyInfo>) {
        if other.segment_count == 0 {
            return;
        }
        if self.segment_count == 0 {
            *self = other;
            return;
        }

        self.col_stats = reduce_block_statistics(&[&self.col_stats, &other.col_stats]);
        self.virtual_col_stats =
            reduce_virtual_column_statistics(&[&self.virtual_col_stats, &other.virtual_col_stats]);
        self.cluster_stats = reduce_cluster_statistics(
            &[&self.cluster_stats, &other.cluster_stats],
            cluster_key_info,
        );
        self.segment_count += other.segment_count;
    }
}

/// Everything ANALYZE accumulates over a set of segments.
///
/// Each collect source owns one instance and ships it to the sink as block meta; the sink
/// merges them and keeps accumulating when it rebases onto segments appended concurrently.
#[derive(Clone, Default)]
pub struct AnalyzeAccumulator {
    /// Rows covered by `column_hlls`.
    pub row_count: u64,
    /// Rows whose blocks carry no HLL and were not scanned.
    pub unstats_rows: u64,
    pub column_hlls: HashMap<ColumnId, MetaHLL>,
    /// Empty unless Top-N collection is enabled.
    pub top_n: BlockTopN,
    /// Empty unless count-min sketch collection is enabled.
    pub count_min_sketch: BlockCountMinSketch,
    /// Columns whose Top-N was dropped because their cardinality was too high.
    pub dropped_top_n_columns: BTreeSet<ColumnId>,
    pub kll_histograms: HashMap<ColumnId, KllSketch>,
    pub segment_stats: ReducedSegmentStats,
}

impl AnalyzeAccumulator {
    pub fn merge(&mut self, other: Self, cluster_key_info: Option<&ClusterKeyInfo>) -> Result<()> {
        merge_column_hll_mut(&mut self.column_hlls, &other.column_hlls);

        self.dropped_top_n_columns
            .extend(other.dropped_top_n_columns);
        let dropped = &self.dropped_top_n_columns;
        self.top_n
            .retain(|column_id, _| !dropped.contains(column_id));
        let mut other_top_n = other.top_n;
        other_top_n.retain(|column_id, _| !dropped.contains(column_id));
        merge_column_top_n_mut(&mut self.top_n, other_top_n)?;
        merge_column_count_min_sketch_mut(&mut self.count_min_sketch, other.count_min_sketch);
        for (column_id, sketch) in other.kll_histograms {
            match self.kll_histograms.get_mut(&column_id) {
                Some(existing) => existing.merge(sketch)?,
                None => {
                    self.kll_histograms.insert(column_id, sketch);
                }
            }
        }

        self.segment_stats
            .merge(other.segment_stats, cluster_key_info);
        self.row_count += other.row_count;
        self.unstats_rows += other.unstats_rows;
        Ok(())
    }
}

impl Debug for AnalyzeAccumulator {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("AnalyzeAccumulator").finish()
    }
}

local_block_meta_serde!(AnalyzeAccumulator);

#[typetag::serde(name = "analyze_accumulator")]
impl BlockMetaInfo for AnalyzeAccumulator {}
