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

pub mod accumulator;
mod cluster_statistics;
mod column_statistic;
mod hilbert;
pub mod reducers;
mod spatial_stats;
mod table_statistics;
mod traverse;

pub use accumulator::ColumnHLLAccumulator;
pub use accumulator::RowOrientedSegmentBuilder;
pub use accumulator::VirtualColumnAccumulator;
pub(crate) use cluster_statistics::BlockOverlapDepth;
pub use cluster_statistics::ClusterStatsGenerator;
pub use cluster_statistics::ClusterStatsKey;
pub use cluster_statistics::ClusterStatsLayout;
pub use cluster_statistics::ClusterStatsState;
pub(crate) use cluster_statistics::PreparedClusterKeyExpr;
pub(crate) use cluster_statistics::RangeMaxTree;
pub use cluster_statistics::VectorClusterInfo;
pub use cluster_statistics::VectorClusterOperator;
pub(crate) use cluster_statistics::calculate_block_overlap_depths;
pub(crate) use cluster_statistics::cluster_key_types_for_depth;
pub(crate) use cluster_statistics::cluster_stats_from_col_stats;
pub(crate) use cluster_statistics::get_min_max_stats;
pub(crate) use cluster_statistics::hilbert_bounds_for_diagnostics;
pub(crate) use cluster_statistics::partition_values;
pub(crate) use cluster_statistics::prepare_cluster_key_exprs;
pub(crate) use cluster_statistics::same_partition;
pub use cluster_statistics::sort_by_cluster_stats;
pub use cluster_statistics::vector_cluster_info_from_column;
pub use column_statistic::END_OF_UNICODE_RANGE;
pub use column_statistic::STATS_STRING_PREFIX_LEN;
pub use column_statistic::Trim;
pub use column_statistic::calc_column_distinct_of_values;
pub use column_statistic::gen_columns_statistics;
pub use column_statistic::scalar_min_max;
pub(crate) use column_statistic::trim_column_min_max;
pub use column_statistic::trim_string_max_with_len;
pub use column_statistic::trim_string_min_with_len;
pub(crate) use hilbert::hilbert_diagnostics;
pub use reducers::merge_statistics;
pub use reducers::reduce_block_metas;
pub use reducers::reduce_block_statistics;
pub use reducers::reduce_cluster_statistics;
pub use spatial_stats::SpatialStatsBuilder;
pub use table_statistics::TableStatsGenerator;
pub(crate) use table_statistics::stamp_table_statistics_with_snapshot_predecessor;
pub use traverse::traverse_values_dfs;
