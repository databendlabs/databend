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

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql_test_support::ColumnStats;
use databend_common_sql_test_support::HistogramStats;
use databend_common_statistics::Histogram;

mod collect_statistics;
mod decorrelate_correlated_aliases;
mod eager_aggregation;
mod join_cardinality;
mod materialized_cte_distribution;
mod normalize_scalar;
mod outer_join_to_anti;
mod push_down_filter_project_set;
mod selectivity;
mod selectivity_smoke;
mod stat_derivation;
mod union_all;

fn table_statistics(rows: u64) -> TableStatistics {
    TableStatistics {
        num_rows: Some(rows),
        data_size: Some(rows.saturating_mul(8)),
        data_size_compressed: None,
        index_size: None,
        bloom_index_size: None,
        ngram_index_size: None,
        inverted_index_size: None,
        vector_index_size: None,
        virtual_column_size: None,
        number_of_blocks: Some(1),
        number_of_segments: Some(1),
    }
}

fn column_stat(json: &str) -> Result<BasicColumnStatistics> {
    let stats: ColumnStats = serde_json::from_str(json)
        .map_err(|err| ErrorCode::Internal(format!("invalid column statistics JSON: {err}")))?;
    Ok(stats.to_basic_column_statistics())
}

fn histogram_stat(json: &str) -> Result<Histogram> {
    let stats: HistogramStats = serde_json::from_str(json)
        .map_err(|err| ErrorCode::Internal(format!("invalid histogram statistics JSON: {err}")))?;
    stats.to_histogram()
}
