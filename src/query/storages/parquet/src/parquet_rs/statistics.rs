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

use common_exception::Result;
use common_expression::Scalar;
use parquet::data_type::AsBytes;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

use crate::parquet_rs::column_nodes::ColumnNodesRS;

/// Collect statistics of a batch of row groups of the specified columns.
///
/// The returned vector's length is the same as `rgs`.
pub fn collect_row_group_stats(
    column_nodes: &ColumnNodesRS,
    rgs: &[RowGroupMetaData],
) -> Result<Vec<StatisticsOfColumns>> {
    let name_to_idx = rgs[0]
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| (col.column_path().parts()[0].clone(), idx))
        .collect::<HashMap<String, usize>>();
    let column_ids = column_nodes
        .column_nodes
        .iter()
        .map(|column_node| name_to_idx[column_node.field.name()])
        .collect::<Vec<_>>();

    let mut stats = Vec::with_capacity(rgs.len());
    for rg in rgs {
        let mut stats_of_columns = HashMap::with_capacity(column_ids.len());

        // Each row_group_stat is a `HashMap` holding key-value pairs.
        // The first element of the pair is the offset in the schema,
        // and the second element is the statistics of the column (according to the offset)
        // `column_nodes` is parallel to the schema, so we can iterate `column_nodes` directly.
        for (index, _column_node) in column_nodes.column_nodes.iter().enumerate() {
            let column_stats = rg.column(column_ids[index]).statistics().unwrap();
            stats_of_columns.insert(index, convert_column_statistics(column_stats));
        }
        stats.push(StatisticsOfColumns::default());
    }
    Ok(stats)
}

fn convert_column_statistics(s: &Statistics) -> ColumnStatistics {
    let (max, min) = if s.has_min_max_set() {
        match s {
            Statistics::Boolean(s) => (Scalar::Boolean(*s.max()), Scalar::Boolean(*s.min())),
            Statistics::Int32(s) => (Scalar::from(*s.max()), Scalar::from(*s.min())),
            Statistics::Int64(s) => (Scalar::from(*s.max()), Scalar::from(*s.min())),
            Statistics::Int96(s) => (
                Scalar::Timestamp(s.max().to_i64()),
                Scalar::Timestamp(s.min().to_i64()),
            ),
            Statistics::Float(s) => (Scalar::from(*s.max()), Scalar::from(*s.min())),
            Statistics::Double(s) => (Scalar::from(*s.max()), Scalar::from(*s.min())),
            Statistics::ByteArray(s) => (
                Scalar::String(s.max().as_bytes().to_vec()),
                Scalar::String(s.max().as_bytes().to_vec()),
            ),
            Statistics::FixedLenByteArray(s) => (
                Scalar::String(s.max().as_bytes().to_vec()),
                Scalar::String(s.max().as_bytes().to_vec()),
            ),
        }
    } else {
        (Scalar::Null, Scalar::Null)
    };
    ColumnStatistics {
        min,
        max,
        null_count: s.null_count(),
        in_memory_size: 0, // this field is not used.
        distinct_of_values: s.distinct_count(),
    }
}
