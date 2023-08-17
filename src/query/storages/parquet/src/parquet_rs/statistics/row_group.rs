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
use common_expression::TableField;
use parquet::file::metadata::RowGroupMetaData;
use storages_common_table_meta::meta::StatisticsOfColumns;

use crate::parquet_rs::statistics::column::convert_column_statistics;

/// Collect statistics of a batch of row groups.
///
/// The returned vector's length is the same as `rgs`.
pub fn collect_row_group_stats(
    rgs: &[RowGroupMetaData],
    leaf_fields: &[TableField],
    columns: &[usize],
) -> Result<Vec<StatisticsOfColumns>> {
    let mut stats = Vec::with_capacity(rgs.len());
    for rg in rgs {
        assert_eq!(rg.num_columns(), leaf_fields.len());
        let mut stats_of_columns = HashMap::with_capacity(rg.columns().len());

        // Each row_group_stat is a `HashMap` holding key-value pairs.
        // The first element of the pair is the offset in the schema,
        // and the second element is the statistics of the column (according to the offset)
        for col_idx in columns.iter() {
            let column = rg.column(*col_idx);
            let field = &leaf_fields[*col_idx];
            let column_stats = column.statistics().unwrap();
            stats_of_columns.insert(
                *col_idx as u32,
                convert_column_statistics(column_stats, &field.data_type().remove_nullable()),
            );
        }
        stats.push(stats_of_columns);
    }
    Ok(stats)
}
