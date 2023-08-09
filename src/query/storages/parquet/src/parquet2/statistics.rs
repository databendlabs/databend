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

use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::io::parquet::read as pread;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::TableDataType;
use common_storage::ColumnNodes;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

/// Collect statistics of a batch of row groups of the specified columns.
///
/// The returned vector's length is the same as `rgs`.
pub fn collect_row_group_stats(
    column_nodes: &ColumnNodes,
    rgs: &[RowGroupMetaData],
) -> Result<Vec<StatisticsOfColumns>> {
    let mut stats = Vec::with_capacity(rgs.len());
    let mut stats_of_row_groups = HashMap::with_capacity(rgs.len());

    // Each row_group_stat is a `HashMap` holding key-value pairs.
    // The first element of the pair is the offset in the schema,
    // and the second element is the statistics of the column (according to the offset)
    // `column_nodes` is parallel to the schema, so we can iterate `column_nodes` directly.
    for (index, column_node) in column_nodes.column_nodes.iter().enumerate() {
        let field = &column_node.field;
        let table_type: TableDataType = field.into();
        let data_type = (&table_type).into();
        let column_stats = pread::statistics::deserialize(field, rgs)?;
        stats_of_row_groups.insert(
            index,
            BatchStatistics::from_statistics(&column_stats, &data_type)?,
        );
    }

    for (rg_idx, _) in rgs.iter().enumerate() {
        let mut cols_stats = HashMap::with_capacity(stats.capacity());
        for index in 0..column_nodes.column_nodes.len() {
            let col_stats = stats_of_row_groups[&index].get(rg_idx);
            cols_stats.insert(index as u32, col_stats);
        }
        stats.push(cols_stats);
    }

    Ok(stats)
}

/// A temporary struct to present [`pread::statistics::Statistics`].
///
/// Convert the inner fields into Databend data structures.
pub struct BatchStatistics {
    pub null_count: Buffer<u64>,
    pub distinct_count: Option<Buffer<u64>>,
    pub min_values: Column,
    pub max_values: Column,
}

impl BatchStatistics {
    pub fn get(&self, index: usize) -> ColumnStatistics {
        ColumnStatistics::new(
            unsafe { self.min_values.index_unchecked(index).to_owned() },
            unsafe { self.max_values.index_unchecked(index).to_owned() },
            self.null_count[index],
            0, // this field is not used.
            self.distinct_count.as_ref().map(|d| d[index]),
        )
    }

    pub fn from_statistics(
        stats: &pread::statistics::Statistics,
        data_type: &DataType,
    ) -> Result<Self> {
        let null_count = stats
            .null_count
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "null_count should be UInt64Array, but is {:?}",
                    stats.null_count.data_type()
                ))
            })?
            .values()
            .clone();
        let distinct_count = stats
            .distinct_count
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|d| d.values())
            .cloned();
        let min_values = Column::from_arrow(&*stats.min_value, data_type);
        let max_values = Column::from_arrow(&*stats.max_value, data_type);
        Ok(Self {
            null_count,
            distinct_count,
            min_values,
            max_values,
        })
    }

    pub fn from_column_statistics(
        stats: &pread::indexes::ColumnPageStatistics,
        data_type: &DataType,
    ) -> Result<Self> {
        let null_count = stats.null_count.values().clone();
        let min_values = Column::from_arrow(&*stats.min, data_type);
        let max_values = Column::from_arrow(&*stats.max, data_type);
        Ok(Self {
            null_count,
            distinct_count: None,
            min_values,
            max_values,
        })
    }
}
