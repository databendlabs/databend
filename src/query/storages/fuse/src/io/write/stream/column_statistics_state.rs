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

use std::collections::BTreeMap;
use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

use crate::io::write::stream::ColumnNDVEstimator;
use crate::io::write::stream::ColumnNDVEstimatorOps;
use crate::io::write::stream::ColumnStatisticsBuilder;
use crate::io::write::stream::ColumnStatsOps;
use crate::io::write::stream::create_column_ndv_estimator;
use crate::io::write::stream::create_column_stats_builder;
use crate::statistics::traverse_values_dfs;

pub struct ColumnStatisticsState {
    col_stats: HashMap<ColumnId, ColumnStatisticsBuilder>,
    distinct_columns: HashMap<ColumnId, ColumnNDVEstimator>,
}

impl ColumnStatisticsState {
    pub fn new(
        stats_columns: &[(ColumnId, DataType)],
        distinct_columns: &[(ColumnId, DataType)],
        col_stats_truncate_lens: &BTreeMap<ColumnId, usize>,
    ) -> Self {
        let col_stats = stats_columns
            .iter()
            .map(|(col_id, data_type)| {
                let string_len = col_stats_truncate_lens
                    .get(col_id)
                    .copied()
                    .unwrap_or(crate::statistics::STATS_STRING_PREFIX_LEN);
                (*col_id, create_column_stats_builder(data_type, string_len))
            })
            .collect();

        let distinct_columns = distinct_columns
            .iter()
            .map(|(col_id, data_type)| (*col_id, create_column_ndv_estimator(data_type)))
            .collect();

        Self {
            col_stats,
            distinct_columns,
        }
    }

    pub fn add_block(&mut self, schema: &TableSchemaRef, data_block: &DataBlock) -> Result<()> {
        let rows = data_block.num_rows();
        let leaves = traverse_values_dfs(data_block.columns(), schema.fields())?;
        for (column_id, col, data_type) in leaves {
            match col {
                Value::Scalar(s) => {
                    self.col_stats.get_mut(&column_id).unwrap().update_scalar(
                        &s.as_ref(),
                        rows,
                        &data_type,
                    );
                    if let Some(estimator) = self.distinct_columns.get_mut(&column_id) {
                        estimator.update_scalar(&s.as_ref(), rows as u64);
                    }
                }
                Value::Column(col) => {
                    self.col_stats
                        .get_mut(&column_id)
                        .unwrap()
                        .update_column(&col);
                    // use distinct count calculated by the xor hash function to avoid repetitive operation.
                    if let Some(estimator) = self.distinct_columns.get_mut(&column_id) {
                        estimator.update_column(&col);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn add_column(
        &mut self,
        field: &databend_common_expression::TableField,
        column: &databend_common_expression::Column,
    ) -> Result<()> {
        let entry = databend_common_expression::BlockEntry::Column(column.clone());
        let leaves =
            traverse_values_dfs(std::slice::from_ref(&entry), std::slice::from_ref(field))?;
        for (column_id, value, data_type) in leaves {
            let Value::Column(column) = value else {
                unreachable!("column traversal cannot produce a scalar")
            };
            let Some(stats) = self.col_stats.get_mut(&column_id) else {
                return Err(databend_common_exception::ErrorCode::Internal(format!(
                    "missing statistics builder for column {column_id}"
                )));
            };
            stats.update_column_streaming(&column);
            if let Some(estimator) = self.distinct_columns.get_mut(&column_id) {
                estimator.update_column(&column);
            }
            debug_assert_eq!(column.data_type(), data_type);
        }
        Ok(())
    }

    pub fn peek_cols_ndv(&self) -> HashMap<ColumnId, usize> {
        self.distinct_columns
            .iter()
            .map(|(column_id, ndv_estimator)| (*column_id, ndv_estimator.peek()))
            .collect()
    }

    pub fn finalize(
        self,
        mut column_distinct_count: HashMap<ColumnId, usize>,
    ) -> Result<StatisticsOfColumns> {
        for (column_id, estimator) in &self.distinct_columns {
            column_distinct_count.insert(*column_id, estimator.finalize());
        }

        let mut statistics = StatisticsOfColumns::with_capacity(self.col_stats.len());
        for (id, stats) in self.col_stats {
            let mut col_stats = stats.finalize()?;
            if let Some(count) = column_distinct_count.get(&id) {
                col_stats.distinct_of_values = Some(*count as u64);
            } else if col_stats.min == col_stats.max {
                // Bloom index will skip the large string column, it also no need to calc distinct values.
                if col_stats.min.is_null() {
                    col_stats.distinct_of_values = Some(0);
                } else {
                    col_stats.distinct_of_values = Some(1);
                }
            }
            statistics.insert(id, col_stats);
        }
        Ok(statistics)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_storages_common_index::Index;
    use databend_storages_common_index::RangeIndex;

    use super::*;
    use crate::statistics::gen_columns_statistics;

    #[test]
    fn test_column_stats_state() -> Result<()> {
        let field1 = TableField::new(
            "a",
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int64))),
        );
        let field2 = TableField::new("b", TableDataType::String);
        let field3 = TableField::new("c", TableDataType::Tuple {
            fields_name: vec!["d".to_string(), "e".to_string()],
            fields_type: vec![
                TableDataType::Number(NumberDataType::UInt64),
                TableDataType::Binary,
            ],
        });
        let schema = Arc::new(TableSchema::new(vec![field1, field2, field3]));
        let block = DataBlock::new_from_columns(vec![
            Int64Type::from_opt_data(vec![Some(1), Some(2), None, Some(4), Some(5)]),
            StringType::from_data(vec!["a", "b", "c", "d", "e"]),
            Column::Tuple(vec![
                UInt64Type::from_data(vec![11, 12, 13, 14, 15]),
                BinaryType::from_data(vec![
                    "hello".as_bytes().to_vec(),
                    "world".as_bytes().to_vec(),
                    "".as_bytes().to_vec(),
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                ]),
            ]),
        ]);

        let stats_0 =
            gen_columns_statistics(&block, None, &schema, &std::collections::BTreeMap::new())?;

        let mut stats_columns = vec![];
        let leaf_fields = schema.leaf_fields();
        for field in leaf_fields.iter() {
            let column_id = field.column_id();
            let data_type = DataType::from(field.data_type());
            if RangeIndex::supported_type(&data_type) {
                stats_columns.push((column_id, data_type.clone()));
            }
        }
        let mut column_stats_state =
            ColumnStatisticsState::new(&stats_columns, &stats_columns, &BTreeMap::new());
        column_stats_state.add_block(&schema, &block)?;
        let stats_1 = column_stats_state.finalize(HashMap::new())?;

        let mut single_fragment_state =
            ColumnStatisticsState::new(&stats_columns, &stats_columns, &BTreeMap::new());
        for (field_index, entry) in block.columns().iter().enumerate() {
            single_fragment_state.add_column(schema.field(field_index), &entry.to_column())?;
        }
        let single_fragment_stats = single_fragment_state.finalize(HashMap::new())?;

        let mut fragmented_state =
            ColumnStatisticsState::new(&stats_columns, &stats_columns, &BTreeMap::new());
        for (field_index, entry) in block.columns().iter().enumerate() {
            let column = entry.to_column();
            fragmented_state.add_column(schema.field(field_index), &column.slice(0..2))?;
            fragmented_state.add_column(schema.field(field_index), &column.slice(2..5))?;
        }
        let fragmented_stats = fragmented_state.finalize(HashMap::new())?;

        assert_eq!(stats_0, stats_1);
        assert_eq!(stats_0, single_fragment_stats);
        for (column_id, expected) in &stats_0 {
            let actual = fragmented_stats.get(column_id).unwrap();
            assert_eq!(actual.min, expected.min);
            assert_eq!(actual.max, expected.max);
            assert_eq!(actual.null_count, expected.null_count);
            assert_eq!(actual.distinct_of_values, expected.distinct_of_values);
        }
        assert_eq!(
            fragmented_stats
                .get(&schema.field(0).column_id())
                .unwrap()
                .in_memory_size,
            41
        );
        let string_column = block.get_by_offset(1).to_column();
        assert_eq!(
            fragmented_stats
                .get(&schema.field(1).column_id())
                .unwrap()
                .in_memory_size,
            string_column.memory_size(true) as u64
        );
        Ok(())
    }
}
