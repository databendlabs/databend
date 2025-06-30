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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

use crate::io::write::stream::create_column_ndv_estimator;
use crate::io::write::stream::create_column_stats_builder;
use crate::io::write::stream::ColumnNDVEstimator;
use crate::io::write::stream::ColumnStatisticsBuilder;
use crate::statistics::traverse_values_dfs;

pub struct ColumnStatisticsState {
    col_stats: HashMap<ColumnId, Box<dyn ColumnStatisticsBuilder>>,
    distinct_columns: HashMap<ColumnId, Box<dyn ColumnNDVEstimator>>,
}

impl ColumnStatisticsState {
    pub fn new(
        stats_columns: &[(ColumnId, DataType)],
        distinct_columns: &[(ColumnId, DataType)],
    ) -> Self {
        let col_stats = stats_columns
            .iter()
            .map(|(col_id, data_type)| (*col_id, create_column_stats_builder(data_type)))
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
                        estimator.update_scalar(&s.as_ref());
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

    pub fn finalize(
        self,
        column_distinct_count: HashMap<ColumnId, usize>,
    ) -> Result<StatisticsOfColumns> {
        let mut statistics = StatisticsOfColumns::with_capacity(self.col_stats.len());
        for (id, stats) in self.col_stats {
            let mut col_stats = stats.finalize()?;
            if let Some(count) = column_distinct_count.get(&id) {
                // value calculated by xor hash function include NULL, need to subtract one.
                let distinct_of_values = if col_stats.null_count > 0 {
                    *count as u64 - 1
                } else {
                    *count as u64
                };
                col_stats.distinct_of_values = Some(distinct_of_values);
            } else if let Some(estimator) = self.distinct_columns.get(&id) {
                col_stats.distinct_of_values = Some(estimator.finalize());
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
