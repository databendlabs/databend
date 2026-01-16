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

use databend_common_expression::ColumnId;
use databend_common_expression::TableSchema;
use databend_common_expression::converts::arrow::table_schema_arrow_leaf_paths;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use parquet::schema::types::ColumnPath;

pub mod delta_binary_packed;
pub mod page_limit;

pub mod dictionary;

pub struct ColumnPathsCache {
    cache: Option<HashMap<ColumnId, ColumnPath>>,
}

impl ColumnPathsCache {
    pub fn new() -> Self {
        Self { cache: None }
    }

    pub fn get_or_build(&mut self, table_schema: &TableSchema) -> &HashMap<ColumnId, ColumnPath> {
        if self.cache.is_none() {
            self.cache = Some(
                table_schema_arrow_leaf_paths(table_schema)
                    .into_iter()
                    .map(|(id, path)| (id, ColumnPath::from(path)))
                    .collect(),
            );
        }
        self.cache.as_ref().unwrap()
    }
}

/// Provides per column NDV statistics.
pub trait NdvProvider {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64>;
}

impl NdvProvider for &StatisticsOfColumns {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.get(column_id).and_then(|item| item.distinct_of_values)
    }
}

pub trait EncodingStatsProvider: NdvProvider {
    fn column_stats(&self, column_id: &ColumnId) -> Option<&ColumnStatistics>;
}

pub struct ColumnStatsView<'a>(pub &'a StatisticsOfColumns);

impl<'a> NdvProvider for ColumnStatsView<'a> {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.0
            .get(column_id)
            .and_then(|item| item.distinct_of_values)
    }
}

impl<'a> EncodingStatsProvider for ColumnStatsView<'a> {
    fn column_stats(&self, column_id: &ColumnId) -> Option<&ColumnStatistics> {
        self.0.get(column_id)
    }
}
