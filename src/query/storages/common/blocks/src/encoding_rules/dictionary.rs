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

use databend_common_expression::TableSchema;
use parquet::file::properties::WriterPropertiesBuilder;

use crate::encoding_rules::ColumnPathsCache;
use crate::encoding_rules::EncodingStatsProvider;

/// Disable dictionary encoding once the NDV-to-row ratio is greater than this threshold.
const HIGH_CARDINALITY_RATIO_THRESHOLD: f64 = 0.1;

pub fn apply_dictionary_high_cardinality_heuristic(
    mut builder: WriterPropertiesBuilder,
    metrics: &dyn EncodingStatsProvider,
    table_schema: &TableSchema,
    num_rows: usize,
    column_paths_cache: &mut ColumnPathsCache,
) -> WriterPropertiesBuilder {
    if num_rows == 0 {
        return builder;
    }
    let column_paths = column_paths_cache.get_or_build(table_schema);
    for (column_id, column_path) in column_paths.iter() {
        if let Some(ndv) = metrics.column_ndv(column_id) {
            if (ndv as f64 / num_rows as f64) > HIGH_CARDINALITY_RATIO_THRESHOLD {
                builder = builder.set_column_dictionary_enabled(column_path.clone(), false);
            }
        }
    }
    builder
}
