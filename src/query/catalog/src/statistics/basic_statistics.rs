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

pub use databend_common_statistics::BasicColumnStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;

/// Adapt persisted per-chunk metadata such as a Parquet row group. Its
/// distinct count describes that chunk only and bounds NDV from above.
/// All statistical interpretation belongs to the shared statistics model.
pub fn from_chunk_column_statistics(
    value: ColumnStatistics,
    num_rows: u64,
) -> BasicColumnStatistics {
    BasicColumnStatistics {
        min: value.min.to_datum(),
        max: value.max.to_datum(),
        ndv: None,
        null_count: value.null_count,
        in_memory_size: value.in_memory_size,
    }
    .with_chunk_ndv_upper(value.distinct_of_values, num_rows)
}
