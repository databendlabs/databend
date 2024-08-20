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

use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use parquet::file::metadata::RowGroupMetaData;

pub(crate) fn build_columns_meta(row_group: &RowGroupMetaData) -> HashMap<u32, ColumnMeta> {
    let mut columns_meta = HashMap::with_capacity(row_group.columns().len());
    for (index, c) in row_group.columns().iter().enumerate() {
        let (offset, len) = c.byte_range();
        columns_meta.insert(
            index as u32,
            ColumnMeta::Parquet(SingleColumnMeta {
                offset,
                len,
                num_values: c.num_values() as u64,
            }),
        );
    }
    columns_meta
}
