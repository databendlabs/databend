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

use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::TableSchemaRef;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::SingleColumnMeta;

pub fn column_parquet_metas(
    file_meta: &ThriftFileMetaData,
    schema: &TableSchemaRef,
) -> Result<HashMap<ColumnId, ColumnMeta>> {
    // currently we use one group only
    let num_row_groups = file_meta.row_groups.len();
    if num_row_groups != 1 {
        return Err(ErrorCode::ParquetFileInvalid(format!(
            "invalid parquet file, expects only one row group, but got {}",
            num_row_groups
        )));
    }
    // use `to_leaf_column_ids` instead of `to_column_ids` to handle nested type column ids.
    let column_ids = schema.to_leaf_column_ids();
    let row_group = &file_meta.row_groups[0];
    // Make sure that schema and row_group has the same number column, or else it is a panic error.
    assert_eq!(column_ids.len(), row_group.columns.len());
    let mut col_metas = HashMap::with_capacity(row_group.columns.len());
    for (idx, col_chunk) in row_group.columns.iter().enumerate() {
        match &col_chunk.meta_data {
            Some(chunk_meta) => {
                let col_start = if let Some(dict_page_offset) = chunk_meta.dictionary_page_offset {
                    dict_page_offset
                } else {
                    chunk_meta.data_page_offset
                };
                let col_len = chunk_meta.total_compressed_size;
                assert!(
                    col_start >= 0 && col_len >= 0,
                    "column start and length should not be negative"
                );
                let num_values = chunk_meta.num_values as u64;
                let res = SingleColumnMeta {
                    offset: col_start as u64,
                    len: col_len as u64,
                    num_values,
                };
                // use column id as key instead of index
                let column_id = column_ids[idx];
                col_metas.insert(column_id, ColumnMeta::Parquet(res));
            }
            None => {
                return Err(ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet file, meta data of column idx {} is empty",
                    idx
                )));
            }
        }
    }
    Ok(col_metas)
}
