//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use common_arrow::parquet::FileMetaData;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::storages::fuse::meta::ColumnId;
use crate::storages::fuse::meta::ColumnMeta;

pub fn column_metas(file_meta: &FileMetaData) -> Result<HashMap<ColumnId, ColumnMeta>> {
    // currently we use one group only
    let num_row_groups = file_meta.row_groups.len();
    if num_row_groups != 1 {
        return Err(ErrorCode::ParquetError(format!(
            "invalid parquet file, expects only one row group, but got {}",
            num_row_groups
        )));
    }
    let row_group = &file_meta.row_groups[0];
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
                let res = ColumnMeta {
                    offset: col_start as u64,
                    len: col_len as u64,
                    num_values,
                };
                col_metas.insert(idx as u32, res);
            }
            None => {
                return Err(ErrorCode::ParquetError(format!(
                    "invalid parquet file, meta data of column idx {} is empty",
                    idx
                )))
            }
        }
    }
    Ok(col_metas)
}
