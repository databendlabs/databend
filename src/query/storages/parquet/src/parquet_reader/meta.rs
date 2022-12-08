// Copyright 2022 Datafuse Labs.
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

use std::fs::File;

use common_arrow::arrow::io::parquet::read as pread;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::ParquetReader;

impl ParquetReader {
    pub fn read_meta(location: &str) -> Result<FileMetaData> {
        let mut file = File::open(location).map_err(|e| {
            ErrorCode::Internal(format!("Failed to open file '{}': {}", location, e))
        })?;
        pread::read_metadata(&mut file).map_err(|e| {
            ErrorCode::Internal(format!(
                "Read parquet file '{}''s meta error: {}",
                location, e
            ))
        })
    }

    #[inline]
    pub fn infer_schema(meta: &FileMetaData) -> Result<DataSchema> {
        Ok(DataSchema::from(pread::infer_schema(meta)?))
    }

    pub fn get_column_metas<'a>(
        &self,
        rg: &'a RowGroupMetaData,
    ) -> Vec<(usize, &'a ColumnChunkMetaData)> {
        let columns = rg.columns();
        let column_leaves = &self.projected_column_leaves.column_leaves;
        let mut column_metas =
            Vec::with_capacity(column_leaves.iter().map(|col| col.leaf_ids.len()).sum());
        for column in column_leaves {
            let indices = &column.leaf_ids;
            for index in indices {
                let column_meta = &columns[*index];
                column_metas.push((*index, column_meta));
            }
        }
        column_metas
    }
}
