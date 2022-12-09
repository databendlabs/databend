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
use common_arrow::parquet::metadata::FileMetaData;
use common_datavalues::DataField;
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
        // Do not use `pread::infer_schema(meta)` becuase it will use metadata `ARROW:schema`.
        // There maybe dictionary types in the schema, which is not supported by Databend.
        // So we need to convert the primitive schema directly.
        let field = pread::schema::parquet_to_arrow_schema(meta.schema().fields())
            .into_iter()
            .map(|mut f| {
                // Need to change all the field name to lowercase.
                f.name = f.name.to_lowercase();
                DataField::from(&f)
            })
            .collect::<Vec<_>>();

        Ok(DataSchema::new(field))
    }
}
