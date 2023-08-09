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

use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::StageInfo;
use common_storage::parquet_rs::infer_schema_with_extension;
use common_storage::parquet_rs::read_metadata;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;

use super::table::create_parquet_table_info;
use crate::ParquetTable;

impl ParquetTable {
    pub fn blocking_create(
        operator: Operator,
        read_options: ParquetReadOptions,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        files_to_read: Option<Vec<StageFileInfo>>,
    ) -> Result<Arc<dyn Table>> {
        let first_file = match &files_to_read {
            Some(files) => files[0].path.clone(),
            None => files_info.blocking_first_file(&operator)?.path,
        };

        let (arrow_schema, schema_descr, compression_ratio) =
            Self::blocking_prepare_metas(&first_file, operator.clone())?;

        let table_info = create_parquet_table_info(&arrow_schema)?;
        Ok(Arc::new(ParquetTable {
            table_info,
            arrow_schema,
            schema_descr,
            operator,
            read_options,
            stage_info,
            files_info,
            files_to_read,
            compression_ratio,
            schema_from: first_file,
        }))
    }

    pub fn blocking_prepare_metas(
        path: &str,
        operator: Operator,
    ) -> Result<(ArrowSchema, SchemaDescPtr, f64)> {
        let op = operator.blocking();
        let size = op.stat(path)?.content_length();
        let first_meta = read_metadata(path, &op, size).map_err(|e| {
            ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
        })?;

        let arrow_schema = infer_schema_with_extension(&first_meta)?;
        let compression_ratio = get_compression_ratio(&first_meta);
        let schema_descr = first_meta.file_metadata().schema_descr_ptr();
        Ok((arrow_schema, schema_descr, compression_ratio))
    }
}

pub fn get_compression_ratio(filemeta: &ParquetMetaData) -> f64 {
    let compressed_size: i64 = filemeta
        .row_groups()
        .iter()
        .map(|g| g.compressed_size())
        .sum();
    let uncompressed_size: i64 = filemeta
        .row_groups()
        .iter()
        .map(|g| {
            g.columns()
                .iter()
                .map(|c| c.uncompressed_size())
                .sum::<i64>()
        })
        .sum();
    if compressed_size == 0 {
        1.0
    } else {
        (uncompressed_size as f64) / (compressed_size as f64)
    }
}
