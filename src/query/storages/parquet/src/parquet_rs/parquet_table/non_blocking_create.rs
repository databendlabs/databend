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
use common_storage::init_stage_operator;
use common_storage::parquet_rs::infer_schema_with_extension;
use common_storage::parquet_rs::read_metadata_async;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use opendal::Operator;
use parquet::schema::types::SchemaDescPtr;

use super::table::create_parquet_table_info;
use crate::parquet_rs::parquet_table::blocking_create::get_compression_ratio;
use crate::ParquetTable;

impl ParquetTable {
    #[async_backtrace::framed]
    pub async fn create(
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        read_options: ParquetReadOptions,
        files_to_read: Option<Vec<StageFileInfo>>,
    ) -> Result<Arc<dyn Table>> {
        let operator = init_stage_operator(&stage_info)?;
        if operator.info().can_blocking() {
            return Self::blocking_create(
                operator,
                read_options,
                stage_info,
                files_info,
                files_to_read,
            );
        }
        let first_file = match &files_to_read {
            Some(files) => files[0].path.clone(),
            None => files_info.first_file(&operator).await?.path.clone(),
        };

        let (arrow_schema, schema_descr, compression_ratio) =
            Self::prepare_metas(&first_file, operator.clone()).await?;

        let table_info = create_parquet_table_info(&arrow_schema)?;

        Ok(Arc::new(ParquetTable {
            table_info,
            arrow_schema,
            operator,
            read_options,
            schema_descr,
            stage_info,
            files_info,
            files_to_read,
            compression_ratio,
            schema_from: first_file,
        }))
    }

    #[async_backtrace::framed]
    pub async fn prepare_metas(
        path: &str,
        operator: Operator,
    ) -> Result<(ArrowSchema, SchemaDescPtr, f64)> {
        // Infer schema from the first parquet file.
        // Assume all parquet files have the same schema.
        // If not, throw error during reading.
        let size = operator.stat(path).await?.content_length();
        let first_meta = read_metadata_async(path, &operator, Some(size))
            .await
            .map_err(|e| {
                ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
            })?;
        let arrow_schema = infer_schema_with_extension(&first_meta)?;
        let compression_ratio = get_compression_ratio(&first_meta);
        let schema_descr = first_meta.file_metadata().schema_descr_ptr();
        Ok((arrow_schema, schema_descr, compression_ratio))
    }
}
