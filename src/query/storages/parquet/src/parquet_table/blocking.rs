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

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read as pread;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::StageInfo;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use opendal::Operator;

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

        let arrow_schema = Self::blocking_prepare_metas(&first_file, operator.clone())?;

        let table_info = create_parquet_table_info(arrow_schema.clone());

        Ok(Arc::new(ParquetTable {
            table_info,
            arrow_schema,
            operator,
            read_options,
            stage_info,
            files_info,
            files_to_read,
        }))
    }

    fn blocking_prepare_metas(path: &str, operator: Operator) -> Result<ArrowSchema> {
        // Infer schema from the first parquet file.
        // Assume all parquet files have the same schema.
        // If not, throw error during reading.
        let mut reader = operator.blocking().reader(path)?;
        let first_meta = pread::read_metadata(&mut reader).map_err(|e| {
            ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
        })?;

        let arrow_schema = pread::infer_schema(&first_meta)?;

        Ok(arrow_schema)
    }
}
