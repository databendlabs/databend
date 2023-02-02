//  Copyright 2023 Datafuse Labs.
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

use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read as pread;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use glob::Pattern;
use opendal::raw::get_basename;
use opendal::raw::get_parent;
use opendal::ObjectMode;
use opendal::Operator;

use super::table::create_parquet_table_info;
use crate::ParquetTable;

impl ParquetTable {
    pub fn blocking_create(
        table_id: u64,
        operator: Operator,
        maybe_glob_locations: Vec<String>,
        read_options: ParquetReadOptions,
        stage_info: UserStageInfo,
    ) -> Result<Arc<dyn Table>> {
        let (file_locations, arrow_schema) =
            Self::blocking_prepare_metas(maybe_glob_locations, operator.clone())?;

        let table_info = create_parquet_table_info(table_id, arrow_schema.clone());

        Ok(Arc::new(ParquetTable {
            file_locations,
            table_info,
            arrow_schema,
            operator,
            read_options,
            stage_info,
        }))
    }

    fn blocking_prepare_metas(
        paths: Vec<String>,
        operator: Operator,
    ) -> Result<(Vec<String>, ArrowSchema)> {
        let mut files = Vec::with_capacity(paths.len());
        for maybe_glob_path in paths {
            let list = Self::blocking_list_files(&maybe_glob_path, &operator)?;
            files.extend(list);
        }

        if files.is_empty() {
            return Err(ErrorCode::BadArguments(
                "No matched files found for read_parquet",
            ));
        }

        // Infer schema from the first parquet file.
        // Assume all parquet files have the same schema.
        // If not, throw error during reading.
        let mut reader = operator.object(&files[0]).blocking_reader()?;
        let first_meta = pread::read_metadata(&mut reader).map_err(|e| {
            ErrorCode::Internal(format!(
                "Read parquet file '{}''s meta error: {}",
                &files[0], e
            ))
        })?;

        let arrow_schema = pread::infer_schema(&first_meta)?;

        Ok((files, arrow_schema))
    }

    /// List files from the given path with pattern.
    ///
    /// Only support simple patterns (one level): `path/to/dir/*.parquet`.
    fn blocking_list_files(maybe_glob_path: &str, operator: &Operator) -> Result<Vec<String>> {
        let basename = get_basename(maybe_glob_path);
        let pattern = match Pattern::new(basename) {
            Ok(pattern) => pattern,
            Err(_) => {
                // not a Unix shell pattern, push the path directly.
                return Ok(vec![maybe_glob_path.to_string()]);
            }
        };

        let obj = operator.object(get_parent(maybe_glob_path));
        let mut files = Vec::new();
        let list = obj.blocking_list()?;
        for de in list {
            let de = de?;
            match de.blocking_mode()? {
                ObjectMode::FILE => {
                    if pattern.matches(de.name()) {
                        files.push(de.path().to_string());
                    }
                }
                _ => continue,
            }
        }

        Ok(files)
    }
}
