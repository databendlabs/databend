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
use common_base::base::tokio;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use futures::TryStreamExt;
use glob::Pattern;
use opendal::raw::get_basename;
use opendal::raw::get_parent;
use opendal::ObjectMode;
use opendal::Operator;

use super::table::create_parquet_table_info;
use crate::ParquetTable;

impl ParquetTable {
    pub async fn create(
        table_id: u64,
        operator: Operator,
        maybe_glob_locations: Vec<String>,
        read_options: ParquetReadOptions,
        stage_info: UserStageInfo,
    ) -> Result<Arc<dyn Table>> {
        if operator.metadata().can_blocking() {
            return Self::blocking_create(
                table_id,
                operator,
                maybe_glob_locations,
                read_options,
                stage_info,
            );
        }

        let (file_locations, arrow_schema) =
            Self::prepare_metas(maybe_glob_locations, operator.clone()).await?;

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

    async fn prepare_metas(
        paths: Vec<String>,
        operator: Operator,
    ) -> Result<(Vec<String>, ArrowSchema)> {
        let mut handles = Vec::with_capacity(paths.len());
        for maybe_glob_path in paths {
            let operator = operator.clone();
            handles.push(async move {
                tokio::spawn(async move { Self::list_files(&maybe_glob_path, &operator).await })
                    .await
                    .unwrap()
            });
        }
        let files = futures::future::try_join_all(handles)
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        if files.is_empty() {
            return Err(ErrorCode::BadArguments(
                "No matched files found for read_parquet",
            ));
        }

        // Infer schema from the first parquet file.
        // Assume all parquet files have the same schema.
        // If not, throw error during reading.
        let mut reader = operator.object(&files[0]).reader().await?;
        let first_meta = pread::read_metadata_async(&mut reader).await.map_err(|e| {
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
    async fn list_files(maybe_glob_path: &str, operator: &Operator) -> Result<Vec<String>> {
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
        let mut list = obj.list().await?;
        while let Some(de) = list.try_next().await? {
            match de.mode().await? {
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
