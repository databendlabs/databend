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
use std::sync::Arc;

use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::io::parquet::read as pread;
use databend_common_arrow::parquet::metadata::FileMetaData;
use databend_common_arrow::parquet::metadata::SchemaDescriptor;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table::Parquet2TableColumnStatisticsProvider;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_storage::infer_schema_with_extension;
use databend_common_storage::init_stage_operator;
use databend_common_storage::read_parquet_metas_in_parallel;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use opendal::Operator;

use super::table::create_parquet_table_info;
use super::Parquet2Table;
use crate::parquet2::parquet_table::table::create_parquet2_statistics_provider;

impl Parquet2Table {
    #[async_backtrace::framed]
    pub async fn create(
        ctx: Arc<dyn TableContext>,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        read_options: ParquetReadOptions,
        files_to_read: Option<Vec<StageFileInfo>>,
    ) -> Result<Arc<dyn Table>> {
        let operator = init_stage_operator(&stage_info)?;
        let first_file = match &files_to_read {
            Some(files) => files[0].path.clone(),
            None => files_info.first_file(&operator).await?.path.clone(),
        };

        let (arrow_schema, schema_descr, compression_ratio) =
            Self::prepare_metas(&first_file, operator.clone()).await?;

        // If the query is `COPY`, we don't need to collect column statistics.
        // It's because the only transform could be contained in `COPY` command is projection.
        let need_stats_provider = !matches!(ctx.get_query_kind(), QueryKind::CopyIntoTable);
        let mut table_info = create_parquet_table_info(arrow_schema.clone(), &stage_info);
        let column_statistics_provider = if need_stats_provider {
            let file_metas = get_parquet2_file_meta(
                ctx,
                &files_to_read,
                &files_info,
                &operator,
                &schema_descr,
                &first_file,
            )
            .await?;
            let num_rows = file_metas.iter().map(|m| m.num_rows as u64).sum();
            table_info.meta.statistics.number_of_rows = num_rows;
            create_parquet2_statistics_provider(file_metas, &arrow_schema)?
        } else {
            Parquet2TableColumnStatisticsProvider::new(HashMap::new(), 0)
        };

        Ok(Arc::new(Parquet2Table {
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
            column_statistics_provider,
        }))
    }

    #[async_backtrace::framed]
    async fn prepare_metas(
        path: &str,
        operator: Operator,
    ) -> Result<(ArrowSchema, SchemaDescriptor, f64)> {
        // Infer schema from the first parquet file.
        // Assume all parquet files have the same schema.
        // If not, throw error during reading.
        let mut reader = operator.reader(path).await?;
        let first_meta = pread::read_metadata_async(&mut reader).await.map_err(|e| {
            ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
        })?;
        let arrow_schema = infer_schema_with_extension(&first_meta)?;
        let compression_ratio = get_compression_ratio(&first_meta);
        let schema_descr = first_meta.schema_descr;
        Ok((arrow_schema, schema_descr, compression_ratio))
    }
}

pub(super) async fn get_parquet2_file_meta(
    ctx: Arc<dyn TableContext>,
    files_to_read: &Option<Vec<StageFileInfo>>,
    files_info: &StageFilesInfo,
    operator: &Operator,
    expect_schema: &SchemaDescriptor,
    schema_from: &str,
) -> Result<Vec<FileMetaData>> {
    let locations = match files_to_read {
        Some(files) => files
            .iter()
            .map(|f| (f.path.clone(), f.size))
            .collect::<Vec<_>>(),
        None => files_info
            .list(operator, false, None)
            .await?
            .into_iter()
            .map(|f| (f.path, f.size))
            .collect::<Vec<_>>(),
    };

    // Read parquet meta data, async reading.
    let max_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_memory_usage = ctx.get_settings().get_max_memory_usage()?;
    let file_metas = read_parquet_metas_in_parallel(
        operator.clone(),
        locations.clone(),
        max_threads,
        max_memory_usage,
    )
    .await?;
    for (idx, file_meta) in file_metas.iter().enumerate() {
        check_parquet_schema(
            expect_schema,
            file_meta.schema(),
            &locations[idx].0,
            schema_from,
        )?;
    }
    Ok(file_metas)
}

pub fn check_parquet_schema(
    expect: &SchemaDescriptor,
    actual: &SchemaDescriptor,
    path: &str,
    schema_from: &str,
) -> Result<()> {
    if expect.fields() != actual.fields() || expect.columns() != actual.columns() {
        return Err(ErrorCode::BadBytes(format!(
            "infer schema from '{}', but get diff schema in file '{}'. Expected schema: {:?}, actual: {:?}",
            schema_from, path, expect, actual
        )));
    }
    Ok(())
}

pub fn get_compression_ratio(filemeta: &FileMetaData) -> f64 {
    let compressed_size: usize = filemeta
        .row_groups
        .iter()
        .map(|g| g.compressed_size())
        .sum();
    let uncompressed_size: usize = filemeta
        .row_groups
        .iter()
        .map(|g| {
            g.columns()
                .iter()
                .map(|c| c.uncompressed_size() as usize)
                .sum::<usize>()
        })
        .sum();
    if compressed_size == 0 {
        1.0
    } else {
        (uncompressed_size as f64) / (compressed_size as f64)
    }
}
