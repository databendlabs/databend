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

use std::intrinsics::unlikely;
use std::sync::Arc;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::plan::FullParquetMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableField;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;
use parquet::schema::types::SchemaDescriptor;

use crate::parquet_rs::statistics::collect_row_group_stats;

#[async_backtrace::framed]
pub async fn read_metas_in_parallel(
    op: &Operator,
    file_infos: &[(String, u64)],
    expected: (SchemaDescPtr, String),
    leaf_fields: Arc<Vec<TableField>>,
    num_threads: usize,
    max_memory_usage: u64,
) -> Result<Vec<Arc<FullParquetMeta>>> {
    if file_infos.is_empty() {
        return Ok(vec![]);
    }
    let num_files = file_infos.len();

    let mut tasks = Vec::with_capacity(num_threads);
    // Equally distribute the tasks
    for i in 0..num_threads {
        let begin = num_files * i / num_threads;
        let end = num_files * (i + 1) / num_threads;
        if begin == end {
            continue;
        }

        let file_infos = file_infos[begin..end].to_vec();
        let op = op.clone();
        let (expected_schema, schema_from) = expected.clone();
        let leaf_fields = leaf_fields.clone();

        tasks.push(read_parquet_metas_batch(
            file_infos,
            op,
            expected_schema,
            leaf_fields,
            schema_from,
            max_memory_usage,
        ));
    }

    let metas = execute_futures_in_parallel(
        tasks,
        num_threads,
        num_threads * 2,
        "read-parquet-metas-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    Ok(metas)
}

fn check_parquet_schema(
    expect: &SchemaDescriptor,
    actual: &SchemaDescriptor,
    path: &str,
    schema_from: &str,
) -> Result<()> {
    if expect.root_schema() != actual.root_schema() {
        return Err(ErrorCode::TableSchemaMismatch(format!(
            "infer schema from '{}', but get diff schema in file '{}'. Expected schema: {:?}, actual: {:?}",
            schema_from, path, expect, actual
        )));
    }
    Ok(())
}

#[async_backtrace::framed]
pub async fn read_metas_in_parallel_for_copy(
    op: &Operator,
    file_infos: &[(String, u64)],
    num_threads: usize,
    max_memory_usage: u64,
) -> Result<Vec<Arc<FullParquetMeta>>> {
    if file_infos.is_empty() {
        return Ok(vec![]);
    }
    let num_files = file_infos.len();

    let mut tasks = Vec::with_capacity(num_threads);
    // Equally distribute the tasks
    for i in 0..num_threads {
        let begin = num_files * i / num_threads;
        let end = num_files * (i + 1) / num_threads;
        if begin == end {
            continue;
        }

        let file_infos = file_infos[begin..end].to_vec();
        let op = op.clone();

        tasks.push(read_parquet_metas_batch_for_copy(
            file_infos,
            op,
            max_memory_usage,
        ));
    }

    let metas = execute_futures_in_parallel(
        tasks,
        num_threads,
        num_threads * 2,
        "read-parquet-metas-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    Ok(metas)
}

/// Load parquet meta and check if the schema is matched.
#[async_backtrace::framed]
async fn load_and_check_parquet_meta(
    file: &str,
    size: u64,
    op: Operator,
    expect: &SchemaDescriptor,
    schema_from: &str,
) -> Result<Arc<ParquetMetaData>> {
    let metadata =
        databend_common_storage::parquet_rs::read_metadata_async(file, &op, Some(size)).await?;
    check_parquet_schema(
        expect,
        metadata.file_metadata().schema_descr(),
        file,
        schema_from,
    )?;
    Ok(Arc::new(metadata))
}

pub async fn read_parquet_metas_batch(
    file_infos: Vec<(String, u64)>,
    op: Operator,
    expect: SchemaDescPtr,
    leaf_fields: Arc<Vec<TableField>>,
    schema_from: String,
    max_memory_usage: u64,
) -> Result<Vec<Arc<FullParquetMeta>>> {
    let mut metas = Vec::with_capacity(file_infos.len());
    for (location, size) in file_infos {
        let meta =
            load_and_check_parquet_meta(&location, size, op.clone(), &expect, &schema_from).await?;
        if unlikely(meta.file_metadata().num_rows() == 0) {
            // Don't collect empty files
            continue;
        }
        let stats = collect_row_group_stats(meta.row_groups(), &leaf_fields, None);
        metas.push(Arc::new(FullParquetMeta {
            location,
            size,
            meta,
            row_group_level_stats: stats,
        }));
    }

    check_memory_usage(max_memory_usage)?;
    Ok(metas)
}

pub async fn read_parquet_metas_batch_for_copy(
    file_infos: Vec<(String, u64)>,
    op: Operator,
    max_memory_usage: u64,
) -> Result<Vec<Arc<FullParquetMeta>>> {
    let mut metas = Vec::with_capacity(file_infos.len());
    for (location, size) in file_infos {
        let meta = Arc::new(
            databend_common_storage::parquet_rs::read_metadata_async(&location, &op, Some(size))
                .await?,
        );
        if unlikely(meta.file_metadata().num_rows() == 0) {
            // Don't collect empty files
            continue;
        }
        metas.push(Arc::new(FullParquetMeta {
            location,
            size,
            meta,
            row_group_level_stats: None,
        }));
    }
    check_memory_usage(max_memory_usage)?;
    Ok(metas)
}

// TODO(parquet): how to limit the memory when running this method is to be determined.
fn check_memory_usage(max_memory_usage: u64) -> Result<()> {
    let used = GLOBAL_MEM_STAT.get_memory_usage();
    if max_memory_usage as i64 - used < 100 * 1024 * 1024 {
        return Err(ErrorCode::Internal(format!(
            "not enough memory to load parquet file metas, max_memory_usage = {}, used = {}.",
            max_memory_usage, used
        )));
    }
    Ok(())
}

// /// Load parquet meta and check if the schema is matched.
// fn load_and_check_parquet_meta_sync(
//     file: &str,
//     size: u64,
//     op: BlockingOperator,
//     expect: &SchemaDescriptor,
//     schema_from: &str,
// ) -> Result<Arc<ParquetMetaData>> {
//     let metadata = databend_common_storage::parquet_rs::read_metadata(file, &op, size)?;
//     check_parquet_schema(
//         expect,
//         metadata.file_metadata().schema_descr(),
//         file,
//         schema_from,
//     )?;
//     Ok(Arc::new(metadata))
// }

// pub fn read_parquet_metas_batch_sync(
//     file_infos: Vec<(String, u64)>,
//     op: BlockingOperator,
//     expect: SchemaDescPtr,
//     leaf_fields: Arc<Vec<TableField>>,
//     schema_from: String,
//     max_memory_usage: u64,
// ) -> Result<Vec<Arc<FullParquetMeta>>> {
//     let mut metas = Vec::with_capacity(file_infos.len());
//     for (location, size) in file_infos {
//         let meta =
//             load_and_check_parquet_meta_sync(&location, size, op.clone(), &expect, &schema_from)?;
//         if unlikely(meta.file_metadata().num_rows() == 0) {
//             // Don't collect empty files
//             continue;
//         }
//         let stats = collect_row_group_stats(meta.row_groups(), &leaf_fields, None);
//         metas.push(Arc::new(FullParquetMeta {
//             location,
//             size,
//             meta,
//             row_group_level_stats: stats,
//         }));
//     }

//     check_memory_usage(max_memory_usage)?;
//     Ok(metas)
// }
