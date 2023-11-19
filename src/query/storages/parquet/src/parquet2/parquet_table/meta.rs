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

use common_arrow::parquet::metadata::FileMetaData;
use common_base::runtime::execute_futures_in_parallel;
use common_base::runtime::GLOBAL_MEM_STAT;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;
use storages_common_cache::LoadParams;

use crate::parquet2::parquet_reader::MetaDataReader;

#[async_backtrace::framed]
pub async fn read_metas_in_parallel(
    op: &Operator,
    file_infos: &[(String, u64)],
    num_threads: usize,
    max_memory_usage: u64,
) -> Result<Vec<Arc<FileMetaData>>> {
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

        tasks.push(read_parquet_metas_batch(file_infos, op, max_memory_usage));
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

#[async_backtrace::framed]
pub async fn read_meta_data(
    dal: Operator,
    location: &str,
    filesize: u64,
) -> Result<Arc<FileMetaData>> {
    let reader = MetaDataReader::meta_data_reader(dal);

    let load_params = LoadParams {
        location: location.to_owned(),
        len_hint: Some(filesize),
        ver: 0,
        put_cache: true,
    };

    reader.read(&load_params).await
}

pub async fn read_parquet_metas_batch(
    file_infos: Vec<(String, u64)>,
    op: Operator,
    max_memory_usage: u64,
) -> Result<Vec<Arc<FileMetaData>>> {
    let mut metas = Vec::with_capacity(file_infos.len());
    for (location, size) in file_infos {
        let meta = read_meta_data(op.clone(), &location, size).await?;
        if unlikely(meta.num_rows == 0) {
            // Don't collect empty files
            continue;
        }
        // let stats = collect_row_group_stats(meta.row_groups(), &leaf_fields, None);
        metas.push(meta);
    }

    // TODO(parquet): how to limit the memory when running this method is to be determined.
    let used = GLOBAL_MEM_STAT.get_memory_usage();
    if max_memory_usage as i64 - used < 100 * 1024 * 1024 {
        Err(ErrorCode::Internal(format!(
            "not enough memory to load parquet file metas, max_memory_usage = {}, used = {}.",
            max_memory_usage, used
        )))
    } else {
        Ok(metas)
    }
}
