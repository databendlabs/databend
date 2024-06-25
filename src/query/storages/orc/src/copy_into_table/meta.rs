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

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_exception::Result;
use opendal::Operator;
use orc_rust::ArrowReaderBuilder;

use crate::chunk_reader_impl::OrcChunkReader;
use crate::copy_into_table::projection::ProjectionFactory;
use crate::hashable_schema::HashableSchema;
use crate::utils::map_orc_error;

#[async_backtrace::framed]
pub async fn read_metas_in_parallel_for_copy(
    op: &Operator,
    file_infos: &[(String, u64)],
    num_threads: usize,
    projections: &Arc<ProjectionFactory>,
) -> Result<()> {
    if file_infos.is_empty() {
        return Ok(());
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

        tasks.push(read_orc_metas_batch_for_copy(
            file_infos,
            op,
            projections.clone(),
        ));
    }

    execute_futures_in_parallel(
        tasks,
        num_threads,
        num_threads * 2,
        "read-orc-metas-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<_>>>()?;

    Ok(())
}

async fn read_one(
    op: Operator,
    path: String,
    size: u64,
    projections: &Arc<ProjectionFactory>,
) -> Result<()> {
    let file = OrcChunkReader {
        operator: op.clone(),
        size,
        path: path.clone(),
    };
    let builder = ArrowReaderBuilder::try_new_async(file)
        .await
        .map_err(|e| map_orc_error(e, &path))?;
    let reader = builder.build_async();
    let schema = reader.schema();
    let schema = HashableSchema::try_create(schema)?;
    projections.get(&schema, &path)?;
    Ok(())
}

pub async fn read_orc_metas_batch_for_copy(
    file_infos: Vec<(String, u64)>,
    op: Operator,
    projections: Arc<ProjectionFactory>,
) -> Result<()> {
    for (location, size) in file_infos {
        read_one(op.clone(), location, size, &projections).await?;
    }
    Ok(())
}
