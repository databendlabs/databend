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

use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::Pipeline;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;

use super::limit_file_size_processor::LimitFileSizeProcessor;
use super::writer_processor::ParquetFileWriter;
/// - LimitFileSizeProcessor * 1: slice/group block to batches (as a block meta) to avoid files being too small when there are many threads.
/// - ParquetFileSink * N:  serialize incoming blocks to Vec to reduce memory, and flush when they are large enough.
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_data_to_parquet_files(
    pipeline: &mut Pipeline,
    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,
    op: Operator,
    query_id: String,
    group_id: &std::sync::atomic::AtomicUsize,
    mem_limit: usize,
    max_threads: usize,
    create_by: String,
) -> Result<()> {
    let is_single = info.options.single;
    let max_file_size = info.options.max_file_size;
    // when serializing block to parquet, the memory may be doubled
    let mem_limit = mem_limit / 2;
    pipeline.try_resize(1)?;
    let max_file_size = if is_single {
        None
    } else {
        let max_file_size = if max_file_size == 0 {
            64 * 1024 * 1024
        } else {
            max_file_size.min(mem_limit)
        };
        pipeline.add_transform(|input, output| {
            LimitFileSizeProcessor::try_create(input, output, max_file_size)
        })?;

        let max_threads = max_threads.min(mem_limit / max_file_size).max(1);
        pipeline.try_resize(max_threads)?;
        Some(max_file_size)
    };
    pipeline.add_transform(|input, output| {
        let gid = group_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ParquetFileWriter::try_create(
            input,
            output,
            info.clone(),
            schema.clone(),
            op.clone(),
            query_id.clone(),
            gid,
            max_file_size,
            create_by.clone(),
        )
    })?;
    Ok(())
}
