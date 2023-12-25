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

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::ParquetPart;

fn limit_parallelism_by_memory(max_memory: usize, sizes: &mut [(usize, usize)]) -> usize {
    // there may be 1 block  reading and 2 blocks in deserializer and sink.
    // memory size of a can be as large as 2 * uncompressed_size.
    // e.g. parquet may use 4 bytes for each string offset, but Block use 8 bytes i64.
    // we can refine it later if this leads to too low parallelism.
    let mut mem = 0;
    for (i, (uncompressed, compressed)) in sizes.iter().enumerate() {
        let s = uncompressed * 2 * 2 + compressed;
        mem += s;
        if mem > max_memory {
            return i;
        }
    }
    sizes.len()
}

// Calculate a proper parallelism for pipelines after reading parquet files from parquet source.
pub(crate) fn calc_parallelism(
    ctx: &Arc<dyn TableContext>,
    plan: &DataSourcePlan,
) -> Result<usize> {
    if plan.parts.partitions.is_empty() {
        return Ok(1);
    }
    let settings = ctx.get_settings();
    let num_partitions = plan.parts.partitions.len();
    let max_threads = settings.get_max_threads()? as usize;
    let max_memory = settings.get_max_memory_usage()? as usize;

    let mut sizes = vec![];
    for p in plan.parts.partitions.iter() {
        let p = ParquetPart::from_part(p)?;
        sizes.push((p.uncompressed_size() as usize, p.compressed_size() as usize));
    }
    sizes.sort_by(|a, b| b.cmp(a));
    let max_split_size = sizes[0].0;
    // 1. used by other query
    // 2. used for file metas, can be huge when there are many files.
    let used_memory = GLOBAL_MEM_STAT.get_memory_usage();
    let available_memory = max_memory.saturating_sub(used_memory as usize);

    let max_by_memory = limit_parallelism_by_memory(available_memory, &mut sizes).max(1);
    if max_by_memory == 0 {
        return Err(ErrorCode::Overflow(format!(
            "Memory limit exceeded before start copy pipeline: max_memory_usage: {}, used_memory: {}, max_split_size = {}",
            max_memory, used_memory, max_split_size,
        )));
    }
    let parall_limit = max_threads.min(max_by_memory).max(1);

    log::info!(
        "loading {num_partitions} partitions \
        with {parall_limit} deserializers, \
        according to \
        max_split_size={max_split_size}, \
        max_threads={max_threads}, \
        max_memory={max_memory}, \
        available_memory={available_memory}"
    );
    Ok(parall_limit)
}
