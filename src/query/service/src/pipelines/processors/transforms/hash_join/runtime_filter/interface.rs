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

use std::time::Instant;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storages_fuse::TableContext;

use super::builder::build_runtime_filter_packet;
use super::convert::build_runtime_filter_infos;
use super::global::get_global_runtime_filter_packet;
use crate::pipelines::processors::HashJoinBuildState;

pub async fn build_and_push_down_runtime_filter(
    build_chunks: &[DataBlock],
    build_num_rows: usize,
    join: &HashJoinBuildState,
) -> Result<()> {
    let overall_start = Instant::now();

    let inlist_threshold = join
        .ctx
        .get_settings()
        .get_inlist_runtime_filter_threshold()? as usize;
    let bloom_threshold = join
        .ctx
        .get_settings()
        .get_bloom_runtime_filter_threshold()? as usize;
    let min_max_threshold = join
        .ctx
        .get_settings()
        .get_min_max_runtime_filter_threshold()? as usize;

    let build_start = Instant::now();
    let mut packet = build_runtime_filter_packet(
        build_chunks,
        build_num_rows,
        join.runtime_filter_desc(),
        &join.func_ctx,
        inlist_threshold,
        bloom_threshold,
        min_max_threshold,
    )?;
    let build_time = build_start.elapsed();

    log::info!("RUNTIME-FILTER: build runtime filter packet: {:?}, build_num_rows: {}, runtime_filter_desc: {:?}", packet, build_num_rows, join.runtime_filter_desc());

    if let Some(broadcast_id) = join.broadcast_id {
        let merge_start = Instant::now();
        packet = get_global_runtime_filter_packet(broadcast_id, packet, &join.ctx).await?;
        let merge_time = merge_start.elapsed();
        log::info!(
            "RUNTIME-FILTER: Merged global runtime filter in {:?}",
            merge_time
        );
    }

    let runtime_filter_descs = join
        .runtime_filter_desc()
        .iter()
        .map(|r| (r.id, r))
        .collect();
    let runtime_filter_infos = build_runtime_filter_infos(packet, runtime_filter_descs)?;

    let total_time = overall_start.elapsed();
    let filter_count = runtime_filter_infos.len();

    log::info!(
        "RUNTIME-FILTER: Built and deployed {} filters in {:?} (build: {:?}) for {} rows",
        filter_count,
        total_time,
        build_time,
        build_num_rows
    );
    log::info!(
        "RUNTIME-FILTER: runtime_filter_infos: {:?}",
        runtime_filter_infos
    );

    join.ctx.set_runtime_filter(runtime_filter_infos);
    join.set_bloom_filter_ready()?;
    Ok(())
}
