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
use databend_common_storages_fuse::TableContext;

use super::convert::build_runtime_filter_infos;
use super::global::get_global_runtime_filter_packet;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::HashJoinBuildState;

pub async fn build_and_push_down_runtime_filter(
    mut packet: JoinRuntimeFilterPacket,
    join: &HashJoinBuildState,
) -> Result<()> {
    let overall_start = Instant::now();

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
    let selectivity_threshold = join
        .ctx
        .get_settings()
        .get_join_runtime_filter_selectivity_threshold()?;
    let probe_ratio_threshold = join
        .ctx
        .get_settings()
        .get_join_runtime_filter_probe_ratio_threshold()? as f64;
    let max_threads = join.ctx.get_settings().get_max_threads()? as usize;
    let build_rows = packet.build_rows;
    let runtime_filter_infos = build_runtime_filter_infos(
        packet,
        runtime_filter_descs,
        selectivity_threshold,
        probe_ratio_threshold,
        max_threads,
    )
    .await?;

    let total_time = overall_start.elapsed();
    let filter_count = runtime_filter_infos.len();

    log::info!(
        "RUNTIME-FILTER: Built and deployed {} filters in {:?} for {} rows",
        filter_count,
        total_time,
        build_rows
    );
    log::info!(
        "RUNTIME-FILTER: runtime_filter_infos: {:?}",
        runtime_filter_infos
    );

    join.ctx.set_runtime_filter(runtime_filter_infos);
    join.set_bloom_filter_ready()?;
    Ok(())
}
