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
use std::time::Instant;

use databend_common_catalog::runtime_filter_info::IndexRuntimeFilters;
use databend_common_catalog::runtime_filter_info::PartitionRuntimeFilters;
use databend_common_catalog::runtime_filter_info::RowRuntimeFilters;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storages_fuse::pruning::BloomRowFilter;
use databend_common_storages_fuse::pruning::InlistBloomIndexFilter;
use databend_common_storages_fuse::pruning::MinMaxPartitionFilter;
use databend_common_storages_fuse::pruning::SpatialIndexFilter;
use databend_storages_common_io::ReadSettings;

use super::convert::build_runtime_filter_infos;
use super::global::get_global_runtime_filter_packet;
use crate::pipelines::processors::HashJoinBuildState;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::sessions::TableContextRuntimeFilter;
use crate::sessions::TableContextSettings;

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
    let max_threads = join.ctx.get_settings().get_max_threads()? as usize;
    let build_rows = packet.build_rows;
    let runtime_filter_infos = build_runtime_filter_infos(
        packet,
        runtime_filter_descs,
        selectivity_threshold,
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

    let func_ctx = join.ctx.get_function_context()?;
    let ctx: Arc<dyn TableContext> = join.ctx.clone();
    let read_settings = ReadSettings::from_ctx(&ctx)?;
    let inlist_bloom_prune_threshold =
        join.ctx
            .get_settings()
            .get_inlist_runtime_bloom_prune_threshold()? as usize;

    // Build trait impls and push to builders
    let build_state = unsafe { &*join.hash_join_state.build_state.get() };
    for (scan_id, info) in &runtime_filter_infos {
        let Some(builder) = build_state.runtime_filter_builders.get(scan_id) else {
            continue;
        };

        let Some(table_schema) = builder.table_schema() else {
            log::warn!(
                "RUNTIME-FILTER: table_schema not set for scan_id={}, skipping trait construction",
                scan_id
            );
            continue;
        };
        let table_schema = table_schema.clone();

        let mut partition_filters: PartitionRuntimeFilters = vec![];
        let mut index_filters: IndexRuntimeFilters = vec![];
        let mut row_filters: RowRuntimeFilters = vec![];

        for entry in &info.filters {
            if let Some(ref expr) = entry.min_max {
                partition_filters.push(Arc::new(MinMaxPartitionFilter::new(
                    func_ctx.clone(),
                    table_schema.clone(),
                    expr.clone(),
                )));
            }

            if let Some(ref expr) = entry.inlist {
                partition_filters.push(Arc::new(MinMaxPartitionFilter::new(
                    func_ctx.clone(),
                    table_schema.clone(),
                    expr.clone(),
                )));
                index_filters.push(Arc::new(InlistBloomIndexFilter::new(
                    func_ctx.clone(),
                    table_schema.clone(),
                    read_settings,
                    expr.clone(),
                    entry.inlist_value_count,
                    inlist_bloom_prune_threshold,
                )));
            }

            if let Some(ref spatial) = entry.spatial {
                if !spatial.rtrees.is_empty() {
                    if let Ok(field) = table_schema.field_with_name(&spatial.column_name) {
                        index_filters.push(Arc::new(SpatialIndexFilter::new(
                            field.column_id(),
                            spatial.srid,
                            spatial.rtrees.clone(),
                            spatial.rtree_bounds,
                            read_settings,
                        )));
                    }
                }
            }

            if let Some(ref bloom) = entry.bloom {
                row_filters.push(BloomRowFilter::create(
                    bloom.column_name.clone(),
                    bloom.filter.clone(),
                ));
            }
        }

        builder.add_partition_filters(partition_filters);
        builder.add_index_filters(index_filters);
        builder.add_row_filters(row_filters);
    }

    // Keep raw entries for reporting/logging
    join.ctx.set_runtime_filter(runtime_filter_infos);
    // Drop builders to signal readiness
    join.set_bloom_filter_ready()?;
    Ok(())
}
