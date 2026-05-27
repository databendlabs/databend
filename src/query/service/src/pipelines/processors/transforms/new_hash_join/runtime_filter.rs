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

use databend_common_catalog::runtime_filter_info::IndexRuntimeFilters;
use databend_common_catalog::runtime_filter_info::PartitionRuntimeFilters;
use databend_common_catalog::runtime_filter_info::RowRuntimeFilters;
use databend_common_catalog::runtime_filter_info::RuntimeFilterBuilder;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_storages_fuse::pruning::BloomRowFilter;
use databend_common_storages_fuse::pruning::InlistBloomIndexFilter;
use databend_common_storages_fuse::pruning::MinMaxPartitionFilter;
use databend_common_storages_fuse::pruning::SpatialIndexFilter;
use databend_storages_common_io::ReadSettings;

use crate::physical_plans::HashJoin;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::build_runtime_filter_infos;
use crate::pipelines::processors::transforms::get_global_runtime_filter_packet;
use crate::sessions::QueryContext;
use crate::sessions::TableContextRuntimeFilter;
use crate::sessions::TableContextSettings;

pub struct RuntimeFiltersDesc {
    ctx: Arc<QueryContext>,
    pub func_ctx: FunctionContext,

    pub bloom_threshold: usize,
    pub inlist_threshold: usize,
    pub min_max_threshold: usize,
    pub spatial_threshold: usize,
    pub selectivity_threshold: u64,

    broadcast_id: Option<u32>,
    pub filters_desc: Vec<RuntimeFilterDesc>,
    /// Per scan_id builder. Dropping all builders notifies the probe side.
    runtime_filter_builders: parking_lot::Mutex<HashMap<usize, RuntimeFilterBuilder>>,
}

impl RuntimeFiltersDesc {
    pub fn create(ctx: &Arc<QueryContext>, join: &HashJoin) -> Result<Arc<RuntimeFiltersDesc>> {
        let settings = ctx.get_settings();
        let bloom_threshold = settings.get_bloom_runtime_filter_threshold()? as usize;
        let inlist_threshold = settings.get_inlist_runtime_filter_threshold()? as usize;
        let min_max_threshold = settings.get_min_max_runtime_filter_threshold()? as usize;
        let spatial_threshold = settings.get_spatial_runtime_filter_threshold()? as usize;
        let selectivity_threshold = settings.get_join_runtime_filter_selectivity_threshold()?;
        let func_ctx = ctx.get_function_context()?;

        let mut filters_desc = Vec::with_capacity(join.runtime_filter.filters.len());
        let mut runtime_filter_builders: HashMap<usize, RuntimeFilterBuilder> = HashMap::new();

        for filter_desc in &join.runtime_filter.filters {
            let filter_desc = RuntimeFilterDesc::from(filter_desc);

            for (_probe_key, scan_id) in &filter_desc.probe_targets {
                runtime_filter_builders
                    .entry(*scan_id)
                    .or_insert_with(|| ctx.get_runtime_filter_builder(*scan_id));
            }

            filters_desc.push(filter_desc);
        }

        Ok(Arc::new(RuntimeFiltersDesc {
            func_ctx,
            filters_desc,
            bloom_threshold,
            inlist_threshold,
            min_max_threshold,
            spatial_threshold,
            selectivity_threshold,
            runtime_filter_builders: parking_lot::Mutex::new(runtime_filter_builders),
            ctx: ctx.clone(),
            broadcast_id: join.broadcast_id,
        }))
    }

    /// Close the broadcast source channel and drop builders to notify probe side.
    /// Called when all threads of a hash join are short-circuited (e.g., downstream
    /// LIMIT satisfied via sequential UNION ALL) and no thread will call `globalization`.
    pub fn close_broadcast(&self) {
        if let Some(broadcast_id) = self.broadcast_id {
            self.ctx.broadcast_source_sender(broadcast_id).close();
        }
        // Drop builders explicitly to notify the probe side immediately.
        self.runtime_filter_builders.lock().clear();
    }

    pub async fn globalization(&self, mut packet: JoinRuntimeFilterPacket) -> Result<()> {
        if let Some(broadcast_id) = self.broadcast_id {
            packet = get_global_runtime_filter_packet(broadcast_id, packet, &self.ctx).await?;
        }

        let runtime_filter_descs = self.filters_desc.iter().map(|r| (r.id, r)).collect();
        let runtime_filter_infos = build_runtime_filter_infos(
            packet,
            runtime_filter_descs,
            self.selectivity_threshold,
            self.ctx.get_settings().get_max_threads()? as usize,
        )
        .await?;

        // Keep raw entries for reporting/logging
        self.ctx.set_runtime_filter(runtime_filter_infos.clone());

        let ctx: Arc<dyn databend_common_catalog::table_context::TableContext> = self.ctx.clone();
        let read_settings = ReadSettings::from_ctx(&ctx)?;
        let inlist_bloom_prune_threshold =
            self.ctx
                .get_settings()
                .get_inlist_runtime_bloom_prune_threshold()? as usize;

        // Build trait impls and push to builders
        for (scan_id, info) in &runtime_filter_infos {
            let (builder, table_schema) = {
                let guard = self.runtime_filter_builders.lock();
                let Some(builder) = guard.get(scan_id) else {
                    continue;
                };
                let Some(table_schema) = builder.table_schema() else {
                    log::warn!(
                        "RUNTIME-FILTER: table_schema not set for scan_id={}, skipping trait construction",
                        scan_id
                    );
                    continue;
                };
                (builder.clone(), table_schema.clone())
            };

            let mut partition_filters: PartitionRuntimeFilters = vec![];
            let mut index_filters: IndexRuntimeFilters = vec![];
            let mut row_filters: RowRuntimeFilters = vec![];

            for entry in &info.filters {
                if let Some(ref expr) = entry.min_max {
                    partition_filters.push(Arc::new(MinMaxPartitionFilter::new(
                        self.func_ctx.clone(),
                        table_schema.clone(),
                        expr.clone(),
                    )));
                }

                if let Some(ref expr) = entry.inlist {
                    partition_filters.push(Arc::new(MinMaxPartitionFilter::new(
                        self.func_ctx.clone(),
                        table_schema.clone(),
                        expr.clone(),
                    )));
                    index_filters.push(Arc::new(InlistBloomIndexFilter::new(
                        self.func_ctx.clone(),
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

        // Drop all builders to signal the probe side that filters are ready.
        self.runtime_filter_builders.lock().clear();

        Ok(())
    }
}
