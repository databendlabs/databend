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

use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;

use crate::physical_plans::HashJoin;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::RuntimeFilterBuildLimit;
use crate::pipelines::processors::transforms::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::build_runtime_filter_infos;
use crate::pipelines::processors::transforms::get_global_runtime_filter_packet;
use crate::sessions::QueryContext;

pub struct RuntimeFiltersDesc {
    ctx: Arc<QueryContext>,
    pub func_ctx: FunctionContext,

    pub bloom_threshold: usize,
    pub inlist_threshold: usize,
    pub min_max_threshold: usize,
    pub selectivity_threshold: u64,
    pub probe_ratio_threshold: f64,
    pub build_limit: Arc<RuntimeFilterBuildLimit>,

    broadcast_id: Option<u32>,
    pub filters_desc: Vec<RuntimeFilterDesc>,
    runtime_filters_ready: Vec<Arc<RuntimeFilterReady>>,
}

impl RuntimeFiltersDesc {
    pub fn create(ctx: &Arc<QueryContext>, join: &HashJoin) -> Result<Arc<RuntimeFiltersDesc>> {
        let settings = ctx.get_settings();
        let bloom_threshold = settings.get_bloom_runtime_filter_threshold()? as usize;
        let inlist_threshold = settings.get_inlist_runtime_filter_threshold()? as usize;
        let min_max_threshold = settings.get_min_max_runtime_filter_threshold()? as usize;
        let selectivity_threshold = settings.get_join_runtime_filter_selectivity_threshold()?;
        let probe_ratio_threshold =
            settings.get_join_runtime_filter_probe_ratio_threshold()? as f64;
        let func_ctx = ctx.get_function_context()?;

        let mut filters_desc = Vec::with_capacity(join.runtime_filter.filters.len());
        let mut runtime_filters_ready = Vec::with_capacity(join.runtime_filter.filters.len());

        for filter_desc in &join.runtime_filter.filters {
            let filter_desc = RuntimeFilterDesc::from(filter_desc);

            if !ctx.get_cluster().is_empty() {
                for (_probe_key, scan_id) in &filter_desc.probe_targets {
                    let ready = Arc::new(RuntimeFilterReady::default());
                    runtime_filters_ready.push(ready.clone());
                    ctx.set_runtime_filter_ready(*scan_id, ready);
                }
            }

            filters_desc.push(filter_desc);
        }

        let build_limit = Arc::new(RuntimeFilterBuildLimit::from_descs(
            &filters_desc,
            selectivity_threshold,
            probe_ratio_threshold,
        ));

        Ok(Arc::new(RuntimeFiltersDesc {
            func_ctx,
            filters_desc,
            bloom_threshold,
            inlist_threshold,
            min_max_threshold,
            selectivity_threshold,
            probe_ratio_threshold,
            build_limit,
            runtime_filters_ready,
            ctx: ctx.clone(),
            broadcast_id: join.broadcast_id,
        }))
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
            self.probe_ratio_threshold,
            self.ctx.get_settings().get_max_threads()? as usize,
        )
        .await?;

        self.ctx.set_runtime_filter(runtime_filter_infos);

        for runtime_filter_ready in self.runtime_filters_ready.iter() {
            runtime_filter_ready
                .runtime_filter_watcher
                .send(Some(()))
                .map_err(|_| ErrorCode::TokioError("watcher channel is closed"))?;
        }

        Ok(())
    }
}
