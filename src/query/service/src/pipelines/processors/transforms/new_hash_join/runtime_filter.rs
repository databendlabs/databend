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

use crate::physical_plans::HashJoin;
use crate::pipelines::processors::transforms::build_runtime_filter_infos;
use crate::pipelines::processors::transforms::get_global_runtime_filter_packet;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::RuntimeFilterDesc;
use crate::sessions::QueryContext;

pub struct PlanRuntimeFilterDesc {
    ctx: Arc<QueryContext>,

    broadcast_id: Option<u32>,
    filters_desc: Vec<RuntimeFilterDesc>,
    runtime_filters_ready: Vec<Arc<RuntimeFilterReady>>,
}

impl PlanRuntimeFilterDesc {
    pub fn create(ctx: &Arc<QueryContext>, join: &HashJoin) -> Arc<PlanRuntimeFilterDesc> {
        let mut filters_desc = Vec::with_capacity(join.runtime_filter.filters.len());
        let mut runtime_filters_ready = Vec::with_capacity(join.runtime_filter.filters.len());

        for filter_desc in &join.runtime_filter.filters {
            let filter_desc = RuntimeFilterDesc::from(filter_desc);

            if !ctx.get_cluster().is_empty() {
                let ready = Arc::new(RuntimeFilterReady::default());
                runtime_filters_ready.push(ready.clone());
                ctx.set_runtime_filter_ready(filter_desc.scan_id, ready);
            }

            filters_desc.push(filter_desc);
        }

        Arc::new(PlanRuntimeFilterDesc {
            filters_desc,
            runtime_filters_ready,
            ctx: ctx.clone(),
            broadcast_id: join.broadcast_id,
        })
    }

    pub async fn globalization(&self, mut packet: JoinRuntimeFilterPacket) -> Result<()> {
        if let Some(broadcast_id) = self.broadcast_id {
            packet = get_global_runtime_filter_packet(broadcast_id, packet, &self.ctx).await?;
        }

        let runtime_filter_descs = self.filters_desc.iter().map(|r| (r.id, r)).collect();
        let runtime_filter_infos = build_runtime_filter_infos(packet, runtime_filter_descs)?;

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
