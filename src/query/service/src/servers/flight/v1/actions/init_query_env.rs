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

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_management::WorkloadGroupResourceManager;

use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::QueryEnv;

pub static INIT_QUERY_ENV: &str = "/actions/init_query_env";

pub async fn init_query_env(env: QueryEnv) -> Result<()> {
    let mut tracking_workload_group = None;
    let mut parent_mem_stat = ParentMemStat::StaticRef(&GLOBAL_MEM_STAT);

    if let Some(workload_group_id) = &env.workload_group {
        let mgr = GlobalInstance::get::<Arc<WorkloadGroupResourceManager>>();
        let workload_group = mgr.get_workload(workload_group_id).await?;
        parent_mem_stat = ParentMemStat::Normal(workload_group.mem_stat.clone());
        tracking_workload_group = Some(workload_group);
    }

    let name = Some(env.query_id.clone());
    let query_mem_stat = MemStat::create_child(name, 0, parent_mem_stat);

    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.query_id = Some(env.query_id.clone());
    tracking_payload.mem_stat = Some(query_mem_stat.clone());
    tracking_payload.workload_group_resource = tracking_workload_group;

    tracking_payload
        .tracking(async move {
            let ctx = match env.request_server_id == GlobalConfig::instance().query.node_id {
                true => None,
                false => Some(env.create_query_ctx().await?),
            };

            if let Err(e) = DataExchangeManager::instance()
                .init_query_env(&env, ctx)
                .await
            {
                DataExchangeManager::instance().on_finished_query(&env.query_id, Some(e.clone()));
                return Err(e);
            }

            Ok(())
        })
        .await
}
