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

use common_cloud_control::cloud_api::CloudControlApiProvider;
use common_cloud_control::pb::ShowTaskRunsRequest;
use common_cloud_control::task_client::make_request;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::plans::ShowTaskRunsPlan;
use common_storages_system::parse_task_runs_to_datablock;

use crate::interpreters::common::get_client_config;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ShowTaskRunsInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowTaskRunsPlan,
}

impl ShowTaskRunsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowTaskRunsPlan) -> Result<Self> {
        Ok(ShowTaskRunsInterpreter { ctx, plan })
    }
}

impl ShowTaskRunsInterpreter {
    async fn build_request(&self) -> Result<ShowTaskRunsRequest> {
        let plan = self.plan.clone();
        let available_roles = self
            .ctx
            .get_current_session()
            .get_all_available_roles()
            .await?;
        let req = ShowTaskRunsRequest {
            tenant_id: plan.tenant,
            scheduled_time_start: "".to_string(),
            scheduled_time_end: "".to_string(),
            result_limit: 10000, // TODO: use plan.limit pushdown
            error_only: false,
            owners: available_roles
                .into_iter()
                .map(|x| x.identity().to_string())
                .collect(),
            task_ids: vec![],
            task_name: "".to_string(),
        };
        Ok(req)
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowTaskRunsInterpreter {
    fn name(&self) -> &str {
        "ShowTaskRunsInterpreter"
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let config = GlobalConfig::instance();
        if config.query.cloud_control_grpc_server_address.is_none() {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot drop task run without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = self.build_request().await?;
        let config = get_client_config(self.ctx.clone())?;
        let req = make_request(req, config);

        let resp = task_client.show_task_runs(req).await?;
        let trs = resp.task_runs;

        let result = parse_task_runs_to_datablock(trs)?;
        PipelineBuildResult::from_blocks(vec![result])
    }
}
