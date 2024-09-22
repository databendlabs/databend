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

use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::ShowTasksRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::ShowTasksPlan;
use databend_common_storages_system::parse_tasks_to_datablock;

use crate::interpreters::common::get_task_client_config;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ShowTasksInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowTasksPlan,
}

impl ShowTasksInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowTasksPlan) -> Result<Self> {
        Ok(ShowTasksInterpreter { ctx, plan })
    }
}

impl ShowTasksInterpreter {
    async fn build_request(&self) -> Result<ShowTasksRequest> {
        let plan = self.plan.clone();
        let available_roles = self
            .ctx
            .get_current_session()
            .get_all_available_roles()
            .await?;
        let req = ShowTasksRequest {
            tenant_id: plan.tenant.tenant_name().to_string(),
            name_like: "".to_string(),
            result_limit: 10000, // TODO: use plan.limit pushdown
            owners: available_roles
                .into_iter()
                .map(|x| x.identity().to_string())
                .collect(),
            task_ids: vec![],
        };
        Ok(req)
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowTasksInterpreter {
    fn name(&self) -> &str {
        "ShowTasksInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let config = GlobalConfig::instance();
        if config.query.cloud_control_grpc_server_address.is_none() {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot drop task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = self.build_request().await?;
        let config = get_task_client_config(self.ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);

        let resp = task_client.show_tasks(req).await?;
        let tasks = resp.tasks;

        let result = parse_tasks_to_datablock(tasks)?;
        PipelineBuildResult::from_blocks(vec![result])
    }
}
