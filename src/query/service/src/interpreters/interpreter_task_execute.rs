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
use common_cloud_control::pb::DescribeTaskRequest;
use common_cloud_control::task_client::make_request;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::plans::ExecuteTaskPlan;
use common_sql::Planner;
use tokio_stream::StreamExt;

use crate::interpreters::common::get_client_config;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ExecuteTaskInterpreter {
    ctx: Arc<QueryContext>,
    plan: ExecuteTaskPlan,
}

impl ExecuteTaskInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ExecuteTaskPlan) -> Result<Self> {
        Ok(ExecuteTaskInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ExecuteTaskInterpreter {
    fn name(&self) -> &str {
        "ExecuteTaskInterpreter"
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let config = GlobalConfig::instance();
        if config.query.cloud_control_grpc_server_address.is_none() {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot execute task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let req = DescribeTaskRequest {
            task_name: self.plan.task_name.clone(),
            tenant_id: self.plan.tenant.clone(),
            if_exist: false,
        };
        let mut config = get_client_config(self.ctx.clone())?;
        config.add_metadata("X-REQUEST-TYPE", "execute");
        let req = make_request(req, config);

        let resp = task_client.describe_task(req).await?;
        let query = resp.task.expect("task is none").query_text;
        let mut planner = Planner::new(self.ctx.clone());
        let (plan, _) = planner.plan_sql(query.as_str()).await?;
        let interpreter = InterpreterFactory::get(self.ctx.clone(), &plan).await?;
        let stream = interpreter.execute(self.ctx.clone()).await?;
        let res = stream.collect::<Result<Vec<_>>>().await?;
        PipelineBuildResult::from_blocks(res)
    }
}
