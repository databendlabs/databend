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
use databend_common_cloud_control::pb::CreateWorkerRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::CreateWorkerPlan;

use crate::interpreters::Interpreter;
use crate::interpreters::common::get_worker_client_config;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct CreateWorkerInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateWorkerPlan,
}

impl CreateWorkerInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateWorkerPlan) -> Result<Self> {
        Ok(CreateWorkerInterpreter { ctx, plan })
    }

    fn build_request(&self) -> CreateWorkerRequest {
        let plan = self.plan.clone();
        CreateWorkerRequest {
            tenant_id: plan.tenant.tenant_name().to_string(),
            name: plan.name,
            if_not_exists: plan.if_not_exists,
            tags: plan.tags,
            options: plan.options,
            r#type: String::new(),
            script: String::new(),
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateWorkerInterpreter {
    fn name(&self) -> &str {
        "CreateWorkerInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot create worker without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let worker_client = cloud_api.get_worker_client();
        let req = self.build_request();
        let config = get_worker_client_config(self.ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        worker_client.create_worker(req).await?;
        Ok(PipelineBuildResult::create())
    }
}
