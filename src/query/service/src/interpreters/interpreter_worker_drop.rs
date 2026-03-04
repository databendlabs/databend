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
use databend_common_cloud_control::pb::DropWorkerRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::DropWorkerPlan;

use crate::interpreters::Interpreter;
use crate::interpreters::common::get_worker_client_config;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct DropWorkerInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropWorkerPlan,
}

impl DropWorkerInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropWorkerPlan) -> Result<Self> {
        Ok(DropWorkerInterpreter { ctx, plan })
    }

    fn build_request(&self) -> DropWorkerRequest {
        let plan = self.plan.clone();
        DropWorkerRequest {
            tenant_id: plan.tenant.tenant_name().to_string(),
            name: plan.name,
            if_exists: plan.if_exists,
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for DropWorkerInterpreter {
    fn name(&self) -> &str {
        "DropWorkerInterpreter"
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
                "cannot drop worker without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let worker_client = cloud_api.get_worker_client();
        let req = self.build_request();
        let config = get_worker_client_config(self.ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        worker_client.drop_worker(req).await?;
        Ok(PipelineBuildResult::create())
    }
}
