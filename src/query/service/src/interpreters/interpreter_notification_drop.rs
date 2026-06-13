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
use databend_common_cloud_control::pb::DropNotificationRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::DropNotificationPlan;

use crate::interpreters::Interpreter;
use crate::interpreters::common::get_notification_client_config;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct DropNotificationInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropNotificationPlan,
}

impl DropNotificationInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropNotificationPlan) -> Result<Self> {
        Ok(DropNotificationInterpreter { ctx, plan })
    }
}

impl DropNotificationInterpreter {
    fn build_request(&self) -> DropNotificationRequest {
        let plan = self.plan.clone();
        DropNotificationRequest {
            tenant_id: plan.tenant.tenant_name().to_string(),
            name: plan.name,
            if_exists: plan.if_exists,
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for DropNotificationInterpreter {
    fn name(&self) -> &str {
        "DropNotificationInterpreter"
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
                "cannot drop notification without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_notification_client();
        let req = self.build_request();
        let config = get_notification_client_config(self.ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);
        task_client.drop_notification(req).await?;
        Ok(PipelineBuildResult::create())
    }
}
