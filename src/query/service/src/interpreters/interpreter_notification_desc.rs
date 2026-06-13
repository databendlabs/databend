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
use databend_common_cloud_control::pb::GetNotificationRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::DescNotificationPlan;
use databend_common_storages_system::parse_notifications_to_datablock;

use crate::interpreters::Interpreter;
use crate::interpreters::common::get_notification_client_config;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct DescNotificationInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescNotificationPlan,
}

impl DescNotificationInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescNotificationPlan) -> Result<Self> {
        Ok(DescNotificationInterpreter { ctx, plan })
    }
}

impl DescNotificationInterpreter {
    fn build_request(&self) -> GetNotificationRequest {
        let plan = self.plan.clone();
        GetNotificationRequest {
            tenant_id: plan.tenant.tenant_name().to_string(),
            name: plan.name,
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for DescNotificationInterpreter {
    fn name(&self) -> &str {
        "DescNotificationInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
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
                "cannot describe notification without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let notification_cli = cloud_api.get_notification_client();
        let req = self.build_request();
        let config = get_notification_client_config(self.ctx.clone(), cloud_api.get_timeout())?;
        let req = make_request(req, config);

        let resp = notification_cli.desc_notification(req).await?;
        if resp.notification.is_none() {
            return Ok(PipelineBuildResult::create());
        }
        let tasks = vec![resp.notification.unwrap()];
        let result = parse_notifications_to_datablock(tasks)?;
        PipelineBuildResult::from_blocks(vec![result])
    }
}
