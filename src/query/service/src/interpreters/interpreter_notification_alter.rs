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

use databend_common_ast::ast::AlterNotificationOptions;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::AlterNotificationRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::AlterNotificationPlan;

use crate::interpreters::Interpreter;
use crate::interpreters::common::get_notification_client_config;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

#[derive(Debug)]
pub struct AlterNotificationInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterNotificationPlan,
}

impl AlterNotificationInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterNotificationPlan) -> Result<Self> {
        Ok(AlterNotificationInterpreter { ctx, plan })
    }
}

impl AlterNotificationInterpreter {
    fn build_request(&self) -> AlterNotificationRequest {
        let plan = self.plan.clone();
        // Cloud Control only accepts operation_type "SET" and applies every
        // field that is present as a patch; absent fields are left untouched.
        let mut req = AlterNotificationRequest {
            tenant_id: self.ctx.get_tenant().tenant_name().to_string(),
            name: plan.name,
            operation_type: "SET".to_string(),
            enabled: None,
            webhook_url: None,
            webhook_method: None,
            webhook_authorization_header: None,
            webhook_body_template: None,
            comments: None,
        };
        match plan.options {
            AlterNotificationOptions::Set(set_options) => {
                req.enabled = set_options.enabled;
                req.webhook_url = set_options
                    .webhook_opts
                    .as_ref()
                    .map(|x| x.url.clone())
                    .unwrap_or_default();
                req.webhook_method = set_options
                    .webhook_opts
                    .as_ref()
                    .map(|x| x.method.clone())
                    .unwrap_or_default();
                req.webhook_authorization_header = set_options
                    .webhook_opts
                    .as_ref()
                    .map(|x| x.authorization_header.clone())
                    .unwrap_or_default();
                req.webhook_body_template = set_options.webhook_body_template;
                req.comments = set_options.comments;
            }
            AlterNotificationOptions::Unset(unset_options) => {
                // The wire form of UNSET is a present-but-empty template; Cloud
                // Control turns it into NULL. Do not send "UNSET" as the
                // operation_type, Cloud Control rejects it.
                if unset_options.webhook_body_template {
                    req.webhook_body_template = Some(String::new());
                }
            }
        }
        req
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterNotificationInterpreter {
    fn name(&self) -> &str {
        "CreateNotificationInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let config = GlobalConfig::instance();
            if config
                .query
                .common
                .cloud_control_grpc_server_address
                .is_none()
            {
                return Err(ErrorCode::CloudControlNotEnabled(
                    "cannot create notification without cloud control enabled, please set cloud_control_grpc_server_address in config",
                ));
            }
            let cloud_api = CloudControlApiProvider::instance();
            let notify_client = cloud_api.get_notification_client();
            let req = self.build_request();
            let config = get_notification_client_config(self.ctx.clone(), cloud_api.get_timeout())?;
            let req = make_request(req, config);
            notify_client.alter_notification(req).await?;
            Ok(PipelineBuildResult::create())
        })
    }
}
