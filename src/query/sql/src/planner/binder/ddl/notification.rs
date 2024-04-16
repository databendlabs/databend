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

use databend_common_ast::ast::AlterNotificationOptions;
use databend_common_ast::ast::AlterNotificationStmt;
use databend_common_ast::ast::CreateNotificationStmt;
use databend_common_ast::ast::DescribeNotificationStmt;
use databend_common_ast::ast::DropNotificationStmt;
use databend_common_ast::ast::NotificationWebhookOptions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::plans::AlterNotificationPlan;
use crate::plans::CreateNotificationPlan;
use crate::plans::DescNotificationPlan;
use crate::plans::DropNotificationPlan;
use crate::plans::NotificationType;
use crate::plans::Plan;
use crate::Binder;

fn verify_notification_type(t: &String) -> Result<NotificationType> {
    match t.to_lowercase().as_str() {
        "webhook" => Ok(NotificationType::Webhook),
        _ => Err(ErrorCode::SyntaxException(format!(
            "Unsupported notification type: {}",
            t
        ))),
    }
}

fn verify_webhook_method(method: &String) -> Result<()> {
    match method.to_lowercase().as_str() {
        "get" => Ok(()),
        "post" => Ok(()),
        _ => Err(ErrorCode::SyntaxException(format!(
            "Unsupported webhook method: {}",
            method
        ))),
    }
}

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_notification(
        &mut self,
        stmt: &CreateNotificationStmt,
    ) -> Result<Plan> {
        let CreateNotificationStmt {
            if_not_exists,
            name,
            notification_type,
            enabled,
            webhook_opts,
            comments,
        } = stmt;
        let t = verify_notification_type(notification_type)?;
        match t {
            NotificationType::Webhook => {
                if webhook_opts.is_none() {
                    return Err(ErrorCode::SyntaxException(
                        "Webhook options are required".to_string(),
                    ));
                }
                let mut method = "GET".to_string();
                if let Some(opts) = webhook_opts.clone() {
                    if opts.url.is_none() || opts.url.unwrap().is_empty() {
                        return Err(ErrorCode::SyntaxException(
                            "Webhook url is required in Webhook Type Notification".to_string(),
                        ));
                    }
                    if opts.method.is_some() {
                        method = opts.method.unwrap();
                        verify_webhook_method(&method)?;
                    }
                }
                let webhook_opts = webhook_opts
                    .as_ref()
                    .map(|opts| NotificationWebhookOptions {
                        url: opts.url.clone(),
                        method: Some(method),
                        authorization_header: opts.authorization_header.clone(),
                    });

                let tenant = self.ctx.get_tenant();

                let plan = CreateNotificationPlan {
                    if_not_exists: *if_not_exists,
                    tenant,
                    name: name.to_string(),
                    notification_type: t,
                    enabled: *enabled,
                    webhook_opts,
                    comments: comments.clone(),
                };
                Ok(Plan::CreateNotification(Box::new(plan)))
            }
        }
    }

    // alter_notification
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_notification(
        &mut self,
        stmt: &AlterNotificationStmt,
    ) -> Result<Plan> {
        let AlterNotificationStmt {
            if_exists,
            name,
            options,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        match options {
            AlterNotificationOptions::Set(opts) => {
                if opts.enabled.is_none() && opts.webhook_opts.is_none() && opts.comments.is_none()
                {
                    return Err(ErrorCode::SyntaxException(
                        "No options to alter".to_string(),
                    ));
                }
            }
        }
        let plan = AlterNotificationPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
            options: options.clone(),
        };
        Ok(Plan::AlterNotification(Box::new(plan)))
    }

    // drop_notification
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_notification(
        &mut self,
        stmt: &DropNotificationStmt,
    ) -> Result<Plan> {
        let DropNotificationStmt { if_exists, name } = stmt;
        let tenant = self.ctx.get_tenant();

        let plan = DropNotificationPlan {
            if_exists: *if_exists,
            tenant,
            name: name.to_string(),
        };
        Ok(Plan::DropNotification(Box::new(plan)))
    }

    // desc_notification
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_desc_notification(
        &mut self,
        stmt: &DescribeNotificationStmt,
    ) -> Result<Plan> {
        let DescribeNotificationStmt { name } = stmt;

        let tenant = self.ctx.get_tenant();

        let plan = DescNotificationPlan {
            tenant,
            name: name.to_string(),
        };
        Ok(Plan::DescNotification(Box::new(plan)))
    }
}
