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
use serde_json::Value;
use serde_json::from_str;

use crate::Binder;
use crate::plans::AlterNotificationPlan;
use crate::plans::CreateNotificationPlan;
use crate::plans::DescNotificationPlan;
use crate::plans::DropNotificationPlan;
use crate::plans::NotificationType;
use crate::plans::Plan;

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

/// Placeholder replaced by Cloud Control with the notification message when
/// rendering `WEBHOOK_BODY_TEMPLATE`.
const WEBHOOK_MESSAGE_PLACEHOLDER: &str = "DATABEND_WEBHOOK_MESSAGE";
/// Upper bound on the raw template size, kept in sync with Cloud Control's
/// `webhooktemplate.MaxTemplateBytes`.
///
/// Why 64 KB: the limit only needs to bound what a template can reasonably
/// be, and the receiving side already bounds that for us. Notification
/// webhooks are static-URL, stateless POSTs, which is the incoming-webhook
/// model of every chat platform (Feishu custom bot, DingTalk, Slack, Teams,
/// ...). Those endpoints cap the *whole rendered body*, template plus the
/// substituted `DATABEND_WEBHOOK_MESSAGE`, at tens of KB; Feishu custom bots
/// for example reject bodies over 20 KB
/// (<https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot>).
/// Any template larger than that is unusable no matter what we accept, so
/// 64 KB leaves ample headroom for legitimate templates while still keeping
/// the value small enough to persist in Cloud Control, ship over gRPC and
/// echo back in `system.notifications` without concern.
///
/// This is an abuse guard, not a functional limit. Cloud Control remains the
/// authority; the check here only fails fast with a clearer error.
const MAX_WEBHOOK_BODY_TEMPLATE_BYTES: usize = 64 * 1024;

/// Validates a `WEBHOOK_BODY_TEMPLATE` value before it is sent to Cloud Control.
///
/// Most rules mirror Cloud Control's `webhooktemplate.Validate` so that invalid
/// templates are rejected without a round trip. Query intentionally retains
/// `serde_json`'s recursion limit because it materializes a recursive `Value`
/// and recursively scans object keys. As a result, Query rejects deeply nested
/// templates that Cloud Control may accept.
///
/// The empty-string check is load-bearing: Cloud Control treats an empty
/// template in an ALTER request as "clear the template" (the wire form of
/// `UNSET WEBHOOK_BODY_TEMPLATE`), so rejecting it here is the only thing that
/// keeps `SET WEBHOOK_BODY_TEMPLATE = ''` from silently clearing the template.
/// Error messages intentionally do not echo the template contents.
fn verify_webhook_body_template(template: &str) -> Result<()> {
    if template.is_empty() {
        return Err(ErrorCode::BadArguments(
            "WEBHOOK_BODY_TEMPLATE must not be empty; use UNSET WEBHOOK_BODY_TEMPLATE to clear it",
        ));
    }
    if template.len() > MAX_WEBHOOK_BODY_TEMPLATE_BYTES {
        return Err(ErrorCode::BadArguments(format!(
            "WEBHOOK_BODY_TEMPLATE is {} bytes, exceeds the {} byte limit",
            template.len(),
            MAX_WEBHOOK_BODY_TEMPLATE_BYTES
        )));
    }
    // Parsing a `&str` into `Value` only yields syntax/EOF errors, whose
    // messages carry a line/column position but never echo the input.
    let value: Value = from_str(template).map_err(|e| {
        ErrorCode::BadArguments(format!(
            "WEBHOOK_BODY_TEMPLATE must be a single valid JSON value: {e}"
        ))
    })?;
    if json_has_placeholder_in_key(&value) {
        return Err(ErrorCode::BadArguments(format!(
            "WEBHOOK_BODY_TEMPLATE placeholder {} must not appear in a JSON object key",
            WEBHOOK_MESSAGE_PLACEHOLDER
        )));
    }
    Ok(())
}

fn json_has_placeholder_in_key(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(k, v)| {
            k.contains(WEBHOOK_MESSAGE_PLACEHOLDER) || json_has_placeholder_in_key(v)
        }),
        Value::Array(items) => items.iter().any(json_has_placeholder_in_key),
        _ => false,
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
            webhook_body_template,
            comments,
        } = stmt;
        let t = verify_notification_type(notification_type)?;
        if let Some(template) = webhook_body_template {
            verify_webhook_body_template(template)?;
        }
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
                    webhook_body_template: webhook_body_template.clone(),
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
                if opts.is_empty() {
                    return Err(ErrorCode::SyntaxException(
                        "No options to alter".to_string(),
                    ));
                }
                if let Some(template) = &opts.webhook_body_template {
                    verify_webhook_body_template(template)?;
                }
            }
            AlterNotificationOptions::Unset(opts) => {
                if !opts.webhook_body_template {
                    return Err(ErrorCode::SyntaxException(
                        "No options to unset".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_templates() {
        for tpl in [
            r#"{"text":"DATABEND_WEBHOOK_MESSAGE"}"#,
            r#"{"msg_type":"text","content":{"text":"DATABEND_WEBHOOK_MESSAGE"}}"#,
            r#"["DATABEND_WEBHOOK_MESSAGE"]"#,
            r#""DATABEND_WEBHOOK_MESSAGE""#,
            "null",
            "true",
            "42",
            r#"{"no":"placeholder"}"#,
            r#"{"text":"告警：DATABEND_WEBHOOK_MESSAGE"}"#,
        ] {
            verify_webhook_body_template(tpl).unwrap_or_else(|e| panic!("{tpl}: {e}"));
        }
    }

    #[test]
    fn rejects_empty_template() {
        let err = verify_webhook_body_template("").unwrap_err();
        assert_eq!(err.code(), ErrorCode::BAD_ARGUMENTS);
        assert!(err.message().contains("UNSET"));
    }

    #[test]
    fn rejects_oversized_template() {
        // Exactly at the limit is fine, one byte over is not.
        let padding = MAX_WEBHOOK_BODY_TEMPLATE_BYTES - r#"{"a":""}"#.len();
        let at_limit = format!(r#"{{"a":"{}"}}"#, "x".repeat(padding));
        assert_eq!(at_limit.len(), MAX_WEBHOOK_BODY_TEMPLATE_BYTES);
        verify_webhook_body_template(&at_limit).unwrap();

        let over = format!(r#"{{"a":"{}"}}"#, "x".repeat(padding + 1));
        let err = verify_webhook_body_template(&over).unwrap_err();
        assert_eq!(err.code(), ErrorCode::BAD_ARGUMENTS);
        assert!(err.message().contains("65536"));
        // The template body must not be echoed back.
        assert!(!err.message().contains("xxxx"));
    }

    #[test]
    fn rejects_deeply_nested_template() {
        // Keep serde_json's default recursion limit. Query materializes a
        // recursive Value and recursively scans it, so accepting deeper input
        // would risk exhausting the thread stack. Cloud Control deliberately
        // has a looser JSON depth limit.
        let accepted = format!("{}0{}", "[".repeat(127), "]".repeat(127));
        verify_webhook_body_template(&accepted).unwrap();

        let rejected = format!("{}0{}", "[".repeat(128), "]".repeat(128));
        let err = verify_webhook_body_template(&rejected).unwrap_err();
        assert_eq!(err.code(), ErrorCode::BAD_ARGUMENTS);
        assert!(!err.message().contains(&rejected));
    }

    #[test]
    fn rejects_invalid_json() {
        for tpl in [
            "text",
            r#"{"text":DATABEND_WEBHOOK_MESSAGE}"#,
            r#"{"text":"DATABEND_WEBHOOK_MESSAGE",}"#,
            "{} {}",
            "{",
        ] {
            let err = verify_webhook_body_template(tpl).unwrap_err();
            assert_eq!(err.code(), ErrorCode::BAD_ARGUMENTS, "{tpl}");
            assert!(err.message().contains("line"), "{tpl}: {}", err.message());
            assert!(!err.message().contains(tpl), "{tpl}: {}", err.message());
        }
    }

    #[test]
    fn rejects_placeholder_in_object_key() {
        for tpl in [
            r#"{"DATABEND_WEBHOOK_MESSAGE":"x"}"#,
            r#"{"prefix_DATABEND_WEBHOOK_MESSAGE_suffix":"x"}"#,
            r#"{"a":{"DATABEND_WEBHOOK_MESSAGE":"x"}}"#,
            r#"{"a":[1,{"DATABEND_WEBHOOK_MESSAGE":"x"}]}"#,
        ] {
            let err = verify_webhook_body_template(tpl).unwrap_err();
            assert_eq!(err.code(), ErrorCode::BAD_ARGUMENTS, "{tpl}");
            assert!(err.message().contains("object key"), "{tpl}");
        }
    }
}
