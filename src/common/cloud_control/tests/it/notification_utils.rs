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

use databend_common_cloud_control::notification_utils::Notification;
use databend_common_cloud_control::notification_utils::NotificationParams;
use databend_common_cloud_control::pb;
use databend_common_cloud_control::utils::Timestamp;

/// A webhook notification as returned by Cloud Control. `webhook_body_template`
/// is left unset, which is exactly what a Cloud Control predating the field
/// (or one where the template was never configured) sends on the wire.
fn webhook_notification_pb() -> pb::Notification {
    let ts = Timestamp {
        seconds: 1_700_000_000,
        nanos: 0,
    };
    pb::Notification {
        notification_id: 1,
        tenant_id: "tenant".to_string(),
        name: "n".to_string(),
        notification_type: pb::NotificationType::Webhook as i32,
        enabled: true,
        webhook_url: "https://example.com/hook".to_string(),
        webhook_method: Some("POST".to_string()),
        webhook_authorization_header: Some("Bearer t".to_string()),
        webhook_body_template: None,
        comments: None,
        created_time: Some(ts),
        updated_time: Some(ts),
        created_by: String::new(),
        updated_by: String::new(),
    }
}

fn webhook_options_json(notification: pb::Notification) -> serde_json::Value {
    let notification = Notification::try_from(notification).unwrap();
    let NotificationParams::Webhook(webhook) = notification.params;
    serde_json::to_value(webhook).unwrap()
}

/// Compatibility with a Cloud Control that does not know about
/// `webhook_body_template`: `webhook_options` must keep its pre-existing
/// shape and not grow a `body_template: null` entry.
#[test]
fn webhook_options_omit_absent_body_template() {
    let json = webhook_options_json(webhook_notification_pb());

    assert_eq!(
        json,
        serde_json::json!({
            "url": "https://example.com/hook",
            "method": "POST",
            "authorization_header": "Bearer t",
        })
    );
}

#[test]
fn webhook_options_include_present_body_template() {
    let mut notification = webhook_notification_pb();
    let template = r#"{"text":"DATABEND_WEBHOOK_MESSAGE"}"#;
    notification.webhook_body_template = Some(template.to_string());

    let json = webhook_options_json(notification);

    assert_eq!(json["body_template"], serde_json::json!(template));
}
