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

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_ast::ast::AlterNotificationOptions;
use databend_common_ast::ast::NotificationWebhookOptions;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType::UInt64;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::tenant::Tenant;

pub fn notification_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("created_on", DataType::Timestamp),
        DataField::new("name", DataType::String),
        DataField::new("id", DataType::Number(UInt64)),
        DataField::new("type", DataType::String),
        DataField::new("enabled", DataType::Boolean),
        DataField::new("webhook_options", DataType::Variant.wrap_nullable()),
        DataField::new("comment", DataType::String.wrap_nullable()),
    ]))
}

pub fn notification_history_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("created_on", DataType::Timestamp),
        DataField::new("processed", DataType::Timestamp.wrap_nullable()),
        DataField::new("message_source", DataType::String),
        DataField::new("integration_name", DataType::String),
        DataField::new("message", DataType::String),
        DataField::new("status", DataType::String),
        DataField::new("error_message", DataType::String),
    ]))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotificationType {
    Webhook,
}

impl Display for NotificationType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            NotificationType::Webhook => write!(f, "webhook"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateNotificationPlan {
    pub if_not_exists: bool,
    pub tenant: Tenant,
    pub name: String,
    pub notification_type: NotificationType,
    pub enabled: bool,
    pub webhook_opts: Option<NotificationWebhookOptions>,
    pub comments: Option<String>,
}

impl CreateNotificationPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

// alter
#[derive(Clone, Debug, PartialEq)]
pub struct AlterNotificationPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
    pub options: AlterNotificationOptions,
}

impl AlterNotificationPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

// drop
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropNotificationPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
}

impl DropNotificationPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescNotificationPlan {
    pub tenant: Tenant,
    pub name: String,
}

impl DescNotificationPlan {
    pub fn schema(&self) -> DataSchemaRef {
        notification_schema()
    }
}
