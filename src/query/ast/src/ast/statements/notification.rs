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

use std::fmt::Display;
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateNotificationStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub notification_type: String,
    pub enabled: bool,
    pub webhook_opts: Option<NotificationWebhookOptions>,
    pub comments: Option<String>,
}

impl Display for CreateNotificationStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE NOTIFICATION INTEGRATION")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.name)?;
        write!(f, " TYPE = {}", self.notification_type)?;
        write!(f, " ENABLED = {}", self.enabled)?;
        if let Some(webhook_opts) = &self.webhook_opts {
            write!(f, " {}", webhook_opts)?;
        }
        if let Some(comments) = &self.comments {
            write!(f, " COMMENTS = '{comments}'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct NotificationWebhookOptions {
    pub url: Option<String>,
    pub method: Option<String>,
    pub authorization_header: Option<String>,
}

impl Display for NotificationWebhookOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let NotificationWebhookOptions {
            url,
            method,
            authorization_header,
        } = self;
        {
            write!(f, "WEBHOOK = (")?;
            if let Some(url) = url {
                write!(f, " URL = '{}'", url)?;
            }
            if let Some(method) = method {
                write!(f, " METHOD = '{}'", method)?;
            }
            if let Some(authorization_header) = authorization_header {
                write!(f, " AUTHORIZATION_HEADER = '{}'", authorization_header)?;
            }
            write!(f, " )")?;
            Ok(())
        }
    }
}

impl FromIterator<(String, String)> for NotificationWebhookOptions {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        let mut url = None;
        let mut method = None;
        let mut authorization_header = None;
        for (k, v) in iter {
            match k.to_uppercase().as_str() {
                "URL" => url = Some(v),
                "METHOD" => method = Some(v),
                "AUTHORIZATION_HEADER" => authorization_header = Some(v),
                _ => {}
            }
        }
        NotificationWebhookOptions {
            url,
            method,
            authorization_header,
        }
    }
}

// drop notification
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropNotificationStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropNotificationStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP NOTIFICATION INTEGRATION")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        write!(f, " {}", self.name)
    }
}

// alter notification
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterNotificationStmt {
    pub if_exists: bool,
    pub name: String,
    pub options: AlterNotificationOptions,
}
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum AlterNotificationOptions {
    Set(AlterNotificationSetOptions),
}
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct AlterNotificationSetOptions {
    pub enabled: Option<bool>,
    pub webhook_opts: Option<NotificationWebhookOptions>,
    pub comments: Option<String>,
}

impl AlterNotificationSetOptions {
    pub fn enabled(enabled: bool) -> Self {
        AlterNotificationSetOptions {
            enabled: Some(enabled),
            webhook_opts: None,
            comments: None,
        }
    }

    pub fn webhook_opts(webhook_opts: NotificationWebhookOptions) -> Self {
        AlterNotificationSetOptions {
            enabled: None,
            webhook_opts: Some(webhook_opts),
            comments: None,
        }
    }

    pub fn comments(comments: String) -> Self {
        AlterNotificationSetOptions {
            enabled: None,
            webhook_opts: None,
            comments: Some(comments),
        }
    }
}

impl Display for AlterNotificationStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER NOTIFICATION INTEGRATION {}", self.name)?;
        match &self.options {
            AlterNotificationOptions::Set(set_opts) => {
                write!(f, " SET ")?;
                if let Some(enabled) = set_opts.enabled {
                    write!(f, "ENABLED = {}", enabled)?;
                }
                if let Some(webhook_opts) = &set_opts.webhook_opts {
                    write!(f, " {}", webhook_opts)?;
                }
                if let Some(comments) = &set_opts.comments {
                    write!(f, " COMMENTS = '{}'", comments)?;
                }
            }
        }
        Ok(())
    }
}

// describe notification
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DescribeNotificationStmt {
    pub name: String,
}

impl Display for DescribeNotificationStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE NOTIFICATION INTEGRATION {}", self.name)
    }
}
